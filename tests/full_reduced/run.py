import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone


root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / 'src/tools/expected'))
from compare import compare_query, read_rows
from hashes import digest_of
from tests.full_suite.monitor import require_running
from tests.q2_images import validate_images


parser = argparse.ArgumentParser(description='Run all tests and Q1–Q5 for two 15-article clients.')
parser.add_argument('--no-build', action='store_true', help='Use existing application images.')
parser.add_argument('--timeout', type=int, default=900, help='Maximum seconds for the clients (default: 900).')
parser.add_argument('--cache-dir', type=Path, help='Reuse an existing image cache instead of an empty per-run cache.')
arguments = parser.parse_args()
if arguments.timeout < 1:
    parser.error('--timeout must be positive')
if not arguments.no_build and not (root / 'src/nodes/classifier/main.py').is_file():
    parser.error('Missing src/nodes/classifier/main.py; restore it before building, or use --no-build with existing images.')
run_id = datetime.now(timezone(timedelta(hours=-3))).strftime('%Y%m%d-%H%M%S')
output = root / '.artifacts/full-reduced' / run_id
output.mkdir(parents=True)
cache = arguments.cache_dir.resolve() if arguments.cache_dir else output / 'cache'
cache.mkdir(parents=True, exist_ok=True)
project = f'tpreduced{run_id.replace("-", "")}'
base = json.loads(subprocess.check_output(['docker', 'compose', '-f', str(root / 'docker-compose.yaml'),
                                          '-f', str(root / 'docker-compose.subsets.yaml'),
                                          'config', '--format', 'json']))
services = base['services']
for name, service in services.items():
    service['image'] = service.get('image', f"{base['name']}-{name}")
    for key in ('container_name', 'ports', 'networks'):
        service.pop(key, None)
    if name.startswith('image_downloader_'):
        service['user'] = f'{os.getuid()}:{os.getgid()}'
        service['environment'].update(PYTHONPATH='/instrument:/app',
                                      Q2_HTTP_TRACE=f'/test_artifacts/http-{name}.jsonl')
        service['volumes'] = [f'{cache}:/cache/q2', f'{root / "tests/q2_live"}:/instrument:ro',
                              f'{output}:/test_artifacts']
    if name.startswith('client_'):
        (output / name).mkdir()
        service['environment']['RESULT_TIMEOUT_SECONDS'] = str(arguments.timeout)
        service['volumes'] = [f'{root / "datasets"}:/datasets:ro', f'{output / name}:/results']
        service['user'] = f'{os.getuid()}:{os.getgid()}'
compose_path = output / 'compose.json'
compose_path.write_text(json.dumps({'services': services}, indent=2))
compose = ['docker', 'compose', '-p', project, '-f', str(compose_path)]
clients = {'client_0': 'subset_a', 'client_1': 'subset_b'}
workers = set(services) - set(clients)
report = {'passed': False, 'rows_per_client': 15, 'clients': {},
          'cache_directory': str(cache), 'reused_cache': arguments.cache_dir is not None,
          'download_interval_seconds': float(services['image_downloader_0']['environment']['DOWNLOAD_INTERVAL_SECONDS'])}
started = False
print(f'Full reduced-suite artifacts: {output}', flush=True)


def command(arguments, timeout=300, capture=False, check=True, log=None):
    with (output / log).open('w') if log else open(os.devnull, 'w') as file:
        return subprocess.run(arguments, check=check, text=True, timeout=timeout,
                              stdout=subprocess.PIPE if capture else file if log else None,
                              stderr=subprocess.STDOUT)


def state_records():
    text = command(compose + ['ps', '-a', '--format', 'json'], capture=True, timeout=30).stdout.strip()
    records = json.loads(text) if text.startswith('[') else [json.loads(line) for line in text.splitlines() if line]
    return {item['Service']: item for item in records}


def queue_state():
    raw = command(compose + ['exec', '-T', 'rabbitmq', 'rabbitmqctl', '-q', 'list_queues',
                            'name', 'messages_ready', 'messages_unacknowledged'], capture=True, timeout=30).stdout
    pending = []
    for line in raw.splitlines():
        fields = line.split('\t')
        if len(fields) == 3 and fields[1].isdigit() and fields[2].isdigit():
            if int(fields[1]) or int(fields[2]):
                pending.append(line)
    (output / 'queues.txt').write_text(raw)
    return pending


def validate_client(name, subset, exit_code):
    directory = output / name
    expected = root / 'datasets' / subset / 'expected'
    result = {'subset': subset, 'exit_code': exit_code, 'queries': {}, 'passed': exit_code == 0}
    summary_path = directory / 'summary.json'
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    result['summary'] = summary
    result['completion_passed'] = (summary.get('all_done') is True and summary.get('sent_rows') == 15
                                   and sorted(summary.get('finished_queries', [])) == [1, 2, 3, 4, 5])
    result['passed'] = result['passed'] and result['completion_passed']
    for query in range(1, 6):
        source, actual = expected / f'q{query}.csv', directory / f'q{query}.jsonl'
        expected_rows = read_rows(str(source), query)
        item = {'expected_rows': len(expected_rows), 'actual_rows': 0, 'passed': False}
        if actual.exists():
            actual_rows = read_rows(str(actual), query)
            problems = compare_query(query, expected_rows, actual_rows)
            item.update(actual_rows=len(actual_rows), problems=problems,
                        expected_hash=digest_of(str(source), query)[1], actual_hash=digest_of(str(actual), query)[1])
            item['passed'] = not problems and item['expected_hash'] == item['actual_hash']
        else:
            item['problems'] = ['Missing result file']
        result['queries'][str(query)] = item
        result['passed'] = result['passed'] and item['passed']
        print(f"{name} Q{query}: {'PASS' if item['passed'] else 'FAIL'} "
              f"({item['actual_rows']}/{item['expected_rows']} rows)", flush=True)
    cache_enabled = all(services[worker]['environment'].get('IMAGE_CACHE_DIR')
                        for worker in services if worker.startswith('image_downloader_'))
    result['image_integrity'] = validate_images(directory, expected / 'q2.csv', cache if cache_enabled else None)
    result['passed'] = result['passed'] and result['image_integrity']['passed']
    return result


try:
    if not arguments.no_build:
        print('Building all application images; output is saved to build.log.', flush=True)
        command(compose + ['build'], timeout=900, log='build.log')
    print('Running the complete unit-test suite.', flush=True)
    unit = command(['docker', 'run', '--rm', '--network', 'none', '-v', f'{root}:/repo:ro', '-w', '/repo',
                    '--entrypoint', 'python3', services['client_0']['image'], '-m', 'unittest',
                    'discover', '-s', 'tests', '-t', '.', '-v'], timeout=300, check=False, log='unit-tests.log')
    unit_log = (output / 'unit-tests.log').read_text()
    ran = re.search(r'Ran (\d+) tests?', unit_log)
    report['unit_tests'] = {'passed': unit.returncode == 0, 'exit_code': unit.returncode,
                            'tests_run': int(ran.group(1)) if ran else None}
    print(f"Unit tests: {'PASS' if unit.returncode == 0 else 'FAIL'} ({report['unit_tests']['tests_run']} tests)", flush=True)
    started = True
    command(compose + ['up', '-d', '--no-build', '--wait', '--wait-timeout', '180', *sorted(workers)],
            timeout=240, log='startup.log')
    expected_consumers = {'clasificacion': 2, 'texto': 2, 'bio': 2, 'imagenes': 2, 'resultados.0': 1,
                          'join_q3.0': 1, 'top20.0': 1}
    expected_consumers.update({f'{prefix}.{index}': 1 for prefix in
                               ('palabras', 'promedio', 'comparacion', 'promedio_final') for index in range(2)})
    deadline = time.monotonic() + 120
    while True:
        raw = command(compose + ['exec', '-T', 'rabbitmq', 'rabbitmqctl', '-q', 'list_queues', 'name', 'consumers'],
                      capture=True, timeout=30).stdout
        observed = {}
        for line in raw.splitlines():
            fields = line.split('\t')
            if len(fields) == 2 and fields[1].isdigit():
                observed[fields[0]] = int(fields[1])
        bindings = command(compose + ['exec', '-T', 'rabbitmq', 'rabbitmqctl', '-q', 'list_bindings', 'source_name'],
                           capture=True, timeout=30).stdout
        bound = Counter(line.strip() for line in bindings.splitlines())
        if (all(observed.get(name) == count for name, count in expected_consumers.items())
                and all(bound[name] == 2 for name in ('control_clasificador', 'control_text_processor',
                                                      'control_filter_biography', 'control_image_downloader'))):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f'Full pipeline was not ready: {observed}; {bound}')
        time.sleep(2)
    print('All workers ready; starting both real clients with 15 rows each.', flush=True)
    command(compose + ['up', '-d', '--no-build', '--no-deps', *clients], timeout=60, log='clients-start.log')
    deadline = time.monotonic() + arguments.timeout + 15
    next_progress = 0
    while True:
        states = state_records()
        require_running(json.dumps(list(states.values())), workers)
        if all(states.get(name, {}).get('State') == 'exited' for name in clients):
            break
        if time.monotonic() >= deadline:
            report['client_timeout'] = True
            break
        if time.monotonic() >= next_progress:
            counts = {}
            for name in clients:
                counts[name] = {f'q{query}': sum(1 for line in path.read_text().splitlines() if line)
                                for query in range(1, 6)
                                if (path := output / name / f'q{query}.jsonl').exists()}
            print(f'Client progress: {counts}', flush=True)
            next_progress = time.monotonic() + 30
        time.sleep(1)
    for name, subset in clients.items():
        report['clients'][name] = validate_client(name, subset, states.get(name, {}).get('ExitCode'))
    deadline = time.monotonic() + 20
    pending = queue_state()
    while pending and time.monotonic() < deadline and not report.get('client_timeout'):
        time.sleep(1)
        pending = queue_state()
    report['queues_drained'] = not pending
    report['pending_queues'] = pending
    gateway_log = command(compose + ['logs', '--no-color', 'gateway'], capture=True, timeout=30).stdout
    (output / 'gateway.log').write_text(gateway_log)
    report['client_sessions'] = len(re.findall(r'Cliente [0-9a-f]{32} conectado desde', gateway_log))
    report['gateway_delivery_passed'] = not bool(re.search(
        r'Resultado sin sesion activa|Resultado invalido descartado|fallo entregando resultados', gateway_log))
    report['passed'] = (report['unit_tests']['passed'] and all(item['passed'] for item in report['clients'].values())
                        and report['queues_drained'] and report['client_sessions'] == 2
                        and report['gateway_delivery_passed'])
except Exception as error:
    report['error'] = str(error)
    print(f'Full reduced-suite error: {error}', flush=True)
finally:
    events = [json.loads(line) for path in output.glob('http-*.jsonl') for line in path.read_text().splitlines()]
    report['http_status_counts'] = dict(Counter(str(event['status']) for event in events if 'status' in event))
    if started:
        try:
            logs = command(compose + ['logs', '--no-color'], capture=True, check=False, timeout=60)
            (output / 'containers.log').write_text(logs.stdout)
        finally:
            cleanup = command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'],
                              timeout=90, check=False, log='cleanup.log')
            report['cleanup_completed'] = cleanup.returncode == 0
            report['passed'] = report['passed'] and report['cleanup_completed']
    (output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Overall: {'PASS' if report['passed'] else 'FAIL'}. Report: {output / 'report.json'}", flush=True)

raise SystemExit(0 if report['passed'] else 1)
