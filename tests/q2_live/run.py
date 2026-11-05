import argparse
import json
import os
import subprocess
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


parser = argparse.ArgumentParser(description='Exercise Q2 with real HTTP downloads and a warm file cache.')
parser.add_argument('--no-build', action='store_true', help='Use existing application images.')
parser.add_argument('--timeout', type=int, default=900, help='Maximum seconds for each phase (default: 900).')
arguments = parser.parse_args()
if arguments.timeout < 1:
    parser.error('--timeout must be positive')
root = Path(__file__).resolve().parents[2]
if not arguments.no_build and not (root / 'src/nodes/classifier/main.py').is_file():
    parser.error('Missing src/nodes/classifier/main.py; restore it before building, or use --no-build with existing images.')
scripts = Path(__file__).resolve().parent
run_id = datetime.now(timezone(timedelta(hours=-3))).strftime('%Y%m%d-%H%M%S')
output = root / '.artifacts/q2-live' / run_id
output.mkdir(parents=True)
cache = output / 'cache'
cache.mkdir()
project = f'tpq2{run_id.replace("-", "")}'
base = json.loads(subprocess.check_output(['docker', 'compose', '-f', str(root / 'docker-compose.yaml'),
                                          'config', '--format', 'json']))
interval = float(base['services']['image_downloader_0']['environment']['DOWNLOAD_INTERVAL_SECONDS'])
names = ['rabbitmq'] + [f'{kind}_{index}' for kind in ('classifier', 'filter_biography', 'image_downloader')
                         for index in range(2)]
services = {}
for name in names:
    service = base['services'][name]
    service.pop('container_name', None)
    service.pop('ports', None)
    service.pop('networks', None)
    service['image'] = service.get('image', f"{base['name']}-{name}")
    if name.startswith('image_downloader'):
        service['user'] = f'{os.getuid()}:{os.getgid()}'
        service['image'] = 'tp-q2-live-downloader'
        service['environment'].update(DOWNLOAD_INTERVAL_SECONDS=str(interval), IMAGE_CACHE_DIR='/cache/q2',
                                      PYTHONPATH='/instrument:/app', Q2_HTTP_TRACE=f'/test_artifacts/http-{name}.jsonl')
        service['volumes'] = [f'{cache}:/cache/q2', f'{scripts}:/instrument:ro', f'{output}:/test_artifacts']
    services[name] = service
services['driver'] = {
    'image': 'tp-q2-live-downloader', 'entrypoint': ['python3', '/instrument/driver.py'],
    'environment': {'PYTHONPATH': '/repo/src', 'PYTHONUNBUFFERED': '1',
                    'Q2_TEST_TIMEOUT_SECONDS': str(arguments.timeout)},
    'volumes': [f'{root}:/repo:ro', f'{scripts}:/instrument:ro', f'{output}:/test_artifacts'],
    'profiles': ['test'],
}
compose_path = output / 'compose.json'
compose_path.write_text(json.dumps({'services': services}, indent=2))
compose = ['docker', 'compose', '-p', project, '-f', str(compose_path)]
print(f'Q2-only live test artifacts: {output}', flush=True)


def command(args, timeout=300, capture=False):
    return subprocess.run(args, check=True, text=True, timeout=timeout,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.STDOUT if capture else None)


def trace_events():
    return {path.name: [json.loads(line) for line in path.read_text().splitlines()]
            for path in output.glob('http-*.jsonl')}


report = {'datasets': ['subset_a', 'subset_b'], 'rows_per_dataset': 15, 'expected_q2_per_dataset': 3,
          'interval_seconds_per_worker': interval, 'passed': False}
started = False
try:
    if not arguments.no_build:
        command(compose + ['build', *names], timeout=900)
    started = True
    command(compose + ['up', '-d', '--no-build', '--wait', '--wait-timeout', '180', *names], timeout=240)
    deadline = time.monotonic() + 120
    while True:
        state = command(compose + ['exec', '-T', 'rabbitmq', 'rabbitmqctl', '-q', 'list_queues', 'name', 'consumers'],
                        timeout=30, capture=True).stdout
        counts = {}
        for line in state.splitlines():
            fields = line.split('\t')
            if len(fields) == 2 and fields[1].isdigit():
                counts[fields[0]] = int(fields[1])
        bindings = command(compose + ['exec', '-T', 'rabbitmq', 'rabbitmqctl', '-q', 'list_bindings', 'source_name'],
                           timeout=30, capture=True).stdout
        bound = Counter(line.strip() for line in bindings.splitlines())
        if (all(counts.get(name) == 2 for name in ('clasificacion', 'bio', 'imagenes'))
                and all(bound[name] == 2 for name in ('control_clasificador', 'control_filter_biography',
                                                       'control_image_downloader'))):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f'Q2 workers were not ready: {counts}; {bound}')
        time.sleep(2)
    print('All Q2 workers and control bindings are ready.', flush=True)
    cold = subprocess.run(compose + ['run', '--rm', '--no-deps', '-T', 'driver', 'cold'],
                          timeout=arguments.timeout + 30)
    report['cold'] = json.loads((output / 'cold/report.json').read_text())
    if cold.returncode != 0 or not report['cold']['passed']:
        raise AssertionError('Cold Q2 run failed; see cold/report.json')
    cold_events = trace_events()
    report['cold_http_requests'] = sum(event['event'] == 'start' for events in cold_events.values() for event in events)
    gaps = {}
    for name, events in cold_events.items():
        times = [event['monotonic'] for event in events if event['event'] == 'start']
        gaps[name] = [b - a for a, b in zip(times, times[1:])]
    report['request_gaps_seconds'] = gaps
    report['throttle_passed'] = all(gap >= max(0, interval - 0.05) for intervals in gaps.values() for gap in intervals)
    cache_check = subprocess.run(compose + ['run', '--rm', '--no-deps', '-T', '--entrypoint', 'python3',
                                            'image_downloader_0', '/instrument/cache_check.py'], timeout=30)
    report['cache_check'] = json.loads((output / 'warm-cache-check.json').read_text())
    if cache_check.returncode != 0 or not report['cache_check']['passed']:
        raise AssertionError('Cache reuse validation failed; see warm-cache-check.json')
    if cold.returncode == 0:
        warm = subprocess.run(compose + ['run', '--rm', '--no-deps', '-T', 'driver', 'warm'],
                              timeout=arguments.timeout + 30)
        report['warm'] = json.loads((output / 'warm/report.json').read_text())
        report['warm_http_requests'] = sum(event['event'] == 'start' for events in trace_events().values()
                                          for event in events) - report['cold_http_requests']
        cold_rows = {row[1]: row[2:] for client in report['cold']['clients'].values() for row in client['actual']}
        warm_rows = {row[1]: row[2:] for client in report['warm']['clients'].values() for row in client['actual']}
        report['cache_passed'] = (report['cold_http_requests'] > 0
                                  and report['warm_http_requests'] == 0 and cold_rows == warm_rows)
        report['passed'] = warm.returncode == 0 and report['cache_passed'] and report['throttle_passed']
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
except Exception as error:
    report['error'] = str(error)
    raise
finally:
    events = [event for items in trace_events().values() for event in items]
    report['http_status_counts'] = dict(Counter(str(event['status']) for event in events if 'status' in event))
    report['retry_after_headers'] = sorted({event['retry_after'] for event in events if event.get('retry_after')})
    report['test_deadline_seconds'] = arguments.timeout
    if started:
        try:
            logs = subprocess.run(compose + ['logs', '--no-color'], text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=60)
            (output / 'containers.log').write_text(logs.stdout)
        finally:
            cleanup = subprocess.run(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'],
                                     timeout=90, check=False)
            report['cleanup_completed'] = cleanup.returncode == 0
            report['passed'] = report['passed'] and report['cleanup_completed']
    (output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f'Report saved: {output / "report.json"}', flush=True)

raise SystemExit(0 if report['passed'] else 1)
