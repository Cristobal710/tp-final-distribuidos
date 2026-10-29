"""Docker-only entry point for the opt-in, single-client full suite."""
import argparse
import json
import os
import re
import subprocess
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from tests.full_suite.monitor import require_running, run_client

ROOT = Path(__file__).resolve().parents[2]


def command(arguments, *, capture=False, timeout=7200, check=True, stdout=None):
    return subprocess.run(arguments, cwd=ROOT, check=check, timeout=timeout, text=True,
                          stdout=subprocess.PIPE if capture else stdout,
                          stderr=subprocess.STDOUT if capture or stdout is not None else None)


def execute(archive, artifacts, timeout=7200, target_bytes=2_000_000_000, images_per_language=5):
    archive, artifacts = Path(archive).resolve(), Path(artifacts).resolve()
    if not archive.is_file():
        raise FileNotFoundError(archive)
    artifacts.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = artifacts / "runs" / run_id
    run_dir.mkdir(parents=True)
    for name in ("actual", "expected"):
        (run_dir / name).mkdir()
    print(f"Full-suite artifacts: {run_dir}", flush=True)
    base = ["docker", "compose", "-f", str(ROOT / "docker-compose.yaml")]
    command(base + ["build"])
    config = json.loads(command(base + ["config", "--format", "json"], capture=True).stdout)
    services = config["services"]
    image_names = {name: service.get("image", f"{config['name']}-{name}") for name, service in services.items()}

    def helper(module, arguments):
        name = f"tpfull-{run_id}-{module.rsplit('.', 1)[-1]}"
        try:
            return command(["docker", "run", "--rm", "--name", name, "--user", f"{os.getuid()}:{os.getgid()}",
                            "-v", f"{ROOT}:/repo:ro", "-v", f"{archive}:/source/distr_dataset.zip:ro",
                            "-v", f"{artifacts}:/artifacts", "-w", "/repo", "--entrypoint", "python3",
                            image_names["client_0"], "-m", module, *arguments], timeout=timeout)
        except Exception as error:
            (run_dir / "failure.json").write_text(json.dumps({"stage": module, "error": str(error)}, indent=2))
            raise
        finally:
            command(["docker", "rm", "-f", name], check=False, capture=True, timeout=30)

    helper("tests.full_suite.prepare", ["--archive", "/source/distr_dataset.zip", "--output", "/artifacts/dataset",
                                       "--target-bytes", str(target_bytes), "--images-per-language", str(images_per_language)])
    stopwords = command(["docker", "run", "--rm", "--network", "none", "--entrypoint", "python3",
                         image_names["text_processor_0"], "-c",
                         "import json; from nltk.corpus import stopwords; print(json.dumps(sorted(set(stopwords.words('english')) | set(stopwords.words('french')))))"],
                        capture=True).stdout
    (run_dir / "stopwords.json").write_text(stopwords)
    relative_run = run_dir.relative_to(artifacts).as_posix()
    helper("tests.full_suite.reference", ["calculate", "--dataset", "/artifacts/dataset",
           "--output", f"/artifacts/{relative_run}/expected", "--stopwords", f"/artifacts/{relative_run}/stopwords.json"])

    for name, service in list(services.items()):
        if name.startswith("client_") and name != "client_0":
            del services[name]
            continue
        service["image"] = image_names[name]
        for key in ("build", "container_name", "ports", "networks"):
            service.pop(key, None)
        service["logging"] = {"driver": "json-file", "options": {"max-size": "20m", "max-file": "2"}}
    services["client_0"]["environment"].update({"LOCAL_DATASET_DIR": "/input", "RESULT_DIR": "/output",
        "RESULT_TIMEOUT_SECONDS": str(timeout), "DATASET_ROWS_PER_PATTERN": "0"})
    services["client_0"]["volumes"] = [f"{artifacts / 'dataset'}:/input:ro", f"{run_dir / 'actual'}:/output"]
    services["client_0"]["user"] = f"{os.getuid()}:{os.getgid()}"
    runtime = {"services": services, "networks": {"default": {}}}
    compose_path = run_dir / "compose.json"
    compose_path.write_text(json.dumps(runtime, indent=2))
    compose = ["docker", "compose", "-p", f"tpfull{run_id.replace('-', '')}", "-f", str(compose_path)]
    started = False
    try:
        started = True
        command(compose + ["up", "-d", *[name for name in services if name != "client_0"]], timeout=300)
        expected_consumers = {"clasificacion": 2, "texto": 2, "bio": 2, "imagenes": 2, "resultados.0": 1,
                              "join_q3.0": 1, "top20.0": 1}
        expected_consumers.update({f"{prefix}.{i}": 1 for prefix in
            ("palabras", "promedio", "comparacion", "promedio_final") for i in range(2)})
        deadline = time.monotonic() + 180
        while True:
            queues = command(compose + ["exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues", "name", "consumers"],
                             capture=True, check=False, timeout=30)
            observed = {}
            for line in queues.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) == 2 and parts[1].isdigit():
                    observed[parts[0]] = int(parts[1])
            bindings = command(compose + ["exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_bindings", "source_name"],
                               capture=True, check=False, timeout=30)
            bound = Counter(line.strip() for line in bindings.stdout.splitlines())
            if (all(observed.get(name) == count for name, count in expected_consumers.items())
                    and all(bound[exchange] == 2 for exchange in
                            ("control_clasificador", "control_text_processor", "control_filter_biography", "control_image_downloader"))):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Consumers or control bindings not ready: {observed}; {dict(bound)}")
            time.sleep(2)
        print("All workers ready. Starting exactly one application client.", flush=True)

        def check_workers():
            state = command(compose + ["ps", "-a", "--format", "json"], capture=True, timeout=30).stdout
            require_running(state, set(services) - {"client_0"})

        def progress(elapsed):
            counts = {}
            for query in range(1, 6):
                path = run_dir / "actual" / f"q{query}.jsonl"
                count = 0
                if path.exists():
                    with path.open("rb") as source:
                        for block in iter(lambda: source.read(1024 * 1024), b""):
                            count += block.count(b"\n")
                counts[f"q{query}"] = count
            print(f"Client active ({elapsed}s); received rows: {counts}", flush=True)

        with open(run_dir / "client.log", "w") as log:
            run_client(compose + ["run", "--rm", "--no-deps", "client_0"], cwd=ROOT, log=log,
                       timeout=timeout, check_workers=check_workers, progress=progress)
        deadline = time.monotonic() + 30
        while True:
            queues = command(compose + ["exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues",
                                       "name", "messages_ready", "messages_unacknowledged"], capture=True, timeout=30).stdout
            pending = []
            for line in queues.splitlines():
                fields = line.split("\t")
                if len(fields) == 3 and fields[1].isdigit() and fields[2].isdigit():
                    if int(fields[1]) or int(fields[2]):
                        pending.append(line)
            if not pending:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"Messages remain after client completion: {pending}")
            time.sleep(1)
        logs = command(compose + ["logs", "--no-color", "gateway"], capture=True).stdout
        sessions = re.findall(r"Cliente [0-9a-f]{32} conectado desde", logs)
        if len(sessions) != 1:
            raise AssertionError(f"Expected one client session, found {len(sessions)}")
        if re.search(r"Resultado sin sesion activa|Resultado invalido descartado|fallo entregando resultados", logs):
            raise AssertionError("The gateway discarded results or failed delivery; see containers.log")
        helper("tests.full_suite.reference", ["validate", "--dataset", "/artifacts/dataset",
               "--expected", f"/artifacts/{relative_run}/expected", "--actual", f"/artifacts/{relative_run}/actual",
               "--output", f"/artifacts/{relative_run}"])
        report_path = run_dir / "report.json"
        report = json.loads(report_path.read_text())
        report["client_sessions"] = len(sessions)
        report["queues_drained"] = True
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        (artifacts / "latest.json").write_text(json.dumps({"run": str(run_dir), "report": str(report_path)}, indent=2))
        print(f"PASS: Q1–Q5 matched. Report: {report_path}", flush=True)
        return report
    except Exception as error:
        (run_dir / "failure.json").write_text(json.dumps({"error": str(error)}, indent=2))
        raise
    finally:
        if started:
            try:
                logs = command(compose + ["logs", "--no-color"], capture=True, check=False, timeout=30).stdout
                (run_dir / "containers.log").write_text(logs)
                queues = command(compose + ["exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues",
                                            "name", "messages_ready", "messages_unacknowledged"],
                                 capture=True, check=False, timeout=30).stdout
                (run_dir / "queues.txt").write_text(queues)
            finally:
                command(compose + ["down", "--volumes", "--remove-orphans", "--timeout", "15"], check=False, timeout=120)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default=str(Path.home() / "Downloads/distr_dataset.zip"))
    parser.add_argument("--artifacts", default=str(ROOT / ".artifacts/full-suite"))
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--target-bytes", type=int, default=2_000_000_000)
    parser.add_argument("--images-per-language", type=int, default=5)
    args = parser.parse_args()
    os.environ["RUN_FULL_SUITE"] = "1"
    from tests.full_suite.test_single_client import SingleClientFullSuite
    SingleClientFullSuite.configuration = vars(args)
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SingleClientFullSuite))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
