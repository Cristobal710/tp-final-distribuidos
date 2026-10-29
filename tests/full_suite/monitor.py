"""Observe the client and required services throughout an integration run."""
import json
import subprocess
import time


def require_running(output, required):
    output = output.strip()
    records = json.loads(output) if output.startswith("[") else [json.loads(line) for line in output.splitlines() if line]
    services = {item["Service"]: item for item in records}
    failures = []
    for name in sorted(required):
        state = services.get(name)
        if state is None:
            failures.append(f"{name}: missing container")
        elif state.get("State") != "running" or state.get("Health") == "unhealthy":
            failures.append(f"{name}: {state.get('State')}, exit={state.get('ExitCode')}, health={state.get('Health')}")
    if failures:
        raise RuntimeError("Required workers failed: " + "; ".join(failures))


def run_client(arguments, *, cwd, log, timeout, check_workers, progress=None, poll_seconds=2):
    started = time.monotonic()
    next_progress = started
    process = subprocess.Popen(arguments, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, text=True)
    try:
        while process.poll() is None:
            check_workers()
            now = time.monotonic()
            if now - started >= timeout:
                raise subprocess.TimeoutExpired(arguments, timeout)
            if progress is not None and now >= next_progress:
                progress(int(now - started))
                next_progress = now + 30
            time.sleep(poll_seconds)
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, arguments)
        check_workers()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
