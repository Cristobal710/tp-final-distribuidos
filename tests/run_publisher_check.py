"""Run the publisher regression in an isolated Docker network."""
import time
import uuid

from tests.full_suite.run import ROOT, command


def main():
    name = "tppublisher" + uuid.uuid4().hex[:12]
    command(["docker", "compose", "build", "gateway", "rabbitmq"])
    command(["docker", "network", "create", name], capture=True)
    try:
        command(["docker", "run", "-d", "--name", name, "--hostname", name,
                 "--network", name, "tp-rabbitmq"], capture=True)
        deadline = time.monotonic() + 90
        while True:
            # Running the first probe as root can create a root-owned Erlang cookie before broker startup.
            ready = command(["docker", "exec", "--user", "rabbitmq", name, "rabbitmq-diagnostics", "-q", "check_running"],
                            capture=True, check=False, timeout=15)
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Publisher regression broker did not start: {ready.stdout}")
            time.sleep(2)
        command(["docker", "run", "--rm", "--name", name + "-check", "--network", name,
                 "-e", f"RABBITMQ_TEST_HOST={name}", "-v", f"{ROOT}:/repo:ro", "-w", "/repo",
                 "--entrypoint", "python3", "tp-gateway", "-m", "tests.rabbitmq_publisher_check"], timeout=120)
    except Exception:
        command(["docker", "logs", name], check=False, timeout=30)
        raise
    finally:
        command(["docker", "rm", "-fv", name + "-check", name], capture=True, check=False, timeout=30)
        command(["docker", "network", "rm", name], capture=True, check=False, timeout=30)


if __name__ == "__main__":
    main()
