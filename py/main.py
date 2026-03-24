"""Project entrypoint."""

from src.jobs.daily import nightly_cn, morning_us
from src.jobs.rebuild import rebuild_metrics, rebuild_signals
from src.jobs.manual import sync_outputs_only


def main() -> None:
    # TODO: add argparse / typer later
    print("Use job functions directly for now.")


if __name__ == "__main__":
    main()
