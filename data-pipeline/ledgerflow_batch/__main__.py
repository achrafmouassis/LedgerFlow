import json
import sys
from .config import DatabaseSettings, parser, date_range
from .job import run_batch


def main():
    cli = parser()
    args = cli.parse_args()
    try:
        period = date_range(args)
        settings = DatabaseSettings.from_env()
        result = run_batch(settings, period)
    except Exception as error:
        # Error classes are safe to report; connection strings/passwords are not.
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
