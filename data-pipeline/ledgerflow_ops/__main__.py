import argparse
import json
import os
from ledgerflow_batch.config import DatabaseSettings


def main():
    parser = argparse.ArgumentParser(description="LedgerFlow operational tools")
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("serve", "monitor", "init-dashboard", "init-bi", "recover", "list-dlt"):
        sub.add_parser(action)
    replay = sub.add_parser("replay")
    replay.add_argument("--id", required=True)
    replay.add_argument("--replacement", help="Explicit corrected event JSON; original DLT is retained")
    args = parser.parse_args()
    if args.action == "serve":
        from .batch_server import serve
        serve()
        return
    if args.action == "monitor":
        from .monitor import run_monitor
        run_monitor()
        return
    with DatabaseSettings.from_env().connect() as connection:
        if args.action == "init-bi":
            from .bi import initialize_bi
            initialize_bi(connection, os.environ["BI_DB_PASSWORD"])
        elif args.action == "recover":
            from .recovery import recover_runs
            print(json.dumps({"recovered": recover_runs(connection)}))
        elif args.action == "list-dlt":
            rows = connection.execute("SELECT id,status,topic,partition_id,offset_id,captured_at FROM analytics.dead_letters ORDER BY captured_at DESC LIMIT 100").fetchall()
            print(json.dumps(rows, default=str))
        elif args.action == "replay":
            from confluent_kafka import Producer
            from .monitor import kafka_settings
            from .dead_letters import replay as replay_letter
            producer = Producer({**kafka_settings(), "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 10000})
            replay_letter(connection, producer, args.id, os.environ.get("KAFKA_TOPIC", "ledgerflow.operations.v1"), args.replacement)
            print(json.dumps({"status": "REPLAYED", "id": args.id}))
        elif args.action == "init-dashboard":
            from psycopg import sql
            password = os.environ["DASHBOARD_DB_PASSWORD"]
            if len(password) < 16:
                raise ValueError("Dashboard database password must contain at least 16 characters")
            with connection.transaction():
                if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname='ledgerflow_dashboard'").fetchone():
                    connection.execute("CREATE ROLE ledgerflow_dashboard LOGIN")
                connection.execute(sql.SQL("ALTER ROLE ledgerflow_dashboard PASSWORD {}").format(sql.Literal(password)))
                connection.execute("ALTER ROLE ledgerflow_dashboard SET default_transaction_read_only=on")
                connection.execute("GRANT USAGE ON SCHEMA analytics TO ledgerflow_dashboard")
                connection.execute("""GRANT SELECT ON analytics.daily_operation_summary,analytics.batch_runs,
                    analytics.monitor_samples,analytics.alerts,analytics.dead_letter_overview TO ledgerflow_dashboard""")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__}), flush=True)
        raise SystemExit(1)
