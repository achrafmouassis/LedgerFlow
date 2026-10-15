"""CLI and database settings; never put credentials in command output."""
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import argparse
import os
from urllib.parse import quote

@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self):
        if self.end < self.start or (self.end - self.start).days > 365:
            raise ValueError("Date range must be ordered and at most 366 days inclusive")

@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls):
        password = os.environ.get("PGPASSWORD")
        if not password:
            raise ValueError("PGPASSWORD is required")
        port = int(os.environ.get("PGPORT", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError("PGPORT must be between 1 and 65535")
        return cls(os.environ.get("PGHOST", "localhost"), port,
                   os.environ.get("PGDATABASE", "ledgerflow"),
                   os.environ.get("PGUSER", "ledgerflow"), password)

    @property
    def jdbc_url(self):
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"jdbc:postgresql://{host}:{self.port}/{quote(self.database, safe='')}?stringtype=unspecified"

    def connect(self):
        import psycopg
        return psycopg.connect(host=self.host, port=self.port, dbname=self.database,
            user=self.user, password=self.password, autocommit=True,
            connect_timeout=10, application_name="ledgerflow-daily-batch")


def parser():
    cli = argparse.ArgumentParser(description="Rebuild LedgerFlow daily summaries with PySpark")
    cli.add_argument("--date", type=date.fromisoformat, help="One UTC date (default: yesterday)")
    cli.add_argument("--from", dest="start", type=date.fromisoformat)
    cli.add_argument("--to", dest="end", type=date.fromisoformat)
    return cli


def date_range(args, today=None):
    if args.date and (args.start or args.end):
        raise ValueError("Use --date or --from/--to, not both")
    if bool(args.start) != bool(args.end):
        raise ValueError("--from and --to must be supplied together")
    if args.start:
        return DateRange(args.start, args.end)
    selected = args.date or (today or datetime.now(timezone.utc).date()) - timedelta(days=1)
    return DateRange(selected, selected)
