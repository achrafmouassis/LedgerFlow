from datetime import date
import pytest
from ledgerflow_batch.config import DateRange, DatabaseSettings, parser, date_range

def test_default_is_previous_utc_day():
    assert date_range(parser().parse_args([]),today=date(2026,10,15)) == DateRange(date(2026,10,14),date(2026,10,14))

def test_single_date_and_explicit_range():
    assert date_range(parser().parse_args(["--date","2026-10-10"])).start == date(2026,10,10)
    assert date_range(parser().parse_args(["--from","2026-10-10","--to","2026-10-12"])).end == date(2026,10,12)

@pytest.mark.parametrize("arguments",[
    ["--date","2026-10-10","--from","2026-10-10"],["--from","2026-10-10"],
    ["--from","2026-10-12","--to","2026-10-10"],["--from","2020-01-01","--to","2022-01-01"]])
def test_invalid_date_combinations_are_rejected(arguments):
    with pytest.raises(ValueError): date_range(parser().parse_args(arguments))

def test_password_is_required_and_not_exposed_in_settings(monkeypatch):
    monkeypatch.delenv("PGPASSWORD",raising=False)
    with pytest.raises(ValueError,match="PGPASSWORD"): DatabaseSettings.from_env()
    settings=DatabaseSettings("localhost",5432,"ledgerflow","user","dummy-secret")
    assert "dummy-secret" not in repr(settings)
    assert "dummy-secret" not in settings.jdbc_url
