from datetime import date
from decimal import Decimal
import pytest
from pyspark.sql.types import StructType, StructField, DateType, StringType, DecimalType
from ledgerflow_batch.transform import aggregate_daily

SCHEMA = StructType([
    StructField("event_date", DateType()), StructField("currency", StringType()),
    StructField("operation_type", StringType()), StructField("amount", DecimalType(19,2))])
DAY = date(2026,10,10)

def frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)

def test_groups_by_date_currency_and_type_with_exact_decimals(spark):
    facts = frame(spark, [(DAY,"MAD","DEPOSIT",Decimal("0.10")),
        (DAY,"MAD","DEPOSIT",Decimal("0.20")), (DAY,"EUR","DEPOSIT",Decimal("2.00")),
        (DAY,"MAD","WITHDRAWAL",Decimal("1.00"))])
    result = {(r.currency,r.operation_type):r for r in aggregate_daily(facts).collect()}
    assert len(result) == 3
    assert result[("MAD","DEPOSIT")].operation_count == 2
    assert result[("MAD","DEPOSIT")].total_amount == Decimal("0.30")
    assert result[("MAD","DEPOSIT")].min_amount == Decimal("0.10")
    assert result[("MAD","DEPOSIT")].max_amount == Decimal("0.20")

def test_total_can_exceed_individual_account_precision(spark):
    value=Decimal("99999999999999999.99")
    row=aggregate_daily(frame(spark,[(DAY,"MAD","DEPOSIT",value)]*2)).first()
    assert row.total_amount == Decimal("199999999999999999.98")

def test_empty_input_has_no_groups(spark):
    assert aggregate_daily(frame(spark,[])).count() == 0

@pytest.mark.parametrize("row",[
    (DAY,"mad","DEPOSIT",Decimal("1")),(DAY,"MAD","UNKNOWN",Decimal("1")),
    (DAY,"MAD","DEPOSIT",Decimal("0")),(DAY,"MAD","DEPOSIT",Decimal("-1")),
    (None,"MAD","DEPOSIT",Decimal("1")),(DAY,None,"DEPOSIT",Decimal("1")),
    (DAY,"MAD","DEPOSIT",None)])
def test_invalid_facts_fail_before_publication(spark,row):
    with pytest.raises(ValueError,match="Invalid analytical fact"):
        aggregate_daily(frame(spark,[row]))
