"""Daily UTC publication with deterministic dates across retries and backfills."""
from datetime import datetime, timedelta, timezone
import json
import os
from urllib.request import Request, urlopen
from airflow.sdk import DAG, Param, task, get_current_context
from airflow.timetables.trigger import CronTriggerTimetable


with DAG(
    dag_id="ledgerflow_daily_summary",
    description="Calcule et publie les statistiques bancaires quotidiennes",
    schedule=CronTriggerTimetable("0 2 * * *", timezone="UTC"),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=20)},
    params={"target_date": Param("", type="string", description="Date UTC YYYY-MM-DD ; vide = veille de la date logique"),
            "lookback_days": Param(3, type="integer", minimum=1, maximum=30,
                description="Nombre de jours recalcules pour integrer les arrivees tardives")},
    tags=["ledgerflow", "daily", "pyspark"],
) as dag:
    @task(execution_timeout=timedelta(minutes=65))
    def publish_daily():
        from datetime import date
        context = get_current_context()
        explicit = context["params"]["target_date"]
        # Never derive retries from today's wall clock. A retry after midnight
        # still rebuilds the same dates as the initial scheduled attempt.
        if explicit:
            end = date.fromisoformat(explicit)
        else:
            anchor = context.get("logical_date") or context["dag_run"].run_after
            end = anchor.date() - timedelta(days=1)
        start = end - timedelta(days=context["params"]["lookback_days"] - 1)
        request = Request(os.environ.get("BATCH_WORKER_URL", "http://batch-worker:8090") + "/run",
            data=json.dumps({"start": str(start), "end": str(end)}).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + os.environ["BATCH_WORKER_TOKEN"]})
        with urlopen(request, timeout=3700) as response:
            result = json.load(response)
        if result.get("status") != "SUCCEEDED":
            raise RuntimeError("Daily publication did not succeed")
        return result

    publish_daily()
