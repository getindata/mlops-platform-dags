import airflow
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

# Referencing the model version from the model registry
# models:/{model_name}@{alias}
# models:/{model_name}/{model_version}

MLFLOW_RUN = "models:/spaceflights_panda/2"
INPUT_DF_FILEPATH = "gs://source/input.csv"
OUTPUT_DF_FILEPATH = "gs://target/output.csv"
SCHEDULE = "0 8 * * *"

with airflow.DAG(
        'model-score-spaceflights_panda/2',
        tags=[],
        max_active_runs=3,
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args=dict(
            owner="airflow",
            depends_on_past=False,
            email_on_failure=False,
            email_on_retry=False,
            retries=0,
            retry_delay=timedelta(minutes=5)
        )
) as dag:
    scoring_task = SparkSubmitOperator(
        task_id='scoring_task',
        conn_id='spark_local',
        application="/home/jovyan/gid-mlops-demo/kedro/spaceflights-pandas/airflow_dags/scoring_spark_job.py",
        application_args=[MLFLOW_RUN, INPUT_DF_FILEPATH, OUTPUT_DF_FILEPATH]
    )