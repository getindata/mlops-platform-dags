import airflow
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


SCHEDULE = ""

with airflow.DAG(
    'spaceflights-scoring-spark',
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
        application="gs://gid-ml-ops-sandbox-airflow-experiments/scoring_spark_job.py",
        env_vars={
            "MODEL_URI": "runs:/e384e6cd511f410bb93d0c5710f53ab4/model",
            "INPUT_DF_FILEPATH": "gs://gid-ml-ops-sandbox-airflow-experiments/data/02_intermediate/x_test.csv",
            "OUTPUT_DF_FILEPATH": "gs://gid-ml-ops-sandbox-airflow-experiments/data/07_model_output/preditions.csv"
        }
    )