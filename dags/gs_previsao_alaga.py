"""DAG horária de previsão vigente. A coleta histórica tem DAG própria."""
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from alaga_previsao import coletar_previsao


def coletar():
    coletar_previsao(
        Path(Variable.get("alaga_config_dir", default_var="/opt/airflow/config")),
        Path(Variable.get("alaga_chegada_dir", default_var="/opt/airflow/chegada")),
    )


with DAG(
    dag_id="gs_previsao_alaga",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["gs", "alaga-alerta", "big-data"],
) as dag:
    PythonOperator(task_id="coletar_previsao_vigente", python_callable=coletar)
