from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

with DAG("uploading_to_Google_Sheets_Subscribe",
         start_date=datetime(2025, 10, 14),
         schedule_interval='20 1 * * *',
         tags=["Google_Sheets"],
         catchup=False,
         describtion="Забираем данные по подпискам из Raw и кладем их в Google таблицы каждые сутки"
         ) as dag:

    Google_Sheets_done = EmptyOperator(
        task_id="Google_Sheets_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    tables = [
        "Astana",
        "Erastov",
        "Fomin",
        "Hunters",
        "Koodrenko",
        "Kvitsiniya",
        "Other",
        "Saljenikina",
        "Unnamed",
        "Urtenova",
        "Vse_scheta"
    ]
    for table in tables:
        cmd_string = f'''
        cd bi-etl-helpers
source venv/bin/activate
cd update_google_sheets/subscribe_group
python {table}.py
        '''
        Task1 = SSHOperator(
            task_id=table,
            ssh_conn_id="ssh_etl_helpers",
            conn_timeout=60,
            cmd_timeout=3600,
            command=cmd_string
        )

        Task1 >> Google_Sheets_done