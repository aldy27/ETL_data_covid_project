from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from script import connector_db

default_arguments = {
    "owner": "rinaldy",
    "start_date": datetime(2021, 12, 15)
}

dag = DAG(
    dag_id="etl_workflow",
    default_args=default_arguments
)

# operators
start = DummyOperator(
    task_id="start",
    dag=dag
)

connector_db_stagging = PythonOperator(
    task_id="connector_db_stagging",
    python_callable=connector_db.covidjson_to_stagging,
    dag=dag
)

connector_db_prod = PythonOperator(
    task_id="connector_db_prod",
    python_callable=connector_db.data_to_prod,
    dag=dag
)

create_table_sql = PostgresOperator(
    task_id="create_table_sql",
    sql="./sql/create_table.sql",
    dag=dag,
    postgres_conn_id="aldy_postgres"
)

# python_task_2 = PythonOperator(
#     task_id="print_something",
#     python_callable=print_something,
#     op_kwargs={"sentence": "This comes from Python Operator with kwargs"},
#     dag=dag
# )

# dependencies
start >> connector_db_stagging >> connector_db_prod >> create_table_sql
# >> python_task_2
