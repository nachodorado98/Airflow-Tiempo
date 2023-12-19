from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import os

def existe_carpeta()->str:

	ruta_actual=os.getcwd()

	ruta_comprobacion=os.path.join(ruta_actual, "dags/tiempo/data")

	return "leer_csv" if os.path.exists(ruta_comprobacion) else "crear_carpeta"

with DAG("dag_tiempo", start_date=datetime(2023,12,19), description="DAG para obtener datos de la API de OpenWeather",
		schedule_interval=timedelta(days=1), catchup=False) as dag:

	comprobar_carpeta=BranchPythonOperator(task_id="comprobar_carpeta", python_callable=existe_carpeta)

	crear_carpeta=BashOperator(task_id="crear_carpeta", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir data")

	mover_csv=BashOperator(task_id="mover_csv", bash_command="cd ../../opt/airflow/dags/tiempo && mv 'ciudades.csv' '/opt/airflow/dags/tiempo/data/ciudades.csv'")

	leer_csv=BashOperator(task_id="leer_csv", bash_command="echo 'Lectura del CSV'", trigger_rule="none_failed_min_one_success")

	comprobar_carpeta >> [crear_carpeta, leer_csv]

	crear_carpeta >> mover_csv >> leer_csv
	