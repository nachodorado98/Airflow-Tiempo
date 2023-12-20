from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import csv
import requests
from typing import Optional, Dict
from airflow.exceptions import AirflowSkipException

from tiempo.config import KEY

# Funcion para comprobar la existencia de la carpeta data
def existe_carpeta()->str:

	ruta_comprobacion=os.path.join(os.getcwd(), "dags/tiempo/data")

	return "leer_csv" if os.path.exists(ruta_comprobacion) else "crear_carpeta"

# Funcion para leer el archivo CSV
def leerCSV(**kwarg)->None:

	ruta_archivo_csv=os.path.join(os.getcwd(), "dags/tiempo/data/ciudades.csv")

	with open(ruta_archivo_csv) as archivo:

		data=[tuple(linea) for linea in csv.reader(archivo, delimiter=",")]

	kwarg["ti"].xcom_push(key="ciudades", value=[registro[1].strip() for registro in data])

# Funcion para realizar una peticion a la API
def peticion_API(ciudad:str)->Optional[Dict]:

	url=f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={KEY}"

	respuesta=requests.get(url)

	if respuesta.status_code==200:

		return respuesta.json()

	else:
		
		print(respuesta.status_code)

		raise AirflowSkipException(f"Error en la extraccion de la ciudad {ciudad}")

# Funcion para extraer los datos de la API
def extraerData(**kwarg)->None:

	ciudades=kwarg["ti"].xcom_pull(key="ciudades", task_ids="leer_csv")

	for ciudad in ciudades:

		data=peticion_API(ciudad)

		print(data)



with DAG("dag_tiempo", start_date=datetime(2023,12,20), description="DAG para obtener datos de la API de OpenWeather",
		schedule_interval=timedelta(days=1), catchup=False) as dag:

	comprobar_carpeta=BranchPythonOperator(task_id="comprobar_carpeta", python_callable=existe_carpeta)

	crear_carpeta=BashOperator(task_id="crear_carpeta", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir data")

	mover_csv=BashOperator(task_id="mover_csv", bash_command="cd ../../opt/airflow/dags/tiempo && mv 'ciudades.csv' '/opt/airflow/dags/tiempo/data/ciudades.csv'")

	leer_csv=PythonOperator(task_id="leer_csv", python_callable=leerCSV, trigger_rule="none_failed_min_one_success")

	extraccion_data=PythonOperator(task_id="extraccion_data", python_callable=extraerData)


comprobar_carpeta >> [crear_carpeta, leer_csv]

crear_carpeta >> mover_csv >> leer_csv >> extraccion_data
	