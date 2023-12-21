from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import csv
import requests
from typing import Optional, Dict, List, Any
from airflow.exceptions import AirflowSkipException
from airflow.providers.redis.hooks.redis import RedisHook
import json

from tiempo.config import KEY

from tiempo.database.conexion import crearConexion


# Funcion para comprobar la existencia de la carpeta data
def existe_carpeta()->str:

	ruta_comprobacion=os.path.join(os.getcwd(), "dags/tiempo/data")

	return "extraccion_data" if os.path.exists(ruta_comprobacion) else "crear_carpeta"

# Funcion para leer el archivo CSV
def leerCSV()->List[str]:

	ruta_archivo_csv=os.path.join(os.getcwd(), "dags/tiempo/data/ciudades.csv")

	with open(ruta_archivo_csv) as archivo:

		data=[tuple(linea) for linea in csv.reader(archivo, delimiter=",")]

	ciudades=[registro[1].strip() for registro in data]

	return ciudades

# Funcion para realizar una peticion a la API
def peticion_API(ciudad:str)->Optional[Dict]:

	url=f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={KEY}"

	respuesta=requests.get(url)

	if respuesta.status_code==200:

		return respuesta.json()

	print(f"Error en la obtencion de los datos en la API de la ciudad {ciudad}")

# Funcion para extraer los datos de la API
def extraccion(redis:Any=crearConexion())->None:

	ciudades=leerCSV()

	for ciudad in ciudades:
		
		data=peticion_API(ciudad)

		if data is not None:

			redis.set(ciudad, json.dumps(data))

	print("Extraccion finalizada")

# Funcion para transformar los datos de la API
def transformacion(redis:Any=crearConexion())->None:

	ciudades=leerCSV()

	for ciudad in ciudades:

		valor=redis.get(ciudad)

		if valor is not None:

			datos=json.loads(valor.decode())

			print(datos)


with DAG("dag_tiempo",
		start_date=datetime(2023,12,21),
		description="DAG para obtener datos de la API de OpenWeather",
		schedule_interval=timedelta(days=1),
		catchup=False) as dag:

	comprobar_carpeta=BranchPythonOperator(task_id="comprobar_carpeta", python_callable=existe_carpeta)

	crear_carpeta=BashOperator(task_id="crear_carpeta", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir data")

	mover_csv=BashOperator(task_id="mover_csv", bash_command="cd ../../opt/airflow/dags/tiempo && mv 'ciudades.csv' '/opt/airflow/dags/tiempo/data/ciudades.csv'")

	extraccion_data=PythonOperator(task_id="extraccion_data", python_callable=extraccion, trigger_rule="none_failed_min_one_success")

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion)


comprobar_carpeta >> [crear_carpeta, extraccion_data]

crear_carpeta >> mover_csv >> extraccion_data >> transformacion_data