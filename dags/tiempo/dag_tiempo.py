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

# Funcion para limpiar los datos
def limpiarDatos(datos:Dict)->Dict:

	tiempo=[valor["main"] for valor in datos["weather"]]

	tiempo_unido=", ".join(tiempo)

	principal=datos["main"]
				
	# Funcion para convertir la temperatura de K a ºC
	def conversion_temperatura(temperatura:float)->float:

		return round(temperatura-273.15, 2)

	# Funcion para convertir la presion de hPa a atm
	def conversion_presion(presion:float)->float:

		return round(presion/1013, 2)

	return {"tiempo":tiempo_unido.lower(),
			"temperatura_media_celsius":conversion_temperatura(principal["temp"]),
			"temperatura_maxima_celsius":conversion_temperatura(principal["temp_max"]),
			"temperatura_minima_celsius":conversion_temperatura(principal["temp_min"]),
			"presion_atm":conversion_presion(principal["pressure"]),
			"humedad_porcentaje":principal["humidity"],
			"viento_m_s":datos["wind"]["speed"]}

# Funcion para transformar los datos de la API
def transformacion(redis:Any=crearConexion())->None:

	ciudades=leerCSV()

	for ciudad in ciudades:

		valores=redis.get(ciudad)

		if valores is not None:

			datos=json.loads(valores.decode())

			datos_limpios=limpiarDatos(datos)

			redis.set(ciudad, json.dumps(datos_limpios))

	print("Transformacion finalizada")

# Funcion para cargar los datos de la API
def carga(redis:Any=crearConexion())->None:

	ciudades=leerCSV()

	for ciudad in ciudades:

		datos=redis.get(ciudad)

		if datos is not None:

			data=json.loads(datos.decode())

			print(data)
			
			

with DAG("dag_tiempo",
		start_date=datetime(2023,12,22),
		description="DAG para obtener datos de la API de OpenWeather",
		schedule_interval=timedelta(days=1),
		catchup=False) as dag:

	comprobar_carpeta=BranchPythonOperator(task_id="comprobar_carpeta", python_callable=existe_carpeta)

	crear_carpeta=BashOperator(task_id="crear_carpeta", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir data")

	mover_csv=BashOperator(task_id="mover_csv", bash_command="cd ../../opt/airflow/dags/tiempo && mv 'ciudades.csv' '/opt/airflow/dags/tiempo/data/ciudades.csv'")

	extraccion_data=PythonOperator(task_id="extraccion_data", python_callable=extraccion, trigger_rule="none_failed_min_one_success")

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion, trigger_rule="none_failed_min_one_success")

	carga_data=PythonOperator(task_id="carga_data", python_callable=carga)


comprobar_carpeta >> [crear_carpeta, extraccion_data]

crear_carpeta >> mover_csv >> extraccion_data >> transformacion_data >> carga_data