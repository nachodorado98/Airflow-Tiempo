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
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tiempo.config import KEY, LOGS

from tiempo.database.redis.conexion import crearConexion

from tiempo.database.postgres.conexion import crearHook

# Funcion para crear el archivo txt y almacenarlo en la carpeta
def crearLogs(ciudades:List)->None:

	ruta_carpeta_logs=os.path.join(os.getcwd(), LOGS)

	archivo_log=f"log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

	ruta_archivo_log=os.path.join(ruta_carpeta_logs, archivo_log)

	with open(ruta_archivo_log, "w") as archivo:

		archivo.write(f"Fecha de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
		
		for ciudad in ciudades:

			archivo.write(f"Ciudad extraida correctamente: {ciudad}\n")

# Funcion para crear la tabla de los datos del tiempo de las ciudades
def crearTabla(hook:PostgresHook=crearHook())->None:

	hook.run("""CREATE TABLE tiempo (id SERIAL PRIMARY KEY,
									ciudad VARCHAR(100),
									fecha TIMESTAMP,
									tiempo VARCHAR(20),
									temp_media FLOAT,
									temp_max FLOAT,
									temp_min FLOAT,
									presion FLOAT,
									humedad FLOAT,
									viento FLOAT);""")
	
	print("Tabla creada correctamente")

# Funcion para comprobar la existencia de la carpeta data
def existe_carpeta()->str:

	ruta_comprobacion=os.path.join(os.getcwd(), "dags/tiempo/data")

	return "extraccion_data" if os.path.exists(ruta_comprobacion) else "crear_carpeta_data"

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
			"temp_media":conversion_temperatura(principal["temp"]),
			"temp_max":conversion_temperatura(principal["temp_max"]),
			"temp_min":conversion_temperatura(principal["temp_min"]),
			"presion":conversion_presion(principal["pressure"]),
			"humedad":principal["humidity"],
			"viento":datos["wind"]["speed"]}

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
def carga(redis:Any=crearConexion(), hook:PostgresHook=crearHook())->None:

	ciudades=leerCSV()

	ciudades_extraidas=[]

	for ciudad in ciudades:

		datos=redis.get(ciudad)

		if datos is not None:

			data=json.loads(datos.decode())

			hook.run("""INSERT INTO tiempo (ciudad, fecha, tiempo, temp_media, temp_max, temp_min, presion, humedad, viento)
						VALUES %s""",
						parameters=((ciudad,
									datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
									data["tiempo"],
									data["temp_media"],
									data["temp_max"],
									data["temp_min"],
									data["presion"],
									data["humedad"],
									data["viento"]),),)

			ciudades_extraidas.append(ciudad)
	
	crearLogs(ciudades_extraidas)
	
	print("Carga finalizada")
			

with DAG("dag_tiempo",
		start_date=datetime(2023,12,24),
		description="DAG para obtener datos de la API de OpenWeather",
		schedule_interval=timedelta(minutes=60),
		catchup=False) as dag:

	comprobar_carpeta=BranchPythonOperator(task_id="comprobar_carpeta", python_callable=existe_carpeta)

	crear_carpeta_data=BashOperator(task_id="crear_carpeta_data", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir data")

	crear_carpeta_logs=BashOperator(task_id="crear_carpeta_logs", bash_command="cd ../../opt/airflow/dags/tiempo && mkdir logs")

	mover_csv=BashOperator(task_id="mover_csv", bash_command="cd ../../opt/airflow/dags/tiempo && mv 'ciudades.csv' '/opt/airflow/dags/tiempo/data/ciudades.csv'")

	creacion_tabla=PythonOperator(task_id="creacion_tabla", python_callable=crearTabla)

	extraccion_data=PythonOperator(task_id="extraccion_data", python_callable=extraccion, trigger_rule="none_failed_min_one_success")

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion, trigger_rule="none_failed_min_one_success")

	carga_data=PythonOperator(task_id="carga_data", python_callable=carga)


comprobar_carpeta >> [crear_carpeta_data, extraccion_data]

crear_carpeta_data >> mover_csv >> crear_carpeta_logs >> creacion_tabla >> extraccion_data >> transformacion_data >> carga_data