from airflow.providers.redis.hooks.redis import RedisHook
from typing import Any

from tiempo.database.hook import crearHook

# Funcion para crear la conexion
def crearConexion(hook:RedisHook=crearHook())->Any:

	return hook.get_conn()