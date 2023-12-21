from airflow.providers.redis.hooks.redis import RedisHook

# Funcion para crear el hook
def crearHook()->RedisHook:

	"""
	Configuracion de la conexion con Redis

	Connection Id: conexion_redis
	Connection Type: Redis
	Host: redis
	Post: 6379
	"""
	return RedisHook(redis_conn_id="conexion_redis")