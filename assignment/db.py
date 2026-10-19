import redis


class RedisConnection:
    def __init__(self):
        try:
            # 1. Added decode_responses=True to handle strings automatically
            pool = redis.ConnectionPool(
                host='localhost', port=6379, db=0, decode_responses=True)
            self.r = redis.Redis(connection_pool=pool)

            # 2. Ping the server immediately to actually verify the connection works
            self.r.ping()
        except redis.ConnectionError as e:
            print(f'Could not connect to Redis server: {e}!!!')
            self.r = None

    def get(self, key: str) -> int:
        val = self.r.get(key)
        if val == None:
            return -1
        # 3. Convert the stored string ("260") safely into an actual integer
        return int(val)

    def set(self, key: str, val: int) -> bool:
        return self.r.set(key, str(val))

    def close_connection(self):
        if self.r:
            self.r.close()
