import redis


class RedisConnection:
    def __init__(self):
        try:
            pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
            self.r = redis.Redis(connection_pool=pool)
            # print(f'testing redis connection: {self.r.ping()}')
        except Exception as e:
            print(f'could not connect to redis: {e}!!!')

    def get_freq(self, key: str) -> int:
        val = self.r.get(key)
        if val is None:
            return 0

        return int(val)

    def close_connection(self):
        self.r.close()
