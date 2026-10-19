import rpyc
from rpyc.utils.server import ThreadedServer
import db

HOST = "127.0.0.1"
PORT = 8003


class WordFrequencyService(rpyc.Service):

    def on_connect(self, conn):
        self.redis_connection = db.RedisConnection()

    def on_disconnect(self, conn):
        redis_connection = getattr(self, "redis_connection", None)
        if redis_connection is not None:
            redis_connection.close_connection()

    def exposed_get_word_frequency(self, key):
        print(f"Request received for: {key}")

        count = 0
        # lookup key in the cache
        if self.redis_connection.get(key) == None:
            with open('top-gun-maverick_final.txt', 'r', encoding='utf-8') as file:
                for line in file:
                    count += line.count(key)
            self.redis_connection.set(key, count)
        else:
            print(f"Frequency for {key}: {count}")

        return count

    def exposed_health_check(self):
        return True


if __name__ == "__main__":
    server = ThreadedServer(
        WordFrequencyService,
        hostname=HOST,
        port=PORT
    )

    print(f"RPyC server listening on {HOST}:{PORT}")

    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down server.")
    finally:
        server.close()
