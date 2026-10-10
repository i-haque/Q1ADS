import rpyc
from rpyc.utils.server import ThreadedServer

import db

HOST = '127.0.0.1'
PORT = 8003


class WordFrequencyService(rpyc.Service):

    def __init__(self):
        self.redis_connection = db.RedisConnection()

    # the exposed keyword is necessary
    def exposed_get_word_frequency(self, key):
        print(f"Request received for: {key}")

        word_count = self.redis_connection.get_freq(key)

        print(f"Frequency for {key}: {word_count}")

        return word_count


if __name__ == "__main__":

    service = WordFrequencyService()

    server = ThreadedServer(
        WordFrequencyService,
        hostname=HOST,
        port=PORT
    )

    print(f"RPyC server is listening on port: {PORT}")

    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down server!")
    finally:
        service.redis_connection.close_connection()
