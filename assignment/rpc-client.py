import rpyc
import time
import random

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000

REQUEST_RATES = [10, 30, 50, 70, 90]
DURATION = 10

WORDS = ['all', 'one', 'maverick', 'phoenix', 'bob', 'omaha',
         'rooster', 'hangman', 'fanboy', 'captain', 'iceman']


def run_workload(conn, rate):

    interval = 1.0 / rate
    start = time.perf_counter()
    next_request = start

    total_latency = 0
    requests_sent = 0

    while time.perf_counter() - start < DURATION:

        now = time.perf_counter()

        if now < next_request:
            time.sleep(next_request - now)

        word = random.choice(WORDS)

        req_start_time = time.perf_counter()

        count = conn.root.get_word_frequency(word)
        print(f'{word}: {count}')

        req_end_time = time.perf_counter()
        total_latency += (req_end_time - req_start_time) * 1000

        requests_sent += 1
        next_request += interval

    elapsed = time.perf_counter() - start

    print(f"Target rate: {rate} requests/sec")
    print(f"Requests sent: {requests_sent}")
    print(f"Actual rate: {requests_sent / elapsed:.2f} requests/sec")
    print(f'Avg Latency: {total_latency / requests_sent:.3f} ms')
    print("-----------------------------")


def client():

    connection = rpyc.connect(SERVER_HOST, SERVER_PORT)

    try:
        for rate in REQUEST_RATES:

            print(f"\nStarting workload: {rate} requests/sec")

            run_workload(connection, rate)

            time.sleep(3)

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        client()
    except KeyboardInterrupt:
        pass
