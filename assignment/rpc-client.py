
import asyncio
import random
import time
import rpyc

LB_HOST = "127.0.0.1"
LB_PORT = 8000

REQUEST_RATES = [10, 30, 50, 70, 90]
DURATION = 10

WORDS = [
    "Maverick", "Rooster", "Hangman", "Phoenix", "Bob",
    "Payback", "Fanboy", "Coyote", "Iceman", "Cyclone",
    "Warlock", "Hondo", "Hammer", "Fritz", "Yale",
    "Omaha", "Goose", "Penny", "Amelia", "Tomcat",
    "Superhornet", "Darkstar", "Dagger", "Bandit", "Harddeck",
    "Dogfight", "Mach", "Centrifuge", "Harddeck", "Ejection",
    "Afterburner", "Radar", "Pitch", "Yaw", "Roll", "Aviator",
    "Wingman", "Carrier", "Tarmac", "Hangar", "Miramar",
    "Northisland", "Sammys", "Canyon", "Uranium", "Bunker",
    "Intercept", "Vector", "Altitude", "Thrust"
]


def rpc_call(word):
    conn = rpyc.connect(
        LB_HOST,
        LB_PORT,
        config={"sync_request_timeout": 5}
    )

    try:
        return conn.root.get_word_frequency(word)
    finally:
        conn.close()


async def run_workload(rate):
    interval = 1.0 / rate
    start = time.perf_counter()
    next_request = start

    total_latency = 0.0
    requests_sent = 0

    while time.perf_counter() - start < DURATION:
        now = time.perf_counter()

        if now < next_request:
            await asyncio.sleep(next_request - now)

        word = random.choice(WORDS)
        request_start = time.perf_counter()

        try:
            count = await asyncio.to_thread(rpc_call, word)
            print(f"{word}: {count}")

        except Exception as exc:
            print(f"Request failed for {word}: {exc}")

        request_end = time.perf_counter()
        total_latency += (request_end - request_start) * 1000
        requests_sent += 1

        next_request += interval

    elapsed = time.perf_counter() - start

    print(f"\nTarget rate: {rate} requests/sec")
    print(f"Requests sent: {requests_sent}")
    print(f"Actual rate: {requests_sent / elapsed:.2f} requests/sec")

    if requests_sent:
        print(f"Avg latency: {total_latency / requests_sent:.3f} ms")

    print("-----------------------------")


async def main():
    for rate in REQUEST_RATES:
        print(f"\nStarting workload: {rate} requests/sec")

        await run_workload(rate)
        await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
