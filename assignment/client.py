import socket
import asyncio
import time
import random

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 8000
WORDS = ['all', 'one', 'maverick', 'phoenix', 'bob', 'omaha',
         'rooster', 'hangman', 'fanboy', 'captain', 'iceman']


async def handle_request():
    try:
        loop = asyncio.get_running_loop()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setblocking(False)

        await loop.sock_connect(client_socket, (SERVER_ADDR, SERVER_PORT))

        word = random.choice(WORDS)
        await loop.sock_sendall(client_socket, word.encode('utf-8'))

        data = await loop.sock_recv(client_socket, 4)

        freq = int.from_bytes(data, byteorder='big')
        return (word, freq)

    finally:
        client_socket.close()


async def main_client():
    start_time = time.perf_counter()

    tasks = [
        asyncio.create_task(
            handle_request()
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(*tasks)
    for (word, freq) in results:
        print(f'({word}, {freq})')

    end_time = time.perf_counter()

    print(f'Total time taken: {(end_time - start_time) * 1000:.2f}ms')

if __name__ == "__main__":
    try:
        asyncio.run(main_client())
    except:
        pass
    finally:
        pass
