import socket
import db
import asyncio


async def handle_client(client_socket, client_addr, redis_connection):
    try:
        print(f'recieved request from: {client_addr}')

        loop = asyncio.get_running_loop()

        data = await loop.sock_recv(client_socket, 1024)
        if not data:
            return

        key = data.decode('utf-8', errors='ignore')
        print(f'data recieved: {key}')

        word_count = redis_connection.get_freq(key)
        response = word_count.to_bytes(4, byteorder='big')
        await loop.sock_sendall(client_socket, response)

    except Exception as e:
        print(f'error handling {client_addr}: {e}')

    finally:
        client_socket.close()


async def run_server(port):
    redis_connection = db.RedisConnection()

    loop = asyncio.get_running_loop()

    # TCP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # make socket non-blocking
    server_socket.setblocking(False)

    # bind socket
    server_socket.bind(('127.0.0.1', port))
    server_socket.listen(10)
    print(f'server is listening on port: {port}')

    try:
        while True:
            # wait for a client asynchronously
            client_socket, client_addr = await loop.sock_accept(server_socket)
            client_socket.setblocking(False)

            asyncio.create_task(
                handle_client(client_socket, client_addr, redis_connection)
            )

    except Exception as e:
        print(f'\nError: {e}')

    finally:
        redis_connection.close_connection()
        server_socket.close()

if __name__ == "__main__":
    try:
        asyncio.run(run_server(8000))
    except KeyboardInterrupt:
        print(f'\nshutting down server!')
