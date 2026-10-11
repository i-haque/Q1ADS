import asyncio
import socket
import rpyc

LB_HOST = "127.0.0.1"
LB_PORT = 8000

# RPC servers
SERVERS = [
    ("127.0.0.1", 8001),
    ("127.0.0.1", 8002),
    ("127.0.0.1", 8003)
]

# Initialize every server as unhealthy
server_health = {server: False for server in SERVERS}

next_server = 0


async def call_rpc_server(host, port, key):

    # RPyC is blocking, so run it in a worker thread
    # instead of blocking the asyncio event loop.
    def rpc_call():
        conn = rpyc.connect(host, port)

        try:
            return conn.root.get_word_frequency(key)
        finally:
            conn.close()

    result = await asyncio.to_thread(rpc_call)

    return result


async def handle_client(client_socket, address):
    global next_server

    loop = asyncio.get_running_loop()

    try:
        # Receive keyword from client
        data = await loop.sock_recv(client_socket, 1024)

        if not data:
            return

        key = data.decode().strip()

        print(f"Request from {address}: {key}")

        # -----------------------------
        # Choose RPC server
        # -----------------------------

        # loop till a healthy server is found
        while server_health[SERVERS[next_server]] == False:
            next_server = (next_server + 1) % len(SERVERS)

        host, port = SERVERS[next_server]

        # Move to next server for next request
        next_server = (next_server + 1) % len(SERVERS)

        print(f"Forwarding '{key}' to {host}:{port}")

        # -----------------------------
        # Call RPC server
        # -----------------------------

        result = await call_rpc_server(host, port, key)

        print(f"Response from {host}:{port}: {result}")

        # -----------------------------
        # Send result back to client
        # -----------------------------

        response = str(result).encode()

        await loop.sock_sendall(
            client_socket,
            response
        )

    except Exception as e:
        print(f"Error handling {address}: {e}")

    finally:
        client_socket.close()


async def health_check(server):
    host, port = server
    health_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    health_socket.setblocking(False)

    try:
        loop = asyncio.get_running_loop()
        await asyncio.wait_for(
            loop.sock_connect(health_socket, (host, port)),
            timeout=2.0
        )
        return True

    except (OSError, asyncio.TimeoutError):
        return False

    finally:
        health_socket.close()


async def helth_monitor():
    while True:
        checkup_status = await asyncio.gather(
            *(health_check(server) for server in SERVERS)
        )

        for (server, health_status) in zip(SERVERS, checkup_status):
            server_health[server] = health_status
            print(f"{server}: {health_status}")

        await asyncio.sleep(5.0)


async def init_load_balancer():

    # Create TCP socket
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    # Allow reuse of the address
    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    # Bind LB socket
    server_socket.bind(
        (LB_HOST, LB_PORT)
    )

    # Start listening
    server_socket.listen(10)

    # Make socket non-blocking
    server_socket.setblocking(False)

    loop = asyncio.get_running_loop()

    print(
        f"Load balancer listening on "
        f"{LB_HOST}:{LB_PORT}"
    )

    while True:

        # Accept client connection
        client_socket, address = await loop.sock_accept(
            server_socket
        )

        print(f"Client connected: {address}")

        # Handle client concurrently
        asyncio.create_task(
            handle_client(
                client_socket,
                address
            )
        )


async def main():
    monitor = asyncio.create_task(helth_monitor())

    try:
        await init_load_balancer()

    finally:
        monitor.cancel()
        await asyncio.gather(
            monitor,
            return_exceptions=True
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down load balancer!")
