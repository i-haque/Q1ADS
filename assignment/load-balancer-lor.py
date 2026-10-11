import asyncio
import random
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


class LeastOutstandingRequests:
    def __init__(self, servers):
        self.outstanding_requests = {server: 0 for server in servers}
        self.lock = asyncio.Lock()

    async def acquire_server(self):
        async with self.lock:

            # Filter out healthy servers
            up_servers = []
            for server in self.outstanding_requests.keys():
                if server_health[server]:
                    up_servers.append(server)

            if not up_servers:
                raise RuntimeError

            minimum_outstanding_requests = min(
                self.outstanding_requests[server] for server in up_servers)

            selected_servers = []
            for server in up_servers:
                if self.outstanding_requests[server] == minimum_outstanding_requests:
                    selected_servers.append(server)

            server_selected = random.choice(selected_servers)
            self.outstanding_requests[server_selected] += 1

            return server_selected

    async def release_server(self, server):
        async with self.lock:
            self.outstanding_requests[server] -= 1


# Initialize every server as unhealthy
server_health = {server: False for server in SERVERS}

lb = LeastOutstandingRequests(SERVERS)


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

    loop = asyncio.get_running_loop()

    # -----------------------------
    # Choose RPC server
    # -----------------------------
    server = await lb.acquire_server()

    try:
        # Receive keyword from client
        data = await loop.sock_recv(client_socket, 1024)

        if not data:
            return

        key = data.decode().strip()

        print(f"Request from {address}: {key}")

        host, port = server
        print(f"Forwarding '{key}' to {host}:{port}")

        # -----------------------------
        # Call RPC server
        # -----------------------------

        result = await call_rpc_server(host, port, key)

        print(f"Response from {host}:{port}: {result}")

        # -----------------------------
        # Send result back to client
        # -----------------------------

        response = int.to_bytes(result)

        await loop.sock_sendall(
            client_socket,
            response
        )

    except Exception as e:
        print(f"Error handling {address}: {e}")

    finally:
        await lb.release_server(server)
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
