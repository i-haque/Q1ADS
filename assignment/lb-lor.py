
import asyncio
import random

LB_HOST = "127.0.0.1"
LB_PORT = 8000

SERVERS = [
    ("127.0.0.1", 8001),
    ("127.0.0.1", 8002),
    ("127.0.0.1", 8003),
]

HEALTH_CHECK_INTERVAL = 5
BUFFER_SIZE = 65536

active_connections = {server: 0 for server in SERVERS}
healthy = {server: False for server in SERVERS}
state_lock = asyncio.Lock()


async def check_server(server):
    host, port = server
    writer = None

    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=2
        )
        return True

    except (OSError, asyncio.TimeoutError):
        return False

    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


async def update_health():
    results = await asyncio.gather(
        *(check_server(server) for server in SERVERS)
    )

    async with state_lock:
        for server, is_healthy in zip(SERVERS, results):
            print(f'{server}: {is_healthy}')
            healthy[server] = is_healthy


async def health_monitor():
    while True:
        await update_health()
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


async def select_server():
    async with state_lock:
        candidates = [
            server
            for server in SERVERS
            if healthy[server]
        ]

        if not candidates:
            return None

        minimum = min(active_connections[candidate]
                      for candidate in candidates)

        least_loaded = [
            candidate
            for candidate in candidates
            if active_connections[candidate] == minimum
        ]

        chosen_server = random.choice(least_loaded)
        active_connections[chosen_server] += 1

        return chosen_server


async def release_server(server):
    async with state_lock:
        active_connections[server] -= 1


async def pipe(reader, writer):
    while True:
        data = await reader.read(BUFFER_SIZE)

        if not data:
            break

        writer.write(data)
        await writer.drain()

    if writer.can_write_eof():
        try:
            writer.write_eof()
            await writer.drain()
        except (OSError, RuntimeError):
            pass


async def handle_client(client_reader, client_writer):
    server = None
    server_writer = None

    try:
        attempted = set()

        for _ in SERVERS:
            server = await select_server()

            if server is None or server in attempted:
                if server is not None:
                    await release_server(server)
                server = None
                break

            attempted.add(server)

            host, port = server

            try:
                server_reader, server_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=3
                )
                break

            except (OSError, asyncio.TimeoutError):
                print(f"Connection failed to backend {server}")

                async with state_lock:
                    healthy[server] = False

                await release_server(server)
                server = None

        if server is None or server_writer is None:
            print("No backend available; rejecting client connection.")
            return

        print(
            f"Forwarding client connection to {server}; "
            f"active connections: {active_connections[server]}"
        )

        await asyncio.gather(
            pipe(client_reader, server_writer),
            pipe(server_reader, client_writer)
        )

    except (OSError, asyncio.CancelledError) as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        print(f"Proxy connection error: {exc}")

    finally:
        if server_writer is not None:
            server_writer.close()
            try:
                await server_writer.wait_closed()
            except OSError:
                pass

        client_writer.close()
        try:
            await client_writer.wait_closed()
        except OSError:
            pass

        if server is not None:
            await release_server(server)


async def main():
    await update_health()

    monitor_task = asyncio.create_task(health_monitor())

    server = await asyncio.start_server(
        handle_client,
        LB_HOST,
        LB_PORT
    )

    print(f"Load balancer listening on {LB_HOST}:{LB_PORT}")

    try:
        async with server:
            await server.serve_forever()
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down load balancer.")
