import socket
from process import process_file


def run_server():
    # create a TCP/IP socket
    # AF_INET -> denotes we are using IPv4
    # SOCK_STREAM -> denotes TCP connection
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # reuse of socket: when we close the server by default the OS waits for stray packages for sometime
    # we might get an error "address already in use"
    # use this option to quickly restart server at the same address
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # socket binding
    server_socket.bind(('localhost', 8000))

    # get the hashmap
    freq = process_file()

    # listening for incoming requests
    server_socket.listen(1)
    print("server is listening at port:8000")

    try:
        while True:
            client_socket, client_addr = server_socket.accept()
            print(f'connection recieved from {client_addr}')

            data = client_socket.recv(1024)
            if data:
                key = data.decode('utf-8', errors='ignore')
                print(
                    f'data recieved: {key}')

                # sending response
                word_count = freq[key]
                response = word_count.to_bytes(4, byteorder='big')

            client_socket.sendall(response)
            client_socket.close()

    except KeyboardInterrupt:
        print("\nShutting down server.")

    finally:
        server_socket.close()


if __name__ == "__main__":
    run_server()
