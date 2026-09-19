import socket

# create a socket and connect it to the address
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(('localhost', 8000))

# send request
client_socket.sendall(b'one')

# recieve response
response = client_socket.recv(4)
print(f'response from server: {int.from_bytes(response, byteorder='big')}')

client_socket.close()
