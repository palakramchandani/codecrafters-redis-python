import socket 
import threading # noqa: F401


def handle_client(connection,address):

    while True:
        data = connection.recv(1024)
        if not data:
            break
        connection.sendall(b"+PONG\r\n")
def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()
    print("Server is listening on port 6379")

    while True:
        connection, address = server_socket.accept()
        print(f"Accepted connection from {address}")
        thread= threading.Thread(target=handle_client, args=(connection, address))
        thread.daemon = True
        thread.start()  


if __name__ == "__main__":
    main()
