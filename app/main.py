import socket 
import threading # noqa: F401

def parse_resp(data):
    lines=data.split(b'\r\n')
    if not lines or lines[0][0] != ord('*'):
        return []
    num_items = int(lines[0][1:])
    parts = []
    i=1
    while len(parts) < num_items and i < len(lines):
        if lines[i].startswith(b'$'):
            length = int(lines[i][1:])
            parts.append(lines[i+1].decode())
            i+=2
        else:
            i+=1
    return parts
def to_bulk_string(message):
    return f"${len(message)}\r\n{message}\r\n".encode()

def handle_client(connection,address):
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            command_parts=parse_resp(data)
            if not command_parts:
                continue
            command = command_parts[0].upper()
            if command =="PING":
                connection.sendall(b"+PONG\r\n")
            elif command == "ECHO" and len(command_parts) >=2:
                message = command_parts[1]
                connection.sendall(to_bulk_string(message))
            else:
                connection.sendall(b"-ERR unknown command\r\n")
    finally:
        connection.close()
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
