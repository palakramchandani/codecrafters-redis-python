import socket 
import threading 
import time# noqa: F401

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
            cmd=command_parts[0].upper()
            if cmd=='SET' and len(command_parts)>= 3:
                key, value = command_parts[1], command_parts[2]
                expiry = None
                if len(command_parts) >=5 and command_parts[3].upper() == 'PX':
                    try:
                        px=int(command_parts[4])
                        expiry = int(time.time() * 1000) + px
                    except (ValueError, IndexError):
                        connection.sendall(b'-ERR invalid PX value\r\n')
                        continue
                elif key in expiry_store:
                    del expiry_store[key]

                data_store[key] = value
                connection.sendall(b'+OK\r\n')
            elif cmd == 'ECHO' and len(command_parts) == 2:
                message = command_parts[1]
                response = to_bulk_string(message)
                connection.sendall(response)
            elif cmd == 'PING':
                connection.sendall(b'+PONG\r\n')


            elif cmd == 'GET' and len(command_parts) == 2:
                key = command_parts[1]
                if key in expiry_store and current_time>expiry_store[key]:
                    del data_store[key]
                    del expiry_store[key]
                    response = b'$-1\r\n'
                elif key in data_store:
                    response = to_bulk_string(data_store[key])
                else:
                    response = b'$-1\r\n'
                connection.sendall(response)
            else:
                connection.sendall(b'-ERR unknown command\r\n')
    finally:
        connection.close()

data_store = {}
expiry_store = {}    
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
