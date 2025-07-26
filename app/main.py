import socket 
import threading 
import time# noqa: F401

data_store = {}
expiry_store = {}
NULL_BULK_STRING = b'$-1\r\n'

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

                data_store[key] = value
                if expiry is not None:
                    expiry_store[key] = expiry
                elif key in expiry_store:
                    del expiry_store[key]
                connection.sendall(b'+OK\r\n')    

            elif cmd == 'RPUSH' and len(command_parts) >= 3:
                key= command_parts[1]
                values = command_parts[2:]
                if key not in data_store:
                    data_store[key] = values[:]
                    length = len(data_store[key]) 
                else:
                    if isinstance(data_store[key], list):
                        data_store[key].extend(values)
                        length = len(data_store[key])
                    else:
                        connection.sendall(b'-ERR value is not a list\r\n')
                        return
                    
                connection.sendall(f':{length}\r\n'.encode())
                
            elif cmd == 'ECHO' and len(command_parts) == 2:
                message = command_parts[1]
                response = to_bulk_string(message)
                connection.sendall(response)
            elif cmd == 'PING':
                connection.sendall(b'+PONG\r\n')

            elif cmd == 'LRANGE' and len(command_parts) == 4:
                    key = command_parts[1]
                    start = int(command_parts[2])
                    end = int(command_parts[3])

                    if key not in data_store or not isinstance(data_store[key], list):
                        connection.sendall(b"*0\r\n")
                        continue

                    lst = data_store[key]
                    if end < 0:
                        end = len(lst) + end
                    result = lst[start:end + 1] if end >= start else []

                    response = f"*{len(result)}\r\n"
                    for item in result:
                        response += f"${len(item)}\r\n{item}\r\n"
                    connection.sendall(response.encode())
            elif cmd == "LPUSH":
                    key = command_parts[1]
                    values = command_parts[2:]
                    if key not in data_store:
                        data_store[key] = []
                    if not isinstance(data_store[key], list):
                        connection.sendall(b'-ERR value is not a list\r\n')
                        continue
                    for val in values:
                        data_store[key].insert(0, val)
                    connection.sendall(f':{len(data_store[key])}\r\n'.encode())

            elif cmd=='LLEN' and len(command_parts) == 2:
                key = command_parts[1]
                if key in data_store:
                    connection.sendall(b':0\r\n')
                    continue
                if not isinstance(data_store[key], list):
                    connection.sendall(b'-ERR value is not a list\r\n')
                    continue
                connection.sendall(f':{len(data_store[key])}\r\n'.encode())

            elif cmd == 'GET' and len(command_parts) == 2:
                key = command_parts[1]
                current_time = int(time.time() * 1000)
                if key in expiry_store and current_time > expiry_store[key]:
                    del data_store[key]
                    del expiry_store[key]
                    response = NULL_BULK_STRING
                elif key in data_store:
                    response = to_bulk_string(data_store[key])
                else:
                    response = b'$-1\r\n'
                connection.sendall(response)
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
