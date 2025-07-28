import socket 
import threading 
import time#
from collections import defaultdict


waiting_clients = defaultdict(list)
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
def encode_resp_array(elements):
    resp = f"*{len(elements)}\r\n"
    for elem in elements:
        resp += f"${len(elem)}\r\n{elem}\r\n"
    return resp.encode()
def parse_entry_id(entry_id):
    try:
        ms_str, seq_str = entry_id.split('-')
        ms = int(ms_str)
        seq = seq_str
        if seq!='*':
            seq = int(seq_str)
        return ms, seq
    except Exception:
        return None
    
def to_bulk_string(message):
    return f"${len(message)}\r\n{message}\r\n".encode()
def is_stream(obj):
                # Helper: True if obj is a stream (list of (id, dict) tuples); False otherwise
                # Rudimentary check only for this challenge!
                if not isinstance(obj, list):
                    return False
                # Empty stream is still a stream
                if len(obj) == 0:
                    return True
                first = obj[0]
                return (isinstance(first, tuple) and len(first) == 2 and isinstance(first[1], dict))
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
                while waiting_clients[key] and isinstance(data_store.get(key), list) and data_store[key]:
                    blocked_connection, blocked_key, event = waiting_clients[key].pop(0)
                    value = data_store[key].pop(0)
                    response = encode_resp_array([key, value])
                    try:
                        blocked_connection.sendall(response)
                    except:
                        pass
                    event.set()  # Notify the waiting client that data is available

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
                if key not in data_store:
                    connection.sendall(b':0\r\n')
                    continue
                elif not isinstance(data_store[key], list):
                    connection.sendall(b'-ERR value is not a list\r\n')
                else:
                    connection.sendall(f':{len(data_store[key])}\r\n'.encode())
            
            
            elif cmd=='LPOP' :
                if len(command_parts) not in (2, 3):
                    connection.sendall(b'-ERR wrong number of arguments for \'lpop\' command\r\n')
                    continue

                key = command_parts[1]
                count=1
                if len(command_parts) == 3:
                    try:
                        count = int(command_parts[2])
                        if count<0:
                            raise ValueError
                    except ValueError:
                        connection.sendall(b'-ERR value is not an integer or out of range\r\n')
                        continue
                if key not in data_store:
                    connection.sendall(b'*0\r\n')
                    continue
                values = data_store[key]
                if not isinstance(values, list):
                    connection.sendall(b'-ERR value is not a list\r\n')
                    continue
                popped=[]
                while len(popped) < count and values:
                    popped.append(values.pop(0))
                if len(command_parts) == 2:
                    if popped:
                        connection.sendall(to_bulk_string(popped[0]))
                    else:
                        connection.sendall(NULL_BULK_STRING)
                else:
                    connection.sendall(encode_resp_array(popped))


            


            elif cmd == "XADD":
                if len(command_parts) < 5 or (len(command_parts)-3) % 2 != 0:
                    connection.sendall(b'-ERR wrong number of arguments for XADD\r\n')
                    continue

                key = command_parts[1]
                entry_id_raw = command_parts[2]
                field_values = command_parts[3:]

                # Auto-generate full ID if entry_id_raw == "*"
                if entry_id_raw == '*':
                    # Get current time in milliseconds
                    ms = int(time.time() * 1000)
                    seq = 0

                    if key in data_store and is_stream(data_store[key]):
                        stream = data_store[key]
                        # Find max sequence number for entries with current millisecond time
                        last_seq = -1
                        for last_entry_id, _ in reversed(stream):
                            last_ms, last_seq_candidate = parse_entry_id(last_entry_id)
                            if last_ms == ms:
                                if last_seq_candidate > last_seq:
                                    last_seq = last_seq_candidate
                            elif last_ms < ms:
                                break
                        if last_seq >= 0:
                            seq = last_seq + 1

                    entry_id = f"{ms}-{seq}"
                else:
                    # Handle previous cases: explicit ID or ms-*
                    parsed = parse_entry_id(entry_id_raw)
                    if parsed is None:
                        connection.sendall(b'-ERR invalid ID format\r\n')
                        continue
                    ms, seq = parsed

                    # Auto-generate sequence number if seq == '*'
                    if seq == '*':
                        last_seq = -1
                        if key in data_store and is_stream(data_store[key]):
                            stream = data_store[key]
                            for last_entry_id, _ in reversed(stream):
                                last_ms, last_seq_candidate = parse_entry_id(last_entry_id)
                                if last_ms == ms:
                                    if last_seq_candidate > last_seq:
                                        last_seq = last_seq_candidate
                                elif last_ms < ms:
                                    break
                        if ms == 0:
                            new_seq = 1
                        else:
                            new_seq = last_seq + 1 if last_seq >= 0 else 0

                        entry_id = f"{ms}-{new_seq}"
                        ms = int(ms)
                        seq = new_seq
                    else:
                        entry_id = entry_id_raw
                        if not (isinstance(seq, int)):
                            connection.sendall(b'-ERR invalid ID format\r\n')
                            continue

                # Check minimal allowed ID
                if ms == 0 and seq == 0:
                    connection.sendall(b'-ERR The ID specified in XADD must be greater than 0-0\r\n')
                    continue

                # Parse fields into dict
                fields = {}
                for i in range(0, len(field_values), 2):
                    fields[field_values[i]] = field_values[i+1]

                # Create stream if missing
                if key not in data_store:
                    data_store[key] = []

                value = data_store[key]
                if not is_stream(value):
                    connection.sendall(b'-ERR key exists and is not a stream\r\n')
                    continue

                # Validate ID ordering if stream has entries
                if value:
                    last_id, _ = value[-1]
                    last_ms, last_seq = parse_entry_id(last_id)
                    if not ((ms > last_ms) or (ms == last_ms and seq > last_seq)):
                        connection.sendall(b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n')
                        continue

                # Append new entry
                data_store[key].append((entry_id, fields))

                # Reply with ID as RESP bulk string
                resp = f"${len(entry_id)}\r\n{entry_id}\r\n".encode()
                connection.sendall(resp)

            elif cmd == 'BLPOP' and len(command_parts) == 3:
                key = command_parts[1]
                try:
                    timeout = float(command_parts[2])
                except ValueError:
                    connection.sendall(b'-ERR value is not a float\r\n')
                    continue
                        
                if key in data_store and isinstance(data_store[key], list) and data_store[key]:
                    value = data_store[key].pop(0)
                    response = encode_resp_array([key, value])
                    connection.sendall(response)
                else:
                    event = threading.Event()
                    waiting_clients[key].append((connection, key, event))
                    if timeout == 0:
                        event.wait() 
                    else: 
                        if not event.wait(timeout):
                            try:
                                waiting_clients[key].remove((connection, key, event))
                            except ValueError:
                                pass
                            try:
                                connection.sendall(NULL_BULK_STRING)
                            except:
                                pass

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
