from multiprocessing.dummy import connection
import socket 
import threading 
import time
import sys
from collections import defaultdict


waiting_clients = defaultdict(list)
data_store = {}
expiry_store = {}
NULL_BULK_STRING = b'$-1\r\n'

stream_conditions = defaultdict(threading.Condition)

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

def parse_entry_id_with_default(entry_id_str, default_seq_for_end=False):
    MAX_SEQ = 18446744073709551615  # Max 64-bit unsigned int
    MAX_MS = 9223372036854775807   # Approx max int64 ms timestamp (can use a large number)
    if entry_id_str == '+':
        # Return maximum possible ID
        return MAX_MS, MAX_SEQ
    if '-' in entry_id_str:
        ms_str, seq_str = entry_id_str.split('-')
        ms = int(ms_str)
        seq = int(seq_str)
        return ms, seq
    else:
        ms = int(entry_id_str)
        seq = MAX_SEQ if default_seq_for_end else 0
        return ms, seq

def encode_resp_nested_array(entries):
    # Helper to encode the list of stream entries for XRANGE
    # entries is a list of tuples: (id_str, {field: value, ...})
    resp = f"*{len(entries)}\r\n"
    for entry_id, fields_dict in entries:
        # Each entry is an array of 2 elements
        resp += "*2\r\n"
        # First element: entry ID as bulk string
        resp += f"${len(entry_id)}\r\n{entry_id}\r\n"

        # Second element: array of field-value strings
        # Must preserve insertion order, so we use iteration order of dict
        n_fields = len(fields_dict) * 2
        resp += f"*{n_fields}\r\n"
        for field, value in fields_dict.items():
            resp += f"${len(field)}\r\n{field}\r\n"
            resp += f"${len(value)}\r\n{value}\r\n"

    return resp.encode()
    
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
def execute_command(command_parts):
    """Execute a single command and return its response as a string"""
    cmd = command_parts[0].upper()
    
    if cmd == 'SET' and len(command_parts) >= 3:
        key, value = command_parts[1], command_parts[2]
        expiry = None
        if len(command_parts) >= 5 and command_parts[3].upper() == 'PX':
            try:
                px = int(command_parts[4])
                expiry = int(time.time() * 1000) + px
            except (ValueError, IndexError):
                return '-ERR invalid PX value\r\n'

        data_store[key] = value
        if expiry is not None:
            expiry_store[key] = expiry
        elif key in expiry_store:
            del expiry_store[key]
        return '+OK\r\n'
    elif cmd == 'INFO':
    # Handle INFO command, specifically for replication section
        if len(command_parts) == 1 or (len(command_parts) == 2 and command_parts[1].lower() == 'replication'):
            # Return replication info with role:master
            info_response = "role:master"
            return f"${len(info_response)}\r\n{info_response}\r\n"
        else:
            # For other sections, return empty for now
            return f"$0\r\n\r\n"
        
    elif cmd == "INCR" and len(command_parts) == 2:
        key = command_parts[1]
        current_time = int(time.time() * 1000)
        if key in expiry_store and current_time > expiry_store[key]:
            del data_store[key]
            del expiry_store[key]
        
        if key in data_store:
            value = data_store[key]
            try:
                new_value = int(value) + 1
                data_store[key] = str(new_value)
                return f":{new_value}\r\n"
            except (ValueError, TypeError):
                return '-ERR value is not an integer or out of range\r\n'
        else:   
            data_store[key] = '1'
            return ":1\r\n"
    
    elif cmd == 'GET' and len(command_parts) == 2:
        key = command_parts[1]
        current_time = int(time.time() * 1000)
        if key in expiry_store and current_time > expiry_store[key]:
            del data_store[key]
            del expiry_store[key]
            return '$-1\r\n'
        elif key in data_store:
            value = data_store[key]
            return f"${len(value)}\r\n{value}\r\n"
        else:
            return '$-1\r\n'
    
    # Add more commands as needed
    elif cmd == 'RPUSH' and len(command_parts) >= 3:
        key = command_parts[1]
        values = command_parts[2:]
        if key not in data_store:
            data_store[key] = values[:]
            length = len(data_store[key]) 
        else:
            if isinstance(data_store[key], list):
                data_store[key].extend(values)
                length = len(data_store[key])
            else:
                return '-ERR value is not a list\r\n'
        return f':{length}\r\n'
    
    # Default case - return error for unimplemented commands
    return '-ERR unknown command\r\n'
def handle_client(connection, address):
    in_multi = False
    queued_commands = []
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            command_parts = parse_resp(data)
            if not command_parts:
                continue
            
            cmd = command_parts[0].upper()
            
            # Handle MULTI command
            if cmd == "MULTI" and len(command_parts) == 1:
                in_multi = True
                queued_commands.clear()  # Clear any previous queued commands
                connection.sendall(b'+OK\r\n')
                continue
            
            # Handle EXEC command
            elif cmd == "EXEC" and len(command_parts) == 1:
                if not in_multi:
                    connection.sendall(b'-ERR EXEC without MULTI\r\n')
                    continue
                
                # Execute all queued commands and collect their responses
                responses = []
                for queued_cmd_parts in queued_commands:
                    response = execute_command(queued_cmd_parts)
                    responses.append(response)
                
                # Send array of responses
                if len(responses) == 0:
                    connection.sendall(b'*0\r\n')
                else:
                    # Build RESP array response
                    resp = f"*{len(responses)}\r\n"
                    for response in responses:
                        resp += response.decode() if isinstance(response, bytes) else response
                    connection.sendall(resp.encode())
                
                in_multi = False
                queued_commands.clear()
                continue
            elif cmd == "DISCARD" and len(command_parts) == 1:
                if not in_multi:
                    connection.sendall(b'-ERR DISCARD without MULTI\r\n')
                    continue

                    # Discard the transaction - clear queued commands and exit multi mode
                in_multi = False
                queued_commands.clear()
                connection.sendall(b'+OK\r\n')
                continue 
            # If we're in a transaction, queue most commands instead of executing them
            if in_multi:
                # Commands that should be queued (not MULTI/EXEC which are handled above)
                if cmd in ['SET', 'GET', 'INCR', 'RPUSH', 'LPUSH', 'LPOP', 'LLEN', 'LRANGE', 'XADD', 'XRANGE', 'XREAD', 'TYPE', 'BLPOP','INFO']:
                    queued_commands.append(command_parts)
                    connection.sendall(b'+QUEUED\r\n')
                    continue
                # Commands like PING, ECHO might still execute immediately, but for this task let's queue them too
                else:
                    queued_commands.append(command_parts)
                    connection.sendall(b'+QUEUED\r\n')
                    continue
            # Handle DISCARD command
         

            # All other commands follow...
            if cmd == 'SET' and len(command_parts) >= 3:
                key, value = command_parts[1], command_parts[2]
                expiry = None
                if len(command_parts) >= 5 and command_parts[3].upper() == 'PX':
                    try:
                        px = int(command_parts[4])
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

            
            elif cmd == "INCR" and len(command_parts) == 2:
                key = command_parts[1]
                # Check if key exists and get current time for expiry check
                current_time = int(time.time() * 1000)
                if key in expiry_store and current_time > expiry_store[key]:
                    del data_store[key]
                    del expiry_store[key]
                
                if key in data_store:
                    value = data_store[key]
                    try:
                        new_value = int(value) + 1
                        data_store[key] = str(new_value)
                        connection.sendall(f":{new_value}\r\n".encode())
                    except (ValueError, TypeError):
                        connection.sendall(b'-ERR value is not an integer or out of range\r\n')
                else:   
                    data_store[key] = '1'
                    connection.sendall(b":1\r\n")

            elif cmd == 'INFO':
    # Handle INFO command, specifically for replication section
                if len(command_parts) == 1 or (len(command_parts) == 2 and command_parts[1].lower() == 'replication'):
        # Return replication info with role:master
                    info_response = "role:master"
                    response = to_bulk_string(info_response)
                    connection.sendall(response)
                else:
        # For other sections, return empty for now
                    response = to_bulk_string("")
                    connection.sendall(response)

            elif cmd == "XRANGE" and len(command_parts) >= 4:
                key = command_parts[1]
                start_id_str = command_parts[2]
                end_id_str = command_parts[3]

                if start_id_str == '-':
                    start_id_str = '0-0'

                # Check stream existence
                if key not in data_store or not is_stream(data_store[key]):
                    # Return empty array if key missing or not a stream
                    connection.sendall(b"*0\r\n")
                    continue

                stream = data_store[key]

                # Parse IDs with proper default sequence numbers
                start_ms, start_seq = parse_entry_id_with_default(start_id_str, default_seq_for_end=False)
                end_ms, end_seq = parse_entry_id_with_default(end_id_str, default_seq_for_end=True)

                # Build the result entries list
                result_entries = []

                # Iterate over stream entries (assumed sorted by insertion)
                for entry_id, fields in stream:
                    # Parse current entry id
                    entry_ms, entry_seq = parse_entry_id(entry_id)
                    # Compare with start and end (inclusive)
                    # Check: start_id <= entry_id <= end_id
                    # Use (ms, seq) tuple comparisons
                    if (entry_ms > end_ms) or (entry_ms == end_ms and entry_seq > end_seq):
                        # Passed end of range, stop early (assuming sorted stream)
                        break
                    if (entry_ms > start_ms) or (entry_ms == start_ms and entry_seq >= start_seq):
                        # Within range, add
                        result_entries.append((entry_id, fields))

                # Encode and send response
                response = encode_resp_nested_array(result_entries)
                connection.sendall(response)

            elif cmd == 'RPUSH' and len(command_parts) >= 3:
                key = command_parts[1]
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
                        continue
                    
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

            elif cmd == "TYPE" and len(command_parts) == 2:
                key = command_parts[1]
                if key not in data_store:
                    connection.sendall(b'+none\r\n')
                else:
                    value = data_store[key]
                    if is_stream(value):
                        connection.sendall(b'+stream\r\n')
                    elif isinstance(value, list):
                        connection.sendall(b'+list\r\n')
                    else:
                        connection.sendall(b'+string\r\n')

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

            elif cmd == 'XREAD':
                block = None # None means no blocking, 0 means block forever
                idx = 1

                # Detect BLOCK option if present
                if len(command_parts) > 2 and command_parts[1].upper() == 'BLOCK':
                    try:
                        block = int(command_parts[2])
                        idx = 3
                    except ValueError:
                        connection.sendall(b'-ERR invalid BLOCK timeout\r\n')
                        continue

                # Next token must be 'STREAMS'
                if len(command_parts) <= idx or command_parts[idx].upper() != 'STREAMS':
                    connection.sendall(b'-ERR syntax error\r\n')
                    continue

                idx += 1
                num_streams = (len(command_parts) - idx) // 2
                keys = command_parts[idx:idx + num_streams]
                last_ids = command_parts[idx + num_streams: idx + num_streams * 2]

                if len(keys) != len(last_ids):
                    connection.sendall(b'-ERR number of streams and IDs do not match\r\n')
                    continue

                # Resolve '$' IDs at the start of XREAD
                resolved_last_ids = []
                for stream_key, last_id_str in zip(keys, last_ids):
                    if last_id_str == '$':
                        if stream_key in data_store and is_stream(data_store[stream_key]):
                            stream = data_store[stream_key]
                            if stream:
                                # Use the last entry ID in the stream at this moment
                                resolved_last_ids.append(stream[-1][0])
                            else:
                                # Empty stream
                                resolved_last_ids.append('0-0')
                        else:
                            # Stream doesn't exist
                            resolved_last_ids.append('0-0')
                    else:
                        resolved_last_ids.append(last_id_str)

                def check_new_entries():
                    results = []
                    for stream_key, last_id_str in zip(keys, resolved_last_ids):
                        if stream_key not in data_store or not is_stream(data_store[stream_key]):
                            results.append((stream_key, []))
                            continue
                        stream = data_store[stream_key]

                        parsed = parse_entry_id(last_id_str)
                        if parsed is None:
                            connection.sendall(b'-ERR invalid ID format\r\n')
                            return None
                        last_ms, last_seq = parsed
                        entries = []
                        for entry_id, fields in stream:
                            ms, seq = parse_entry_id(entry_id)
                            if (ms > last_ms) or (ms == last_ms and seq > last_seq):
                                entries.append((entry_id, fields))
                        results.append((stream_key, entries))
                    return results

                # Check for new entries immediately
                resp_data = check_new_entries()
                if resp_data is None:
                    continue  # error already sent

                # If any entries found or no blocking requested, send response immediately
                if any(len(entries) > 0 for _, entries in resp_data) or block is None:
                    if all(len(entries) == 0 for _, entries in resp_data):
                        # No entries found and no blocking: respond with empty array
                        connection.sendall(b'*0\r\n')
                    else:
                        resp = f"*{len(resp_data)}\r\n"
                        for stream_key, entries in resp_data:
                            resp += f"*2\r\n"
                            resp += f"${len(stream_key)}\r\n{stream_key}\r\n"
                            resp += f"*{len(entries)}\r\n"
                            for entry_id, fields in entries:
                                resp += "*2\r\n"
                                resp += f"${len(entry_id)}\r\n{entry_id}\r\n"
                                resp += f"*{len(fields)*2}\r\n"
                                for field, value in fields.items():
                                    resp += f"${len(field)}\r\n{field}\r\n"
                                    resp += f"${len(value)}\r\n{value}\r\n"
                        connection.sendall(resp.encode())
                else:
                    # Blocking requested
                    start_time = time.time()
                    timeout_sec = block / 1000.0 if block > 0 else None

                    while True:
                        # Wait for notifications on any of the monitored streams
                        notified = False
                        for key in keys:
                            cond = stream_conditions[key]
                            with cond:
                                if block == 0:
                                    # Wait infinitely
                                    cond.wait()
                                    notified = True
                                    break
                                else:
                                    # Wait with timeout
                                    remaining = timeout_sec - (time.time() - start_time) if timeout_sec else 0
                                    if remaining <= 0:
                                        break
                                    if cond.wait(remaining):
                                        notified = True
                                        break

                        # Check timeout
                        if block > 0:
                            elapsed = time.time() - start_time
                            if elapsed >= timeout_sec:
                                # Timeout expired, no new entries
                                connection.sendall(b"$-1\r\n")
                                break

                        # After waking (or timeout check), check if new entries arrived
                        resp_data = check_new_entries()
                        if resp_data is None:
                            break  # error was sent

                        if any(len(entries) > 0 for _, entries in resp_data):
                            # Send new entries response
                            resp = f"*{len(resp_data)}\r\n"
                            for stream_key, entries in resp_data:
                                resp += f"*2\r\n"
                                resp += f"${len(stream_key)}\r\n{stream_key}\r\n"
                                resp += f"*{len(entries)}\r\n"
                                for entry_id, fields in entries:
                                    resp += "*2\r\n"
                                    resp += f"${len(entry_id)}\r\n{entry_id}\r\n"
                                    resp += f"*{len(fields)*2}\r\n"
                                    for field, value in fields.items():
                                        resp += f"${len(field)}\r\n{field}\r\n"
                                        resp += f"${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                            break

                        # If not notified and no timeout, something went wrong
                        if not notified and block == 0:
                            continue  # Keep waiting for infinite block

            elif cmd == 'LLEN' and len(command_parts) == 2:
                key = command_parts[1]
                if key not in data_store:
                    connection.sendall(b':0\r\n')
                    continue
                elif not isinstance(data_store[key], list):
                    connection.sendall(b'-ERR value is not a list\r\n')
                else:
                    connection.sendall(f':{len(data_store[key])}\r\n'.encode())
            
            elif cmd == 'LPOP':
                if len(command_parts) not in (2, 3):
                    connection.sendall(b'-ERR wrong number of arguments for \'lpop\' command\r\n')
                    continue

                key = command_parts[1]
                count = 1
                if len(command_parts) == 3:
                    try:
                        count = int(command_parts[2])
                        if count < 0:
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
                    
                popped = []
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
                if key in stream_conditions:
                    with stream_conditions[key]:
                        stream_conditions[key].notify_all()
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
                
    except Exception as e:
        print(f"Error handling client {address}: {e}")
    finally:
        connection.close()

def main():
    port = 6379  # default port
    
    # Simple argument parsing for --port
    if len(sys.argv) >= 3 and sys.argv[1] == '--port':
        try:
            port = int(sys.argv[2])
        except ValueError:
            print("Error: Invalid port number")
            sys.exit(1)
    
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    server_socket.listen()
    print(f"Server is listening on port {port}")

    while True:
        connection, address = server_socket.accept()
        print(f"Accepted connection from {address}")
        thread = threading.Thread(target=handle_client, args=(connection, address))
        thread.daemon = True
        thread.start()  

if __name__ == "__main__":
    main()