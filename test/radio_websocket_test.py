#!/usr/bin/env python3
import base64
import hashlib
import socket
import struct
import subprocess
import sys
import threading


HOST = "127.0.0.1"
PORT = 20400
PAYLOAD = b'{"envelope_id":"ack-123"}'


def read_frame(connection):
    header = connection.recv(2)
    if len(header) != 2:
        raise AssertionError(f"incomplete websocket frame header: {header!r}")
    first, second = header
    opcode = first & 0x0F
    masked = second & 0x80
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", connection.recv(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", connection.recv(8))[0]
    mask = connection.recv(4) if masked else b""
    frame = bytearray()
    while len(frame) < length:
        chunk = connection.recv(length - len(frame))
        if not chunk:
            raise AssertionError("websocket closed before the complete frame arrived")
        frame.extend(chunk)
    if masked:
        frame = bytearray(value ^ mask[index % 4] for index, value in enumerate(frame))
    return opcode, bytes(frame)


def websocket_server(result):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(1)
        server.settimeout(10)
        try:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(10)
                request = b""
                while b"\r\n\r\n" not in request:
                    request += connection.recv(4096)
                key = next(
                    line.split(b":", 1)[1].strip()
                    for line in request.split(b"\r\n")
                    if line.lower().startswith(b"sec-websocket-key:")
                )
                accept = base64.b64encode(
                    hashlib.sha1(
                        key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                    ).digest()
                )
                connection.sendall(
                    b"HTTP/1.1 101 Switching Protocols\r\n"
                    b"Upgrade: websocket\r\n"
                    b"Connection: Upgrade\r\n"
                    b"Sec-WebSocket-Accept: "
                    + accept
                    + b"\r\n\r\n"
                )
                while True:
                    frame = read_frame(connection)
                    if frame[0] == 1:
                        result.append(frame)
                        return
                    if frame[0] == 8:
                        raise AssertionError("websocket closed before a text frame arrived")
        except Exception as error:
            result.append(error)


def main():
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <duckdb-binary>")

    received = []
    server = threading.Thread(target=websocket_server, args=(received,), daemon=True)
    server.start()
    sql = (
        "CALL radio_subscribe('ws://127.0.0.1:20400');"
        "CALL radio_sleep(INTERVAL '1 second');"
        "CALL radio_transmit_message('ws://127.0.0.1:20400', NULL, "
        "'{\"envelope_id\":\"ack-123\"}'::BLOB, 10, "
        "INTERVAL '100 milliseconds');"
        "CALL radio_sleep(INTERVAL '1 second');"
        "CALL radio_unsubscribe('ws://127.0.0.1:20400');"
    )
    completed = subprocess.run(
        [sys.argv[1], "-c", sql], capture_output=True, text=True, check=False
    )
    server.join()
    if completed.returncode != 0:
        raise AssertionError(
            f"DuckDB failed with status {completed.returncode}:\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    if len(received) != 1:
        raise AssertionError(f"expected one websocket frame, received {received!r}")
    if isinstance(received[0], Exception):
        raise received[0]
    opcode, payload = received[0]
    if opcode != 1:
        raise AssertionError(f"expected a text frame (opcode 1), got opcode {opcode}")
    if payload != PAYLOAD:
        raise AssertionError(f"expected raw JSON bytes {PAYLOAD!r}, got {payload!r}")
    print("radio websocket frame test: PASS (text opcode, raw JSON bytes)")


if __name__ == "__main__":
    main()
