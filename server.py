import sqlite3
import sys
import time

import zmq
from google.protobuf import text_format

import chat_pb2


def now_unix_ms():
    return time.time_ns() // 1_000_000


def create_database():
    connection = sqlite3.connect("/data/server.sqlite")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS logins (
            username TEXT NOT NULL,
            timestamp_unix_ms INTEGER NOT NULL,
            origin_server TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY COLLATE NOCASE,
            created_by TEXT NOT NULL,
            created_at_unix_ms INTEGER NOT NULL,
            origin_server TEXT NOT NULL
        );
        """
    )
    connection.commit()
    return connection


def make_response(payload_field, payload, timestamp=None):
    response = chat_pb2.Envelope(sent_at_unix_ms=timestamp or now_unix_ms())
    getattr(response, payload_field).CopyFrom(payload)
    return response


def handle_request(request, connection, server_id):
    payload = request.WhichOneof("payload")

    if payload == "login_request":
        username = request.login_request.username
        connection.execute(
            "INSERT INTO logins VALUES (?, ?, ?)",
            (username, request.sent_at_unix_ms, server_id),
        )
        connection.commit()
        response = chat_pb2.LoginResponse(success=True)
        return make_response("login_response", response)

    if payload == "create_channel_request":
        username = request.create_channel_request.username
        channel_name = request.create_channel_request.channel_name
        response = chat_pb2.CreateChannelResponse(channel_name=channel_name)
        cursor = connection.execute(
            "INSERT OR IGNORE INTO channels VALUES (?, ?, ?, ?)",
            (channel_name, username, request.sent_at_unix_ms, server_id),
        )
        connection.commit()
        response.success = cursor.rowcount == 1
        if not response.success:
            response.error = "Ja existe um canal com esse nome."
        return make_response("create_channel_response", response)

    if payload == "list_channels_request":
        response = chat_pb2.ChannelListResponse()
        response.channels.extend(
            row[0]
            for row in connection.execute("SELECT name FROM channels ORDER BY name")
        )
        return make_response("channel_list_response", response)

    return make_response(
        "error_response",
        chat_pb2.ErrorResponse(error="Tipo de requisicao desconhecido."),
    )


def main():
    server_id = sys.argv[1]
    connection = create_database()

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5556")
    print(f"Servidor {server_id} escutando na porta 5556", flush=True)

    try:
        while True:
            raw_request = socket.recv()
            request = chat_pb2.Envelope()
            request.ParseFromString(raw_request)
            print(
                f"Recebida: {text_format.MessageToString(request, as_one_line=True)}",
                flush=True,
            )
            response = handle_request(request, connection, server_id)
            print(
                f"Enviada: {text_format.MessageToString(response, as_one_line=True)}",
                flush=True,
            )
            socket.send(response.SerializeToString())
    finally:
        connection.close()
        socket.close()
        context.term()


if __name__ == "__main__":
    main()