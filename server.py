import sqlite3
import sys
import time

import zmq
from google.protobuf import text_format

import chat_pb2


LOGICAL_CLOCK = 0
CLOCK_OFFSET_MS = 0


def now_unix_ms():
    return time.time_ns() // 1_000_000 + CLOCK_OFFSET_MS


def update_logical_clock(message):
    global LOGICAL_CLOCK
    LOGICAL_CLOCK = max(LOGICAL_CLOCK, message.logical_clock)


def make_envelope(payload_field, payload):
    global LOGICAL_CLOCK
    LOGICAL_CLOCK += 1
    envelope = chat_pb2.Envelope(
        sent_at_unix_ms=now_unix_ms(), logical_clock=LOGICAL_CLOCK
    )
    getattr(envelope, payload_field).CopyFrom(payload)
    return envelope


def reference_exchange(reference_socket, payload_field, payload):
    request = make_envelope(payload_field, payload)
    print(
        f"Enviada a referencia: {text_format.MessageToString(request, as_one_line=True)}",
        flush=True,
    )
    reference_socket.send(request.SerializeToString())
    response = chat_pb2.Envelope()
    response.ParseFromString(reference_socket.recv())
    update_logical_clock(response)
    print(
        f"Recebida da referencia: {text_format.MessageToString(response, as_one_line=True)}",
        flush=True,
    )
    return response


def synchronize_clock(reference_socket, server_id):
    global CLOCK_OFFSET_MS
    response = reference_exchange(
        reference_socket,
        "heartbeat_request",
        chat_pb2.HeartbeatRequest(server_name=server_id),
    )
    if response.heartbeat_response.success:
        CLOCK_OFFSET_MS = (
            response.heartbeat_response.server_time_unix_ms
            - time.time_ns() // 1_000_000
        )
        print(f"Relogio sincronizado: {now_unix_ms()}", flush=True)


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
        CREATE TABLE IF NOT EXISTS publications (
            username TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            message TEXT NOT NULL,
            request_sent_at_unix_ms INTEGER NOT NULL,
            received_at_unix_ms INTEGER NOT NULL,
            published_at_unix_ms INTEGER NOT NULL,
            origin_server TEXT NOT NULL
        );
        """
    )
    connection.commit()
    return connection


def handle_request(request, connection, server_id, publisher):
    payload = request.WhichOneof("payload")

    if payload == "login_request":
        username = request.login_request.username
        connection.execute(
            "INSERT INTO logins VALUES (?, ?, ?)",
            (username, request.sent_at_unix_ms, server_id),
        )
        connection.commit()
        response = chat_pb2.LoginResponse(success=True)
        return make_envelope("login_response", response)

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
        return make_envelope("create_channel_response", response)

    if payload == "list_channels_request":
        response = chat_pb2.ChannelListResponse()
        response.channels.extend(
            row[0]
            for row in connection.execute("SELECT name FROM channels ORDER BY name")
        )
        return make_envelope("channel_list_response", response)

    if payload == "publish_request":
        publication = request.publish_request
        response = chat_pb2.PublishResponse()
        channel = connection.execute(
            "SELECT 1 FROM channels WHERE name = ?", (publication.channel_name,)
        ).fetchone()
        if channel is None:
            response.error = "Canal inexistente."
            return make_envelope("publish_response", response)

        received_at = now_unix_ms()
        published_at = now_unix_ms()
        message = make_envelope(
            "channel_message",
            chat_pb2.ChannelMessage(
                username=publication.username,
                channel_name=publication.channel_name,
                message=publication.message,
            ),
        )
        published_at = message.sent_at_unix_ms
        connection.execute(
            "INSERT INTO publications VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                publication.username,
                publication.channel_name,
                publication.message,
                request.sent_at_unix_ms,
                received_at,
                published_at,
                server_id,
            ),
        )
        connection.commit()
        publisher.send_multipart(
            [publication.channel_name.encode(), message.SerializeToString()]
        )
        print(
            f"Publicada: {text_format.MessageToString(message, as_one_line=True)}",
            flush=True,
        )
        response.success = True
        return make_envelope("publish_response", response)

    return make_envelope(
        "error_response",
        chat_pb2.ErrorResponse(error="Tipo de requisicao desconhecido."),
    )


def main():
    server_id = sys.argv[1]
    connection = create_database()

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5556")
    publisher = context.socket(zmq.PUB)
    publisher.connect("tcp://proxy:5557")
    reference_socket = context.socket(zmq.REQ)
    reference_socket.connect("tcp://reference:5559")

    registration = reference_exchange(
        reference_socket,
        "register_server_request",
        chat_pb2.RegisterServerRequest(server_name=server_id),
    )
    server_rank = registration.register_server_response.rank
    server_list = reference_exchange(
        reference_socket,
        "list_servers_request",
        chat_pb2.ListServersRequest(),
    )
    print(
        f"Servidor {server_id}, rank {server_rank}; servidores ativos: "
        f"{[server.server_name for server in server_list.list_servers_response.servers]}",
        flush=True,
    )

    try:
        client_message_count = 0
        while True:
            raw_request = socket.recv()
            request = chat_pb2.Envelope()
            request.ParseFromString(raw_request)
            update_logical_clock(request)
            print(
                f"Recebida: {text_format.MessageToString(request, as_one_line=True)}",
                flush=True,
            )
            response = handle_request(request, connection, server_id, publisher)
            print(
                f"Enviada: {text_format.MessageToString(response, as_one_line=True)}",
                flush=True,
            )
            socket.send(response.SerializeToString())
            client_message_count += 1
            if client_message_count == 10:
                synchronize_clock(reference_socket, server_id)
                client_message_count = 0
    finally:
        connection.close()
        socket.close()
        publisher.close()
        reference_socket.close()
        context.term()


if __name__ == "__main__":
    main()