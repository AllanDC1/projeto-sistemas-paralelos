import sqlite3
import sys
import threading
import time

import zmq
from google.protobuf import text_format

import chat_pb2


LOGICAL_CLOCK = 0
CLOCK_OFFSET_MS = 0
COORDINATOR_NAME = None
SERVER_RANK = 0
ACTIVE_SERVERS = []


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


def set_clock(target_time_ms):
    global CLOCK_OFFSET_MS
    CLOCK_OFFSET_MS = target_time_ms - time.time_ns() // 1_000_000
    print(f"Relogio sincronizado: {now_unix_ms()}", flush=True)


def peer_exchange(context, server_name, payload_field, payload):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.SNDTIMEO, 1_000)
    socket.setsockopt(zmq.RCVTIMEO, 1_000)
    socket.connect(f"tcp://{server_name}:5556")
    request = make_envelope(payload_field, payload)
    print(
        f"Enviada a {server_name}: {text_format.MessageToString(request, as_one_line=True)}",
        flush=True,
    )
    try:
        socket.send(request.SerializeToString())
        response = chat_pb2.Envelope()
        response.ParseFromString(socket.recv())
        update_logical_clock(response)
        print(
            f"Recebida de {server_name}: "
            f"{text_format.MessageToString(response, as_one_line=True)}",
            flush=True,
        )
        return response
    except zmq.Again:
        print(f"Servidor {server_name} nao respondeu", flush=True)
        return None
    finally:
        socket.close(0)


def announce_coordinator(publisher, server_id, server_rank):
    global COORDINATOR_NAME
    COORDINATOR_NAME = server_id
    announcement = make_envelope(
        "coordinator_announcement",
        chat_pb2.CoordinatorAnnouncement(
            server_name=server_id, rank=server_rank
        ),
    )
    publisher.send_multipart([b"servers", announcement.SerializeToString()])
    print(
        f"Enviada no topico servers: "
        f"{text_format.MessageToString(announcement, as_one_line=True)}",
        flush=True,
    )


def get_active_servers(reference_socket):
    response = reference_exchange(
        reference_socket,
        "list_servers_request",
        chat_pb2.ListServersRequest(),
    )
    return list(response.list_servers_response.servers)


def start_election(context, reference_socket, publisher, server_id, server_rank):
    global ACTIVE_SERVERS, COORDINATOR_NAME
    COORDINATOR_NAME = None
    ACTIVE_SERVERS = get_active_servers(reference_socket)
    higher_rank_servers = [
        server
        for server in ACTIVE_SERVERS
        if server.rank > server_rank and server.server_name != server_id
    ]
    higher_rank_responded = False

    for server in higher_rank_servers:
        response = peer_exchange(
            context,
            server.server_name,
            "election_request",
            chat_pb2.ElectionRequest(server_name=server_id, rank=server_rank),
        )
        if response is not None and response.election_response.accepted:
            higher_rank_responded = True

    if not higher_rank_responded:
        announce_coordinator(publisher, server_id, server_rank)


def synchronize_berkeley(
    context,
    reference_socket,
    server_id,
    requester_name=None,
    requester_time=None,
):
    global ACTIVE_SERVERS
    ACTIVE_SERVERS = get_active_servers(reference_socket)
    clock_values = [now_unix_ms()]
    if requester_name is not None:
        clock_values.append(requester_time)

    for server in ACTIVE_SERVERS:
        if server.server_name in (server_id, requester_name):
            continue
        started = time.monotonic()
        response = peer_exchange(
            context,
            server.server_name,
            "clock_request",
            chat_pb2.ClockRequest(server_name=server_id),
        )
        if response is not None and response.HasField("clock_response"):
            round_trip_ms = (time.monotonic() - started) * 1_000
            clock_values.append(
                response.sent_at_unix_ms + int(round_trip_ms / 2)
            )

    average_time = sum(clock_values) // len(clock_values)
    set_clock(average_time)
    return make_envelope(
        "clock_response", chat_pb2.ClockResponse(is_coordinator=True)
    )


def synchronize_with_coordinator(
    context, reference_socket, publisher, server_id, server_rank
):
    if COORDINATOR_NAME == server_id:
        synchronize_berkeley(context, reference_socket, server_id)
        return

    if COORDINATOR_NAME is None:
        start_election(
            context, reference_socket, publisher, server_id, server_rank
        )
        if COORDINATOR_NAME == server_id:
            synchronize_berkeley(context, reference_socket, server_id)
            return
        if COORDINATOR_NAME is None:
            return

    response = peer_exchange(
        context,
        COORDINATOR_NAME,
        "clock_request",
        chat_pb2.ClockRequest(server_name=server_id),
    )
    if (
        response is None
        or not response.HasField("clock_response")
        or not response.clock_response.is_coordinator
    ):
        start_election(context, reference_socket, publisher, server_id, server_rank)
    else:
        set_clock(response.sent_at_unix_ms)


def listen_for_coordinator(context):
    global COORDINATOR_NAME
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://proxy:5558")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"servers")

    while True:
        _, raw_message = subscriber.recv_multipart()
        message = chat_pb2.Envelope()
        message.ParseFromString(raw_message)
        update_logical_clock(message)
        print(
            f"Recebida no topico servers: "
            f"{text_format.MessageToString(message, as_one_line=True)}",
            flush=True,
        )
        if message.HasField("coordinator_announcement"):
            COORDINATOR_NAME = message.coordinator_announcement.server_name
            print(f"Coordenador atual: {COORDINATOR_NAME}", flush=True)


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
    global SERVER_RANK, ACTIVE_SERVERS
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
    SERVER_RANK = registration.register_server_response.rank
    ACTIVE_SERVERS = get_active_servers(reference_socket)
    print(
        f"Servidor {server_id}, rank {SERVER_RANK}; servidores ativos: "
        f"{[server.server_name for server in ACTIVE_SERVERS]}",
        flush=True,
    )

    subscriber_thread = threading.Thread(
        target=listen_for_coordinator, args=(context,), daemon=True
    )
    subscriber_thread.start()
    time.sleep(1)
    start_election(
        context, reference_socket, publisher, server_id, SERVER_RANK
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
            payload = request.WhichOneof("payload")
            start_new_election = False

            if payload == "clock_request":
                if COORDINATOR_NAME == server_id:
                    clock_request = request.clock_request
                    response = synchronize_berkeley(
                        context,
                        reference_socket,
                        server_id,
                        clock_request.server_name,
                        request.sent_at_unix_ms,
                    )
                else:
                    response = make_envelope(
                        "clock_response", chat_pb2.ClockResponse(is_coordinator=False)
                    )
            elif payload == "election_request":
                election = request.election_request
                accepted = election.rank < SERVER_RANK
                response = make_envelope(
                    "election_response",
                    chat_pb2.ElectionResponse(accepted=accepted),
                )
                start_new_election = accepted
            else:
                response = handle_request(request, connection, server_id, publisher)

            print(
                f"Enviada: {text_format.MessageToString(response, as_one_line=True)}",
                flush=True,
            )
            socket.send(response.SerializeToString())
            if start_new_election:
                start_election(
                    context, reference_socket, publisher, server_id, SERVER_RANK
                )

            if payload not in ("clock_request", "election_request"):
                client_message_count += 1
                if client_message_count % 10 == 0:
                    reference_exchange(
                        reference_socket,
                        "heartbeat_request",
                        chat_pb2.HeartbeatRequest(server_name=server_id),
                    )
                if client_message_count % 15 == 0:
                    synchronize_with_coordinator(
                        context,
                        reference_socket,
                        publisher,
                        server_id,
                        SERVER_RANK,
                    )
    finally:
        connection.close()
        socket.close()
        publisher.close()
        reference_socket.close()
        context.term()


if __name__ == "__main__":
    main()