import time

import zmq
from google.protobuf import text_format

import chat_pb2


SERVER_TIMEOUT_SECONDS = 30
LOGICAL_CLOCK = 0


def now_unix_ms():
    return time.time_ns() // 1_000_000


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


def handle_request(request, ranks, last_heartbeat):
    payload = request.WhichOneof("payload")

    if payload == "register_server_request":
        name = request.register_server_request.server_name
        if name not in ranks:
            ranks[name] = len(ranks) + 1
        last_heartbeat[name] = time.monotonic()
        return make_envelope(
            "register_server_response",
            chat_pb2.RegisterServerResponse(rank=ranks[name]),
        )

    if payload == "list_servers_request":
        response = chat_pb2.ListServersResponse()
        for name in sorted(last_heartbeat, key=lambda server: ranks[server]):
            response.servers.add(server_name=name, rank=ranks[name])
        return make_envelope("list_servers_response", response)

    if payload == "heartbeat_request":
        name = request.heartbeat_request.server_name
        success = name in ranks
        if success:
            last_heartbeat[name] = time.monotonic()
        return make_envelope(
            "heartbeat_response",
            chat_pb2.HeartbeatResponse(success=success),
        )

    return make_envelope(
        "error_response", chat_pb2.ErrorResponse(error="Requisicao desconhecida.")
    )


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")
    ranks = {}
    last_heartbeat = {}
    print("Servico de referencia escutando na porta 5559", flush=True)

    try:
        while True:
            now = time.monotonic()
            expired_servers = [
                name
                for name, last_seen in last_heartbeat.items()
                if now - last_seen > SERVER_TIMEOUT_SECONDS
            ]
            for name in expired_servers:
                del last_heartbeat[name]
                print(f"Servidor removido por falta de heartbeat: {name}", flush=True)

            if not socket.poll(1_000, zmq.POLLIN):
                continue

            request = chat_pb2.Envelope()
            request.ParseFromString(socket.recv())
            update_logical_clock(request)
            print(
                f"Recebida: {text_format.MessageToString(request, as_one_line=True)}",
                flush=True,
            )
            response = handle_request(request, ranks, last_heartbeat)
            print(
                f"Enviada: {text_format.MessageToString(response, as_one_line=True)}",
                flush=True,
            )
            socket.send(response.SerializeToString())
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()