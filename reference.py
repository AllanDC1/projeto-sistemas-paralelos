import time

import zmq

import chat_pb2
from common import log, receive, stamp

HEARTBEAT_TIMEOUT_S = 30

ranks = {}
last_heartbeat = {}

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")
print("Servico de referencia escutando na porta 5559", flush=True)

while True:
    request = receive(socket.recv())
    log("<- servidor", request)
    kind = request.WhichOneof("payload")

    if kind == "register_server_request":
        server_name = request.register_server_request.server_name
        ranks.setdefault(server_name, len(ranks) + 1)
        last_heartbeat[server_name] = time.time()
        response = stamp(
            register_server_response=chat_pb2.RegisterServerResponse(rank=ranks[server_name])
        )
    elif kind == "list_servers_request":
        active = [
            chat_pb2.ServerInfo(server_name=server_name, rank=rank)
            for server_name, rank in ranks.items()
            if time.time() - last_heartbeat[server_name] < HEARTBEAT_TIMEOUT_S
        ]
        response = stamp(list_servers_response=chat_pb2.ListServersResponse(servers=active))
    else:
        last_heartbeat[request.heartbeat_request.server_name] = time.time()
        response = stamp(heartbeat_response=chat_pb2.HeartbeatResponse())

    log("-> servidor", response)
    socket.send(response.SerializeToString())
