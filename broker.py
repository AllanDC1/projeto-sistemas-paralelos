import zmq


CLIENT_ENDPOINT = "tcp://*:5555"
SERVER_ENDPOINTS = (
    "tcp://server-1:5556",
    "tcp://server-2:5556",
)


def main():
    context = zmq.Context()
    client_socket = context.socket(zmq.ROUTER)
    client_socket.bind(CLIENT_ENDPOINT)

    server_sockets = []
    for endpoint in SERVER_ENDPOINTS:
        socket = context.socket(zmq.REQ)
        socket.connect(endpoint)
        server_sockets.append(socket)

    client_servers = {}
    next_server = 0
    print(f"Broker escutando em {CLIENT_ENDPOINT}", flush=True)

    try:
        while True:
            client_id, _, request = client_socket.recv_multipart()

            if client_id not in client_servers:
                client_servers[client_id] = next_server
                next_server = (next_server + 1) % len(server_sockets)

            server_index = client_servers[client_id]
            server_socket = server_sockets[server_index]
            server_socket.send(request)
            response = server_socket.recv()
            client_socket.send_multipart([client_id, b"", response])
            print(
                f"Encaminhado cliente {client_id.hex()} para server-{server_index + 1}",
                flush=True,
            )
    finally:
        client_socket.close()
        for socket in server_sockets:
            socket.close()
        context.term()


if __name__ == "__main__":
    main()