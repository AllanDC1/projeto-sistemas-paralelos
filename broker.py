import zmq

context = zmq.Context()

clients = context.socket(zmq.ROUTER)
clients.bind("tcp://*:5555")

servers = context.socket(zmq.DEALER)
servers.bind("tcp://*:5556")

print("Broker escutando: clientes na 5555, servidores na 5556", flush=True)
zmq.proxy(clients, servers)
