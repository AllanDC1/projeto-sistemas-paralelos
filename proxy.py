import zmq


context = zmq.Context()
xsub = context.socket(zmq.XSUB)
xpub = context.socket(zmq.XPUB)
xsub.bind("tcp://*:5557")
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub escutando nas portas 5557 (XSUB) e 5558 (XPUB)", flush=True)
zmq.proxy(xsub, xpub)