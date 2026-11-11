import sqlite3
import sys
import time

import zmq

import chat_pb2
import common
from common import log, receive, stamp

server_name = sys.argv[1]
coordinator = None
message_count = 0

db = sqlite3.connect("/data/server.sqlite")
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS logins (username TEXT, sent_at INTEGER);
    CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY COLLATE NOCASE, sent_at INTEGER);
    CREATE TABLE IF NOT EXISTS messages (username TEXT, channel_name TEXT, message TEXT, sent_at INTEGER);
    """
)

context = zmq.Context()
work = context.socket(zmq.REP)
peer = context.socket(zmq.REP)
peer.bind("tcp://*:5560")
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt(zmq.SUBSCRIBE, b"servers")
sub.setsockopt(zmq.SUBSCRIBE, b"replication")
reference = context.socket(zmq.REQ)
reference.connect("tcp://reference:5559")


def call_reference(**payload):
    request = stamp(**payload)
    log("-> reference", request)
    reference.send(request.SerializeToString())
    response = receive(reference.recv())
    log("<- reference", response)
    return response


def ask_peer(peer_name, **payload):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 1000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(f"tcp://{peer_name}:5560")
    request = stamp(**payload)
    log(f"-> {peer_name}", request)
    socket.send(request.SerializeToString())
    try:
        response = receive(socket.recv())
        log(f"<- {peer_name}", response)
    except zmq.Again:
        response = None
        print(f"{peer_name} nao respondeu", flush=True)
    socket.close()
    return response


def publish(topic, **payload):
    envelope = stamp(**payload)
    pub.send_multipart([topic.encode(), envelope.SerializeToString()])
    log(f"PUB {topic}", envelope)


def elect():
    global coordinator
    coordinator = None
    servers = call_reference(
        list_servers_request=chat_pb2.ListServersRequest()
    ).list_servers_response.servers
    answers = [
        ask_peer(server.server_name, election_request=chat_pb2.ElectionRequest())
        for server in servers
        if server.rank > rank
    ]
    if all(answer is None for answer in answers):
        coordinator = server_name
        publish(
            "servers",
            coordinator_announcement=chat_pb2.CoordinatorAnnouncement(server_name=server_name),
        )


def sync_clock():
    if coordinator is None or coordinator == server_name:
        return
    started = time.time()
    response = ask_peer(coordinator, clock_request=chat_pb2.ClockRequest())
    if response is None:
        elect()
    else:
        latency_ms = (time.time() - started) * 1000
        common.set_time(response.sent_at_unix_ms + latency_ms / 2)


def store(request):
    kind = request.WhichOneof("payload")
    if kind == "login_request":
        cursor = db.execute(
            "INSERT INTO logins VALUES (?, ?)",
            (request.login_request.username, request.sent_at_unix_ms),
        )
    elif kind == "create_channel_request":
        cursor = db.execute(
            "INSERT OR IGNORE INTO channels VALUES (?, ?)",
            (request.create_channel_request.channel_name, request.sent_at_unix_ms),
        )
    else:
        publication = request.publish_request
        cursor = db.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?)",
            (
                publication.username,
                publication.channel_name,
                publication.message,
                request.sent_at_unix_ms,
            ),
        )
    db.commit()
    return cursor.rowcount == 1


def handle(request):
    kind = request.WhichOneof("payload")

    if kind == "list_channels_request":
        channels = [row[0] for row in db.execute("SELECT name FROM channels ORDER BY name")]
        return stamp(list_channels_response=chat_pb2.ListChannelsResponse(channels=channels))

    if kind == "history_request":
        rows = db.execute(
            "SELECT username, channel_name, message, sent_at FROM messages "
            "WHERE channel_name = ? ORDER BY sent_at",
            (request.history_request.channel_name,),
        )
        messages = [
            chat_pb2.ChannelMessage(
                username=username, channel_name=channel_name, message=message, sent_at_unix_ms=sent_at
            )
            for username, channel_name, message, sent_at in rows
        ]
        return stamp(history_response=chat_pb2.HistoryResponse(messages=messages))

    stored = store(request)
    if stored:
        publish(
            "replication",
            replication_event=chat_pb2.ReplicationEvent(origin_server=server_name, request=request),
        )

    if kind == "login_request":
        return stamp(login_response=chat_pb2.LoginResponse(success=True))
    if kind == "create_channel_request":
        error = "" if stored else "canal ja existe"
        return stamp(
            create_channel_response=chat_pb2.CreateChannelResponse(success=stored, error=error)
        )
    publication = request.publish_request
    publish(
        publication.channel_name,
        channel_message=chat_pb2.ChannelMessage(
            username=publication.username,
            channel_name=publication.channel_name,
            message=publication.message,
            sent_at_unix_ms=request.sent_at_unix_ms,
        ),
    )
    return stamp(publish_response=chat_pb2.PublishResponse(success=True))


# aguarda as conexoes Pub/Sub ficarem ativas antes de registrar e atender
time.sleep(3)
rank = call_reference(
    register_server_request=chat_pb2.RegisterServerRequest(server_name=server_name)
).register_server_response.rank
print(f"{server_name} registrado com rank {rank}", flush=True)
elect()
work.connect("tcp://broker:5556")

poller = zmq.Poller()
for s in (work, peer, sub):
    poller.register(s, zmq.POLLIN)

while True:
    ready = dict(poller.poll())

    if work in ready:
        request = receive(work.recv())
        log("<- cliente", request)
        response = handle(request)
        log("-> cliente", response)
        work.send(response.SerializeToString())
        message_count += 1
        if message_count % 10 == 0:
            call_reference(heartbeat_request=chat_pb2.HeartbeatRequest(server_name=server_name))
        if message_count % 15 == 0:
            sync_clock()

    if peer in ready:
        request = receive(peer.recv())
        log("<- servidor", request)
        is_election = request.WhichOneof("payload") == "election_request"
        if is_election:
            response = stamp(election_response=chat_pb2.ElectionResponse())
        else:
            response = stamp(clock_response=chat_pb2.ClockResponse())
        log("-> servidor", response)
        peer.send(response.SerializeToString())
        if is_election:
            elect()

    if sub in ready:
        topic, data = sub.recv_multipart()
        envelope = receive(data)
        if envelope.WhichOneof("payload") == "coordinator_announcement":
            log("SUB servers", envelope)
            coordinator = envelope.coordinator_announcement.server_name
        elif envelope.replication_event.origin_server != server_name:
            log("SUB replication", envelope)
            store(envelope.replication_event.request)
