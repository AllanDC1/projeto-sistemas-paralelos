import time
import threading

from google.protobuf import text_format

import chat_pb2

logical_clock = 0
offset_ms = 0  # ajuste do relogio fisico definido na sincronizacao com o coordenador
clock_lock = threading.RLock()


def now_ms():
    with clock_lock:
        return time.time_ns() // 1_000_000 + offset_ms


def hora(ms):
    return time.strftime("%H:%M:%S", time.localtime(ms // 1000)) + f".{ms % 1000:03d}"


def set_time(target_ms):
    global offset_ms
    with clock_lock:
        offset_ms = int(target_ms) - time.time_ns() // 1_000_000
    print(f"Relogio sincronizado: {hora(now_ms())}", flush=True)


def stamp(**payload):
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return chat_pb2.Envelope(
            sent_at_unix_ms=now_ms(), logical_clock=logical_clock, **payload
        )


def receive(data):
    global logical_clock
    envelope = chat_pb2.Envelope()
    envelope.ParseFromString(data)
    with clock_lock:
        logical_clock = max(logical_clock, envelope.logical_clock)
    return envelope


def describe(envelope):
    kind = envelope.WhichOneof("payload")
    if kind == "replication_event":
        event = envelope.replication_event
        return f"{kind} de {event.origin_server}: {describe(event.request)}"
    body = text_format.MessageToString(getattr(envelope, kind), as_one_line=True).strip()
    return f"{kind} {{ {body} }}" if body else f"{kind} {{}}"


def log(direction, envelope):
    print(
        f"{hora(envelope.sent_at_unix_ms)} [relogio {envelope.logical_clock}] "
        f"{direction:<16} {describe(envelope)}",
        flush=True,
    )
