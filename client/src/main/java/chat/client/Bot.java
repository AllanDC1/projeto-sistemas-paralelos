package chat.client;

import chat.protocol.CreateChannelRequest;
import chat.protocol.Envelope;
import chat.protocol.HistoryRequest;
import chat.protocol.ListChannelsRequest;
import chat.protocol.LoginRequest;
import chat.protocol.PublishRequest;
import com.google.protobuf.TextFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Bot {
    private static final Random RANDOM = new Random();
    private static final TextFormat.Printer PRINTER = TextFormat.printer().emittingSingleLine(true);
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    private static long logicalClock = 0;

    public static void main(String[] args) throws Exception {
        String username = args[0];

        Thread.sleep(5000);

        try (ZContext context = new ZContext()) {
            ZMQ.Socket server = context.createSocket(SocketType.REQ);
            server.connect("tcp://broker:5555");
            ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
            subscriber.connect("tcp://proxy:5558");

            send(server, Envelope.newBuilder()
                    .setLoginRequest(LoginRequest.newBuilder().setUsername(username)));

            List<String> channels = listChannels(server, username);
            if (channels.size() < 5 && !channels.contains(username)) {
                send(server, Envelope.newBuilder().setCreateChannelRequest(
                        CreateChannelRequest.newBuilder()
                                .setUsername(username)
                                .setChannelName(username)));
                channels = listChannels(server, username);
            }

            String subscribed = channels.get(RANDOM.nextInt(channels.size()));
            subscriber.subscribe(subscribed);
            System.out.println("Inscrito no canal " + subscribed);
            send(server, Envelope.newBuilder().setHistoryRequest(
                    HistoryRequest.newBuilder().setChannelName(subscribed)));

            while (true) {
                String channel = channels.get(RANDOM.nextInt(channels.size()));
                for (int i = 0; i < 10; i++) {
                    send(server, Envelope.newBuilder().setPublishRequest(
                            PublishRequest.newBuilder()
                                    .setUsername(username)
                                    .setChannelName(channel)
                                    .setMessage("mensagem " + RANDOM.nextInt(1000))));
                    pause(subscriber, 1000);
                }
                pause(subscriber, 10000);
            }
        }
    }

    private static List<String> listChannels(ZMQ.Socket server, String username) throws Exception {
        return send(server, Envelope.newBuilder().setListChannelsRequest(
                ListChannelsRequest.newBuilder().setUsername(username)))
                .getListChannelsResponse().getChannelsList();
    }

    private static Envelope send(ZMQ.Socket socket, Envelope.Builder request) throws Exception {
        request.setSentAtUnixMs(System.currentTimeMillis()).setLogicalClock(++logicalClock);
        log("-> servidor", request.build(), "");
        socket.send(request.build().toByteArray());
        Envelope response = receive(socket.recv());
        log("<- servidor", response, "");
        return response;
    }

    private static Envelope receive(byte[] data) throws Exception {
        Envelope envelope = Envelope.parseFrom(data);
        logicalClock = Math.max(logicalClock, envelope.getLogicalClock());
        return envelope;
    }

    private static void pause(ZMQ.Socket subscriber, long millis) throws Exception {
        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end) {
            receivePublications(subscriber);
            Thread.sleep(50);
        }
    }

    private static void receivePublications(ZMQ.Socket subscriber) throws Exception {
        byte[] topic;
        while ((topic = subscriber.recv(ZMQ.DONTWAIT)) != null) {
            Envelope publication = receive(subscriber.recv());
            String receivedAt = TIME.format(Instant.now());
            log("SUB " + new String(topic), publication, " (recebida " + receivedAt + ")");
        }
    }

    private static void log(String direction, Envelope envelope, String suffix) {
        Envelope payload = envelope.toBuilder().clearSentAtUnixMs().clearLogicalClock().build();
        System.out.printf("%s [relogio %d] %-16s %s%s%n",
                TIME.format(Instant.ofEpochMilli(envelope.getSentAtUnixMs())),
                envelope.getLogicalClock(), direction,
                PRINTER.printToString(payload).strip(), suffix);
    }
}
