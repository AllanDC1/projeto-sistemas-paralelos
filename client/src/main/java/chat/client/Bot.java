package chat.client;

import chat.protocol.ChannelListResponse;
import chat.protocol.ChannelMessage;
import chat.protocol.CreateChannelRequest;
import chat.protocol.Envelope;
import chat.protocol.ListChannelsRequest;
import chat.protocol.LoginRequest;
import chat.protocol.PublishRequest;
import chat.protocol.PublishResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Bot {
    private static final Random RANDOM = new Random();
    private static final AtomicLong LOGICAL_CLOCK = new AtomicLong();
    private static final List<String> MESSAGES = List.of(
            "Ola, canal!", "Mensagem do bot", "Testando publicacao", "ZeroMQ na pratica");

    public static void main(String[] args) throws Exception {
        String username = args[0];

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5555");

            System.out.printf("Bot %s conectado%n", username);
            login(socket, username);
            ChannelListResponse channels = listChannels(socket, username);

            if (channels.getChannelsCount() < 5
                    && channels.getChannelsList().stream()
                            .noneMatch(name -> name.equalsIgnoreCase(username))) {
                createChannel(socket, username, username);
            }

            channels = listChannels(socket, username);
            String subscribedChannel = channels.getChannels(
                    RANDOM.nextInt(channels.getChannelsCount()));

            Thread receiver = new Thread(() -> receiveMessages(context, subscribedChannel));
            receiver.setDaemon(true);
            receiver.start();
            Thread.sleep(1_000);

            while (true) {
                String channel = channels.getChannels(
                        RANDOM.nextInt(channels.getChannelsCount()));
                for (int i = 0; i < 10; i++) {
                    String message = MESSAGES.get(RANDOM.nextInt(MESSAGES.size()));
                    publish(socket, username, channel, message);
                    Thread.sleep(1_000);
                }
            }
        }
    }

    private static void login(ZMQ.Socket socket, String username) throws Exception {
        while (true) {
            Envelope request = newEnvelope()
                    .setLoginRequest(LoginRequest.newBuilder().setUsername(username))
                    .build();
            Envelope response = exchange(socket, request);
            if (response.hasLoginResponse() && response.getLoginResponse().getSuccess()) {
                return;
            }
            System.err.printf("Login recusado; nova tentativa em 1 segundo: %s%n", response);
            Thread.sleep(1_000);
        }
    }

    private static ChannelListResponse listChannels(ZMQ.Socket socket, String username)
            throws Exception {
        Envelope request = newEnvelope()
                .setListChannelsRequest(
                        ListChannelsRequest.newBuilder().setUsername(username))
                .build();
        return exchange(socket, request).getChannelListResponse();
    }

    private static void createChannel(ZMQ.Socket socket, String username, String channelName)
            throws Exception {
        Envelope request = newEnvelope()
                .setCreateChannelRequest(CreateChannelRequest.newBuilder()
                        .setUsername(username).setChannelName(channelName))
                .build();
        exchange(socket, request);
    }

    private static void publish(
            ZMQ.Socket socket, String username, String channelName, String message)
            throws Exception {
        Envelope request = newEnvelope()
                .setPublishRequest(PublishRequest.newBuilder()
                        .setUsername(username)
                        .setChannelName(channelName)
                        .setMessage(message))
                .build();
        Envelope response = exchange(socket, request);
        PublishResponse result = response.getPublishResponse();
        if (!result.getSuccess()) {
            System.err.printf("Publicacao recusada: %s%n", result.getError());
        }
    }

    private static void receiveMessages(ZContext context, String channel) {
        ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
        subscriber.connect("tcp://proxy:5558");
        subscriber.subscribe(channel.getBytes(StandardCharsets.UTF_8));
        System.out.printf("Inscrito no canal %s%n", channel);

        while (true) {
            String topic = new String(subscriber.recv(0), StandardCharsets.UTF_8);
            Envelope publication = Envelope.parseFrom(subscriber.recv(0));
            updateLogicalClock(publication);
            ChannelMessage message = publication.getChannelMessage();
            System.out.printf(
                    "Recebida no canal %s: usuario=%s mensagem=%s enviada_em=%d recebida_em=%d%n",
                    topic,
                    message.getUsername(),
                    message.getMessage(),
                    publication.getSentAtUnixMs(),
                    System.currentTimeMillis());
        }
    }

    private static Envelope exchange(ZMQ.Socket socket, Envelope request) throws Exception {
        System.out.printf("Enviada: %s%n", request);
        socket.send(request.toByteArray(), 0);
        Envelope response = Envelope.parseFrom(socket.recv(0));
        updateLogicalClock(response);
        System.out.printf("Recebida: %s%n", response);
        return response;
    }

    private static Envelope.Builder newEnvelope() {
        return Envelope.newBuilder()
                .setSentAtUnixMs(System.currentTimeMillis())
                .setLogicalClock(LOGICAL_CLOCK.incrementAndGet());
    }

    private static void updateLogicalClock(Envelope message) {
        LOGICAL_CLOCK.accumulateAndGet(message.getLogicalClock(), Math::max);
    }
}