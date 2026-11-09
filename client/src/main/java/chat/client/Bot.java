package chat.client;

import chat.protocol.ChannelListResponse;
import chat.protocol.CreateChannelRequest;
import chat.protocol.Envelope;
import chat.protocol.ListChannelsRequest;
import chat.protocol.LoginRequest;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Bot {
    public static void main(String[] args) throws Exception {
        String username = args[0];
        String desiredChannel = args[1];

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5555");

            System.out.printf("Bot %s conectado%n", username);
            login(socket, username);
            ChannelListResponse channels = listChannels(socket, username);
            boolean exists = channels.getChannelsList().stream()
                    .anyMatch(name -> name.equalsIgnoreCase(desiredChannel));
            if (!exists) {
                createChannel(socket, username, desiredChannel);
            }
            listChannels(socket, username);
        }
    }

    private static void login(ZMQ.Socket socket, String username) throws Exception {
        while (true) {
            Envelope request = Envelope.newBuilder()
                    .setSentAtUnixMs(System.currentTimeMillis())
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
        Envelope request = Envelope.newBuilder()
                .setSentAtUnixMs(System.currentTimeMillis())
                .setListChannelsRequest(
                        ListChannelsRequest.newBuilder().setUsername(username))
                .build();
        return exchange(socket, request).getChannelListResponse();
    }

    private static void createChannel(ZMQ.Socket socket, String username, String channelName)
            throws Exception {
        Envelope request = Envelope.newBuilder()
                .setSentAtUnixMs(System.currentTimeMillis())
                .setCreateChannelRequest(CreateChannelRequest.newBuilder()
                        .setUsername(username).setChannelName(channelName))
                .build();
        exchange(socket, request);
    }

    private static Envelope exchange(ZMQ.Socket socket, Envelope request) throws Exception {
        System.out.printf("Enviada: %s%n", request);
        socket.send(request.toByteArray(), 0);
        Envelope response = Envelope.parseFrom(socket.recv(0));
        System.out.printf("Recebida: %s%n", response);
        return response;
    }
}