package com.dist.simplekafka;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestOrResponse;

import java.io.IOException;
import java.net.Socket;

public class SocketClient {

    public RequestOrResponse sendReceiveTcp(RequestOrResponse message, InetAddressAndPort to) throws IOException {
        try (Socket clientSocket = new Socket(to.getAddress(), to.getPort())) {
            SocketIO<RequestOrResponse> socketIO = new SocketIO<>(clientSocket, RequestOrResponse.class);
            return socketIO.requestResponse(message);
        }
    }
}