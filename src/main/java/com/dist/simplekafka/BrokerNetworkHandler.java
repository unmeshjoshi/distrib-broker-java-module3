package com.dist.simplekafka;

import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestOrResponse;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class BrokerNetworkHandler {
    private static final Logger logger = Logger.getLogger(BrokerNetworkHandler.class.getName());

    private final int brokerId;
    private final String host;
    private final int port;
    private final SimpleKafkaApi kafkaApis;
    private TcpListener listener;

    public BrokerNetworkHandler(int brokerId, String host, int port, SimpleKafkaApi kafkaApis) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.kafkaApis = kafkaApis;
    }

    public void startup() {
        listener = new TcpListener(InetAddressAndPort.create(host, port), kafkaApis, this);
        listener.start();
        logger.info("Started socket server");
    }

    public void shutdown() {
        logger.info("Shutting down");
        listener.shutdown();
        logger.info("Shutdown completed");
    }

    public RequestOrResponse sendReceiveTcp(RequestOrResponse message,
                                            InetAddressAndPort to) {
        try (Socket clientSocket = new Socket(to.getAddress(), to.getPort())) {
            return new SocketIO<>(clientSocket, RequestOrResponse.class).requestResponse(message);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static class TcpListener extends Thread {
        private static final Logger logger = Logger.getLogger(TcpListener.class.getName());

        private final InetAddressAndPort localEp;
        private final SimpleKafkaApi kafkaApis;
        private final BrokerNetworkHandler socketServer;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private ServerSocket serverSocket;

        public TcpListener(InetAddressAndPort localEp, SimpleKafkaApi kafkaApis, BrokerNetworkHandler socketServer) {
            this.localEp = localEp;
            this.kafkaApis = kafkaApis;
            this.socketServer = socketServer;
        }

        public void shutdown() {
            Utils.swallow(() -> serverSocket.close());
        }

        @Override
        public void run() {
            Utils.swallow(() -> {
                serverSocket = new ServerSocket();
                serverSocket.bind(new InetSocketAddress(localEp.getAddress(),
                        localEp.getPort()));
                logger.info("Listening on " + localEp);
                while (isRunning.get()) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> processConnection(socket)).start();
                }
            });
        }

        private void processConnection(Socket socket) {
            try {
                new SocketIO<>(socket, RequestOrResponse.class).readHandleRespond(request -> {
                    try {
                        return kafkaApis.handle(request);
                    } catch (Exception e) {
                        throw e;
                    }
                });

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}


