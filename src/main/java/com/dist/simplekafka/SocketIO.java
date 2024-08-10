package com.dist.simplekafka;

import com.dist.common.JsonSerDes;
import com.dist.net.RequestOrResponse;

import java.net.Socket;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SocketIO<T> {
    private final Socket clientSocket;
    private final Class<T> responseClass;

    public SocketIO(Socket clientSocket, Class<T> responseClass) throws SocketException {
        this.clientSocket = clientSocket;
        this.responseClass = responseClass;
        this.clientSocket.setSoTimeout(500000);
        this.clientSocket.setKeepAlive(true);
    }

    public void readHandleWithSocket(BiConsumer<T, Socket> handler) throws IOException {
        byte[] responseBytes = read(clientSocket);
        T message = JsonSerDes.deserialize(responseBytes, responseClass);
        handler.accept(message, clientSocket);
    }

    public void readHandleRespond(Function<T, Object> handler) throws IOException {
        byte[] responseBytes = read(clientSocket);
        T message = JsonSerDes.deserialize(responseBytes, responseClass);
        Object response = handler.apply(message);
        write(clientSocket, JsonSerDes.serialize(response));
    }

    private byte[] read(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        int size = dataInputStream.readInt();
        byte[] responseBytes = new byte[size];
        dataInputStream.readFully(responseBytes);
        return responseBytes;
    }

    public T requestResponse(T requestMessage) throws IOException {
        write(clientSocket, JsonSerDes.serialize(requestMessage));
        byte[] responseBytes = read(clientSocket);
        return JsonSerDes.deserialize(responseBytes, responseClass);
    }

    private void write(Socket socket, byte[] serializedMessage) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        DataOutputStream dataStream = new DataOutputStream(outputStream);
        byte[] messageBytes = serializedMessage;
        dataStream.writeInt(messageBytes.length);
        dataStream.write(messageBytes);
        outputStream.flush();
    }
}