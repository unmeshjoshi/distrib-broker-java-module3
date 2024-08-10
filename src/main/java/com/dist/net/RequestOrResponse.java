package com.dist.net;

import com.dist.common.JsonSerDes;

import java.util.Objects;

public final class RequestOrResponse {
    private final short requestId;
    private final byte[] messageBodyJson;
    private final int correlationId;

    public RequestOrResponse(short requestId, byte[] messageBodyJson,
                             int correlationId) {
        this.requestId = requestId;
        this.messageBodyJson = messageBodyJson;
        this.correlationId = correlationId;
    }

    //for jackson
    private RequestOrResponse() {
        this((short) -1, new byte[0], -1);
    }

    public String serialize() {
        return JsonSerDes.toJson(this);
    }


    @Override
    public String toString() {
        return "RequestOrResponse{" +
                "requestId=" + requestId +
                ", messageBodyJson='" + messageBodyJson + '\'' +
                ", correlationId=" + correlationId +
                '}';
    }

    public short getRequestId() {
        return requestId;
    }

    public byte[] getMessageBodyJson() {
        return messageBodyJson;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RequestOrResponse) obj;
        return this.requestId == that.requestId &&
                Objects.equals(this.messageBodyJson, that.messageBodyJson) &&
                this.correlationId == that.correlationId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, messageBodyJson, correlationId);
    }

}