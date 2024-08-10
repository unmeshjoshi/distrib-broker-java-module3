package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestOrResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class StubBrokerNetworkHandler extends BrokerNetworkHandler {
    private List<RequestOrResponse> messages;
    private Set<InetAddressAndPort> toAddresses;

    public StubBrokerNetworkHandler(Config config) {
        super(config.getBrokerId(), config.getHostName(), config.getPort(), null);
        this.messages = new ArrayList<>();
        this.toAddresses = new HashSet<>();
    }

    @Override
    public RequestOrResponse sendReceiveTcp(RequestOrResponse message, InetAddressAndPort to) {
        this.messages.add(message);
        this.toAddresses.add(to);
        return new RequestOrResponse(message.getRequestId(), "".getBytes(),
                message.getCorrelationId());
    }

    public List<RequestOrResponse> getMessages() {
        return messages;
    }

    public Set<InetAddressAndPort> getToAddresses() {
        return toAddresses;
    }
}
