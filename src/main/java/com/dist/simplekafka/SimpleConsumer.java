package com.dist.simplekafka;

import com.dist.common.JsonSerDes;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class SimpleConsumer {
    private static final Logger logger = Logger.getLogger(SimpleConsumer.class.getName());

    private final InetAddressAndPort bootstrapBroker;
    private final SocketClient socketClient;
    private final AtomicInteger correlationId = new AtomicInteger(0);
    private final String consumerId = "consumer1";
    private final String groupId = "default";
    private int lastCommittedOffset = 0;

    public SimpleConsumer(InetAddressAndPort bootstrapBroker) {
        this(bootstrapBroker, new SocketClient());
    }

    public SimpleConsumer(InetAddressAndPort bootstrapBroker, SocketClient socketClient) {
        this.bootstrapBroker = bootstrapBroker;
        this.socketClient = socketClient;
    }


    public Map<String, String> consume(String topic, FetchIsolation fetchIsolation) throws IOException {
        return Collections.emptyMap();

        //Assignment: Implement Consumer.

//        Map<String, String> result = new HashMap<>();
//        Map<TopicAndPartition, PartitionInfo> topicMetadata = fetchTopicMetadata(topic);
//        for (Map.Entry<TopicAndPartition, PartitionInfo> tp : topicMetadata.entrySet()) {
//            TopicAndPartition topicPartition = tp.getKey();
//            PartitionInfo partitionInfo = tp.getValue();
//            Broker leader = partitionInfo.getLeaderBroker();
//            RequestOrResponse request = new RequestOrResponse(RequestKeys.FetchKey,
//                    JsonSerDes.serialize(new ConsumeRequest(topicPartition,
//                            fetchIsolation.toString(), 0, -1)),
//                    correlationId.getAndIncrement());
//            RequestOrResponse response = socketClient.sendReceiveTcp(request,
//                    InetAddressAndPort.create(leader.host(), leader.port()));
//            ConsumeResponse consumeResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), ConsumeResponse.class);
//            for (Map.Entry<String, String> m : consumeResponse.getMessages().entrySet()) {
//                if (!m.getKey().equals("producer1")) {
//                    result.put(m.getKey(), m.getValue());
//                }
//            }
//        }
//        return result;
    }

    private Map<TopicAndPartition, PartitionInfo> fetchTopicMetadata(String topic) throws IOException {
        RequestOrResponse request = new RequestOrResponse(RequestKeys.GetMetadataKey,
                JsonSerDes.serialize(new TopicMetadataRequest(topic)), 1);
        RequestOrResponse response = socketClient.sendReceiveTcp(request, bootstrapBroker);
        TopicMetadataResponse topicMetadataResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), TopicMetadataResponse.class);
        return topicMetadataResponse.getTopicPartitions();
    }
}