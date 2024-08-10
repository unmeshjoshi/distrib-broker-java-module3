package com.dist.simplekafka;

import com.dist.common.JsonSerDes;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class SimpleProducer {
    private static final Logger logger = Logger.getLogger(SimpleProducer.class.getName());

    private final InetAddressAndPort bootstrapBroker;
    private final SocketClient socketClient;
    private final AtomicInteger correlationId = new AtomicInteger(0);

    public SimpleProducer(InetAddressAndPort bootstrapBroker) {
        this(bootstrapBroker, new SocketClient());
    }

    public SimpleProducer(InetAddressAndPort bootstrapBroker, SocketClient socketClient) {
        this.bootstrapBroker = bootstrapBroker;
        this.socketClient = socketClient;
    }

    public long produce(String topic, String key, String message) throws IOException {
        return 0;
        //Assignment: Implement Producer

//        Map<TopicAndPartition, PartitionInfo> topicPartitions = fetchTopicMetadata(topic);
//        int partitionId = partitionFor(key, topicPartitions.size());
//        Broker leaderBroker = leaderFor(topic, partitionId, topicPartitions);
//        ProduceRequest produceRequest = new ProduceRequest(new TopicAndPartition(topic, partitionId), key, message);
//        RequestOrResponse producerRequest = new RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(produceRequest), correlationId.incrementAndGet());
//        RequestOrResponse produceResponse =
//                socketClient.sendReceiveTcp(producerRequest,
//                        InetAddressAndPort.create(leaderBroker.host(), leaderBroker.port()));
//        ProduceResponse response = JsonSerDes.deserialize(produceResponse.getMessageBodyJson(), ProduceResponse.class);
//
//        logger.info(String.format("Produced message %s -> %s on leader broker %s. Message offset is %d", key, message, leaderBroker, response.getOffset()));
//        return response.getOffset();
    }

    private Map<TopicAndPartition, PartitionInfo> fetchTopicMetadata(String topic) throws IOException {
        RequestOrResponse request = new RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(new TopicMetadataRequest(topic)), correlationId.getAndIncrement());
        RequestOrResponse response = socketClient.sendReceiveTcp(request, bootstrapBroker);
        TopicMetadataResponse topicMetadataResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), TopicMetadataResponse.class);
        return topicMetadataResponse.getTopicPartitions();
    }

    public int partitionFor(String key, int numPartitions) {
        return Math.abs(key.hashCode()) % numPartitions;
    }

    public Broker leaderFor(String topicName, int partitionId, Map<TopicAndPartition, PartitionInfo> topicPartitions) {
        PartitionInfo info = topicPartitions.get(new TopicAndPartition(topicName, partitionId));
        return info.getLeaderBroker();
    }
}
