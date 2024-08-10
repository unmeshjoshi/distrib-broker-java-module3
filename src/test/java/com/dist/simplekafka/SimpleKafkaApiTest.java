package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.JsonSerDes;
import com.dist.common.Networks;
import com.dist.common.TestUtils;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class SimpleKafkaApiTest {

    @Test
    public void shouldCreateLeaderAndFollowerReplicas() {
        Config config = new Config(0, new Networks().hostname(),
                TestUtils.choosePort(), "127.0.0.1:2020",
                List.of(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        LeaderAndReplicas leaderAndReplicas = new LeaderAndReplicas(
                new TopicAndPartition("topic1", 0),
                new PartitionInfo(1, List.of(
                        new Broker(0, "10.10.10.10", 8000),
                        new Broker(1, "10.10.10.11", 8000))));

        RequestOrResponse request = new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(new LeaderAndReplicaRequest(List.of(leaderAndReplicas))), 1);

        simpleKafkaApi.handle(request);

        assertEquals(1, replicaManager.getAllPartitions().size());
        Partition partition = replicaManager.getAllPartitions().get(new TopicAndPartition("topic1", 0));
        assertTrue(partition.getLogFile().exists()); // Assuming logFile is a boolean flag
    }

    @Test
    public void testUpdateMetadataCache() {
        Networks networks = new Networks();
        Config config = new Config(0, networks.hostname(), TestUtils.choosePort(),
                "zkConnect", Arrays.asList(TestUtils.tempDir().getAbsolutePath()));

        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        List<Broker> aliveBrokers = Arrays.asList(
                new Broker(0, "10.10.10.10", 8000),
                new Broker(0, "10.10.10.11", 8000),
                new Broker(0, "10.10.10.12", 8000)
        );

        List<LeaderAndReplicas> leaderAndReplicasList = Arrays.asList(
                new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 0),
                        new PartitionInfo(
                               1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                ),
                new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 1),
                        new PartitionInfo(
                               1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                )
        );

        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest(aliveBrokers, leaderAndReplicasList);

        RequestOrResponse request = new RequestOrResponse(RequestKeys.UpdateMetadataKey,
                JsonSerDes.serialize(updateMetadataRequest), 1);

        simpleKafkaApi.handle(request);

        assertEquals(aliveBrokers, simpleKafkaApi.getAliveBrokers());

        PartitionInfo expectedPartitionInfo = new PartitionInfo(
                1,
                Arrays.asList(
                        new Broker(0, "10.10.10.10", 8000),
                        new Broker(1, "10.10.10.11", 8000)
                )
        );

        assertEquals(expectedPartitionInfo,
                simpleKafkaApi.getLeaderCache().get(new TopicAndPartition("topic1", 0)));
    }

    @Test
    public void shouldGetTopicMetadataForGivenTopic() throws Exception {
        Config config = new Config(0, new Networks().hostname(),
                TestUtils.choosePort(), "zkConnect",
                Arrays.asList(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        List<Broker> aliveBrokers = Arrays.asList(
                new Broker(0, "10.10.10.10", 8000),
                new Broker(1, "10.10.10.11", 8000),
                new Broker(2, "10.10.10.12", 8000)
        );

        RequestOrResponse request = new RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(new TopicMetadataRequest("topic1")), 1);

        simpleKafkaApi.setAliveBrokers(new ArrayList<>(aliveBrokers));

        simpleKafkaApi.getLeaderCache().put(
                new TopicAndPartition("topic1", 0),
                new PartitionInfo(
                        0,
                        Arrays.asList(new Broker(0, "10.10.10.10", 8000), new Broker(1, "10.10.10.11", 8000))
                )
        );

        simpleKafkaApi.getLeaderCache().put(
                new TopicAndPartition("topic1", 1),
                new PartitionInfo(
                        1,
                        Arrays.asList(new Broker(0, "10.10.10.10", 8000), new Broker(1, "10.10.10.11", 8000))
                )
        );

        RequestOrResponse response = simpleKafkaApi.handle(request);
        TopicMetadataResponse topicMetadataResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), TopicMetadataResponse.class);

        assertEquals(2, topicMetadataResponse.getTopicPartitions().size());
        assertEquals(
                new PartitionInfo(
                        0,
                        Arrays.asList(new Broker(0, "10.10.10.10", 8000), new Broker(1, "10.10.10.11", 8000))
                ),
                topicMetadataResponse.getTopicPartitions().get(new TopicAndPartition("topic1", 0))
        );
        assertEquals(
                new PartitionInfo(
                        1,
                        Arrays.asList(new Broker(0, "10.10.10.10", 8000), new Broker(1, "10.10.10.11", 8000))
                ),
                topicMetadataResponse.getTopicPartitions().get(new TopicAndPartition("topic1", 1))
        );
    }

    @Test
    public void shouldHandleProduceRequest() {
        Config config = new Config(0, new Networks().hostname(), TestUtils.choosePort(), "zkConnect", Arrays.asList(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        LeaderAndReplicaRequest leaderAndReplicas = new LeaderAndReplicaRequest(
                Arrays.asList(new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 0),
                        new PartitionInfo(
                                1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                ))
        );

        simpleKafkaApi.handle(new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1));
        assertEquals(1, replicaManager.getAllPartitions().size());

        RequestOrResponse request = new RequestOrResponse(
                RequestKeys.ProduceKey,
                JsonSerDes.serialize(new ProduceRequest(new TopicAndPartition("topic1", 0), "key1", "message")),
                1
        );

        RequestOrResponse response = simpleKafkaApi.handle(request);
        ProduceResponse produceResponse = JsonSerDes.deserialize(response.getMessageBodyJson(), ProduceResponse.class);
        assertEquals(1, produceResponse.getOffset());
    }


    @Test
    public void multipleProduceRequestsShouldIncrementOffset() throws Exception {
        Config config = new Config(0, new Networks().hostname(), TestUtils.choosePort(), "zkConnect", Arrays.asList(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        LeaderAndReplicaRequest leaderAndReplicas = new LeaderAndReplicaRequest(
                Arrays.asList(new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 0),
                        new PartitionInfo(
                               1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                ))
        );

        simpleKafkaApi.handle(new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1));
        assertEquals(1, replicaManager.getAllPartitions().size());

        // First produce request
        RequestOrResponse request1 = new RequestOrResponse(
                RequestKeys.ProduceKey,
                JsonSerDes.serialize(new ProduceRequest(new TopicAndPartition("topic1", 0), "key1", "message1")),
                1
        );

        RequestOrResponse response1 = simpleKafkaApi.handle(request1);
        ProduceResponse produceResponse1 = JsonSerDes.deserialize(response1.getMessageBodyJson(), ProduceResponse.class);
        assertEquals(1, produceResponse1.getOffset());

        // Second produce request
        RequestOrResponse request2 = new RequestOrResponse(
                RequestKeys.ProduceKey,
                JsonSerDes.serialize(new ProduceRequest(new TopicAndPartition("topic1", 0), "key2", "message2")),
                1
        );

        RequestOrResponse response2 = simpleKafkaApi.handle(request2);
        ProduceResponse produceResponse2 = JsonSerDes.deserialize(response2.getMessageBodyJson(), ProduceResponse.class);
        assertEquals(2, produceResponse2.getOffset());
    }

    @Test
    public void shouldGetNoMessagesWhenNothingIsProducedOnTheTopicPartition() throws Exception {
        Config config = new Config(0, new Networks().hostname(),
                TestUtils.choosePort(), "zkConnect", Arrays.asList(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        LeaderAndReplicaRequest leaderAndReplicas = new LeaderAndReplicaRequest(
                Arrays.asList(new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 0),
                        new PartitionInfo(
                                1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                ))
        );

        simpleKafkaApi.handle(new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1));
        assertEquals(1, replicaManager.getAllPartitions().size());

        ConsumeRequest consumeRequest = new ConsumeRequest(new TopicAndPartition("topic1", 0));
        RequestOrResponse request = new RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), 1);
        RequestOrResponse consumeResponse = simpleKafkaApi.handle(request);
        ConsumeResponse response = JsonSerDes.deserialize(consumeResponse.getMessageBodyJson(), ConsumeResponse.class);
        assertEquals(0, response.getMessages().size());
    }

    @Test
    public void shouldConsumeMessagesFromStartOffset() throws Exception {
        Config config = new Config(0, new Networks().hostname(),
                TestUtils.choosePort(), "zkConnect", Arrays.asList(TestUtils.tempDir().getAbsolutePath()));
        ReplicaManager replicaManager = new ReplicaManager(config);
        SimpleKafkaApi simpleKafkaApi = new SimpleKafkaApi(config, replicaManager);

        LeaderAndReplicaRequest leaderAndReplicas = new LeaderAndReplicaRequest(
                Arrays.asList(new LeaderAndReplicas(
                        new TopicAndPartition("topic1", 0),
                        new PartitionInfo(
                                1,
                                Arrays.asList(
                                        new Broker(0, "10.10.10.10", 8000),
                                        new Broker(1, "10.10.10.11", 8000)
                                )
                        )
                ))
        );

        simpleKafkaApi.handle(new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1));
        assertEquals(1, replicaManager.getAllPartitions().size());

        produceTwoMessages(simpleKafkaApi);

        ConsumeRequest consumeRequest =
                new ConsumeRequest(new TopicAndPartition("topic1", 0),
                        FetchIsolation.FetchLogEnd.toString(), 0, 2);
        RequestOrResponse request = new RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), 1);
        RequestOrResponse consumeResponse = simpleKafkaApi.handle(request);
        ConsumeResponse response = JsonSerDes.deserialize(consumeResponse.getMessageBodyJson(), ConsumeResponse.class);

        assertEquals(2, response.getMessages().size());
        assertTrue(response.getMessages().containsKey("key1"));
        assertEquals("message1", response.getMessages().get("key1"));
        assertTrue(response.getMessages().containsKey("key2"));
        assertEquals("message2", response.getMessages().get("key2"));
    }

    private void produceTwoMessages(SimpleKafkaApi simpleKafkaApi) throws Exception {
        // Produce first message
        RequestOrResponse request1 = new RequestOrResponse(
                RequestKeys.ProduceKey,
                JsonSerDes.serialize(new ProduceRequest(new TopicAndPartition("topic1", 0), "key1", "message1")),
                1
        );
        RequestOrResponse response1 = simpleKafkaApi.handle(request1);
        ProduceResponse produceResponse1 = JsonSerDes.deserialize(response1.getMessageBodyJson(), ProduceResponse.class);
        assertEquals(1, produceResponse1.getOffset());

        // Produce second message
        RequestOrResponse request2 = new RequestOrResponse(
                RequestKeys.ProduceKey,
                JsonSerDes.serialize(new ProduceRequest(new TopicAndPartition("topic1", 0), "key2", "message2")),
                1
        );
        RequestOrResponse response2 = simpleKafkaApi.handle(request2);
        ProduceResponse produceResponse2 = JsonSerDes.deserialize(response2.getMessageBodyJson(), ProduceResponse.class);
        assertEquals(2, produceResponse2.getOffset());
    }
}
