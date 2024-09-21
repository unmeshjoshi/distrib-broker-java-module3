package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.JsonSerDes;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static com.dist.simplekafka.FetchIsolation.FetchHighWatermark;
import static com.dist.simplekafka.FetchIsolation.FetchLogEnd;

class TopicMetadataRequest {
    private final String topicName;

    public TopicMetadataRequest(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    private TopicMetadataRequest() {
        this("");
    }
}

class TopicMetadataResponse {
    private final Map<TopicAndPartition, PartitionInfo> topicPartitions;

    public TopicMetadataResponse(Map<TopicAndPartition, PartitionInfo> topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    public Map<TopicAndPartition, PartitionInfo> getTopicPartitions() {
        return topicPartitions;
    }


    private TopicMetadataResponse() {
        this(Collections.EMPTY_MAP);
    }
}

class ConsumeResponse {
    private final Map<String, String> messages;

    public ConsumeResponse(Map<String, String> messages) {
        this.messages = messages;
    }

    public Map<String, String> getMessages() {
        return messages;
    }

    private ConsumeResponse() {
        this(Collections.EMPTY_MAP); // for jackson.
    }
}

class ConsumeRequest {
    private final TopicAndPartition topicAndPartition;
    private final String isolation;
    private final long offset;
    private final int replicaId;

    public ConsumeRequest(TopicAndPartition topicAndPartition,
                          String isolation, long offset, int replicaId) {
        this.topicAndPartition = topicAndPartition;
        this.isolation = isolation;
        this.offset = offset;
        this.replicaId = replicaId;
    }

    public ConsumeRequest(TopicAndPartition topicAndPartition) {
        this(topicAndPartition, FetchHighWatermark.toString(), 1, -1);
    }

    private ConsumeRequest() {
        this(null);//for jackson
    }

    public TopicAndPartition getTopicAndPartition() {
        return topicAndPartition;
    }

    public String getIsolation() {
        return isolation;
    }

    public long getOffset() {
        return offset;
    }

    public int getReplicaId() {
        return replicaId;
    }
}

class ProduceResponse {
    private final long offset;

    public ProduceResponse(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public ProduceResponse() {
        this(-1);
    }
}

class ProduceRequest {
    private final TopicAndPartition topicAndPartition;
    private final String key;
    private final String message;
    private final long producerId;

    public ProduceRequest(TopicAndPartition topicAndPartition, String key, String message, long producerId) {
        this.topicAndPartition = topicAndPartition;
        this.key = key;
        this.message = message;
        this.producerId = producerId;
    }

    public ProduceRequest(TopicAndPartition topicAndPartition, String key, String message) {
        this(topicAndPartition, key, message,  -1);
    }


    private ProduceRequest() {
        this(null, "", ""); //for jackson.
    }

    public TopicAndPartition getTopicAndPartition() {
        return topicAndPartition;
    }

    public String getKey() {
        return key;
    }

    public String getMessage() {
        return message;
    }

    public long getProducerId() {
        return producerId;
    }
}

public class SimpleKafkaApi {
    Logger logger = LogManager.getLogger(SimpleKafkaApi.class);

    private List<Broker> aliveBrokers = new ArrayList<>();
    private final Map<TopicAndPartition, PartitionInfo> leaderCache = new ConcurrentHashMap<>();
    private static final int DefaultReplicaId = -1;
    private final Config config;
    private final ReplicaManager replicaManager;

    public SimpleKafkaApi(Config config, ReplicaManager replicaManager) {
        this.config = config;
        this.replicaManager = replicaManager;
    }

    public boolean isRequestFromReplica(ConsumeRequest consumeRequest) {
        return consumeRequest.getReplicaId() != DefaultReplicaId;
    }

    public int partitionFor(String key) {
        return Math.abs(key.hashCode()) % config.DefaultNumPartitions;
    }

    public RequestOrResponse handle(RequestOrResponse request) {
        switch (request.getRequestId()) {
            case RequestKeys.LeaderAndIsrKey: {
                LeaderAndReplicaRequest leaderAndReplicasRequest = JsonSerDes.deserialize(
                        request.getMessageBodyJson(), LeaderAndReplicaRequest.class);
                handleLeaderAndReplicas(leaderAndReplicasRequest.leadeReplicas());
                return new RequestOrResponse(RequestKeys.LeaderAndIsrKey, "".getBytes()
                        , request.getCorrelationId());
            }
            case RequestKeys.UpdateMetadataKey: {

                UpdateMetadataRequest updateMetadataRequest = JsonSerDes.deserialize(
                        request.getMessageBodyJson(), UpdateMetadataRequest.class);
                System.out.println("Updating metadata for " + updateMetadataRequest + " on broker" + config.getBrokerId());

                aliveBrokers = new ArrayList<>(updateMetadataRequest.aliveBrokers());
                for (LeaderAndReplicas leaderReplica :
                        updateMetadataRequest.leaderReplicas()) {

                    leaderCache.put(leaderReplica.topicPartition(),
                            leaderReplica.partitionStateInfo());
                }
                return new RequestOrResponse(RequestKeys.UpdateMetadataKey,
                        "".getBytes(), request.getCorrelationId());
            }
            case RequestKeys.GetMetadataKey: {
                TopicMetadataRequest topicMetadataRequest = JsonSerDes.deserialize(
                        request.getMessageBodyJson(), TopicMetadataRequest.class);
                Set<TopicAndPartition> topicAndPartitions = getTopicMetadata(topicMetadataRequest.getTopicName());
                Map<TopicAndPartition, PartitionInfo> partitionInfo = new HashMap<>();
                for (TopicAndPartition tp : topicAndPartitions) {
                    partitionInfo.put(tp, leaderCache.get(tp));
                }
                TopicMetadataResponse topicMetadata = new TopicMetadataResponse(partitionInfo);
                return new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(topicMetadata), request.getCorrelationId());
            }
            case RequestKeys.ProduceKey: {
                ProduceRequest produceRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), ProduceRequest.class);
                Partition partition = replicaManager.getPartition(produceRequest.getTopicAndPartition());
                long offset = partition.append(
                        produceRequest.getKey(), produceRequest.getMessage());
//                waitUntilTrue(() -> offset == partition.highWatermark(),
//                        "Waiting for message to replicate", 1000, 100);
                return new RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(new ProduceResponse(offset)), request.getCorrelationId());
            }
            case RequestKeys.FetchKey: {
                ConsumeRequest consumeRequest = JsonSerDes.deserialize(request.getMessageBodyJson(), ConsumeRequest.class);
                Partition partition = replicaManager.getPartition(consumeRequest.getTopicAndPartition());
                FetchIsolation isolation =
                        FetchIsolation.valueOf(consumeRequest.getIsolation());

                if (isRequestFromReplica(consumeRequest)) {
                    isolation = FetchLogEnd;
                }
                List<Log.Message> rows = (partition == null) ? new ArrayList<>() :
                        partition.read(consumeRequest.getOffset(), consumeRequest.getReplicaId(), isolation);
                if (isRequestFromReplica(consumeRequest) && partition != null) {
                    partition.updateLastReadOffsetAndHighWaterMark(consumeRequest.getReplicaId(), consumeRequest.getOffset());
                }
                Map<String, String> result = new HashMap<>();
                for (Log.Message row : rows) {
                    result.put(new String(row.key), new String(row.value));
                }
                ConsumeResponse consumeResponse = new ConsumeResponse(result);
                return new RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeResponse), request.getCorrelationId());
            }
            default:
                throw new IllegalArgumentException("Invalid request: " + request.getRequestId());
        }
    }

    public void handleLeaderAndReplicas(List<LeaderAndReplicas> leaderReplicas) {
        System.out.println("Handling LeaderAndISR Request in " + config.getBrokerId() + " " + leaderReplicas);

        for (LeaderAndReplicas leaderAndReplicas : leaderReplicas) {
            TopicAndPartition topicAndPartition = leaderAndReplicas.topicPartition();
            Broker leader =
                    leaderAndReplicas.partitionStateInfo().getLeaderBroker();

            if (leader.id() == config.getBrokerId()) {
                replicaManager.makeLeader(topicAndPartition);
            } else {
                replicaManager.makeFollower(topicAndPartition, leader);
            }
        }
    }

    private Set<TopicAndPartition> getTopicMetadata(String topicName) {
        return leaderCache.keySet().stream()
                .filter(topicAndPartition -> topicAndPartition.topic().equals(topicName))
                .collect(Collectors.toSet());
    }

    public void waitUntilTrue(BooleanSupplier condition, String msg,
                              long waitTimeMs, long pause) {
        long startTime = System.currentTimeMillis();

        while (true) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.currentTimeMillis() > startTime + waitTimeMs) {
                throw new RuntimeException(msg);
            }

            try {
                Thread.sleep(Math.min(waitTimeMs, pause));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<Broker> getAliveBrokers() {
        return aliveBrokers;
    }

    public Map getLeaderCache() {
        return leaderCache;
    }

    public void setAliveBrokers(ArrayList arrayList) {
        this.aliveBrokers = arrayList;
    }

    public void setLeaderCache(HashMap<TopicAndPartition, PartitionInfo> topicAndPartitionPartitionInfoHashMap) {
        this.leaderCache.putAll(topicAndPartitionPartitionInfoHashMap);
    }
}
