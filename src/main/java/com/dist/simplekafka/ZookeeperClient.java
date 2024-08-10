package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.JsonSerDes;
import com.dist.common.ZKStringSerializer;
import com.fasterxml.jackson.core.type.TypeReference;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;

import java.util.*;


final class PartitionInfo {
    private final int leaderBrokerId;
    private final List<Broker> allReplicas;

    PartitionInfo(int leaderBrokerId, List<Broker> allReplicas) {
        this.leaderBrokerId = leaderBrokerId;
        this.allReplicas = allReplicas;
    }

    private PartitionInfo() {
        this(-1, Collections.emptyList());
    }

    public int leaderBrokerId() {
        return leaderBrokerId;
    }

    public Broker getLeaderBroker() {
        return allReplicas.stream().filter(b -> b.id() == leaderBrokerId).findFirst().get();
    }

    public List<Broker> allBrokers() {
        return allReplicas;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PartitionInfo) obj;
        return this.leaderBrokerId == that.leaderBrokerId &&
                Objects.equals(this.allReplicas, that.allReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderBrokerId, allReplicas);
    }

    @Override
    public String toString() {
        return "PartitionInfo[" +
                "partitionId=" + leaderBrokerId + ", " +
                "brokerIds=" + allReplicas + ']';
    }
}

final class LeaderAndReplicas {
    private final TopicAndPartition topicPartition;
    private final PartitionInfo partitionStateInfo;

    public LeaderAndReplicas(TopicAndPartition topicPartition,
                      PartitionInfo partitionStateInfo) {
        this.topicPartition = topicPartition;
        this.partitionStateInfo = partitionStateInfo;
    }

    private LeaderAndReplicas() {
        this(null, null); //Hack for jackson; TODO:Figure out why records
        // dont work.
    }

    public TopicAndPartition topicPartition() {
        return topicPartition;
    }

    public PartitionInfo partitionStateInfo() {
        return partitionStateInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LeaderAndReplicas) obj;
        return Objects.equals(this.topicPartition, that.topicPartition) &&
                Objects.equals(this.partitionStateInfo, that.partitionStateInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, partitionStateInfo);
    }

    @Override
    public String toString() {
        return "LeaderAndReplicas[" +
                "topicPartition=" + topicPartition + ", " +
                "partitionStateInfo=" + partitionStateInfo + ']';
    }
}

final class LeaderAndReplicaRequest {
    private final List<LeaderAndReplicas> leadeReplicas;

    LeaderAndReplicaRequest(List<LeaderAndReplicas> leadeReplicas) {
        this.leadeReplicas = leadeReplicas;
    }

    //for jackson
    private LeaderAndReplicaRequest() {
        this(Collections.emptyList());
    }

    public List<LeaderAndReplicas> leadeReplicas() {
        return leadeReplicas;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LeaderAndReplicaRequest) obj;
        return Objects.equals(this.leadeReplicas, that.leadeReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leadeReplicas);
    }

    @Override
    public String toString() {
        return "LeaderAndReplicaRequest[" +
                "leadeReplicas=" + leadeReplicas + ']';
    }
}

final class UpdateMetadataRequest {
    private final List<Broker> aliveBrokers;
    private final List<LeaderAndReplicas> leaderReplicas;

    UpdateMetadataRequest(List<Broker> aliveBrokers,
                          List<LeaderAndReplicas> leaderReplicas) {
        this.aliveBrokers = aliveBrokers;
        this.leaderReplicas = leaderReplicas;
    }

    //for jackson
    private UpdateMetadataRequest() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    @Override
    public String toString() {
        return "UpdateMetadataRequest{" +
                "aliveBrokers=" + aliveBrokers +
                ", leaderReplicas=" + leaderReplicas +
                '}';
    }

    public List<Broker> aliveBrokers() {
        return aliveBrokers;
    }

    public List<LeaderAndReplicas> leaderReplicas() {
        return leaderReplicas;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (UpdateMetadataRequest) obj;
        return Objects.equals(this.aliveBrokers, that.aliveBrokers) &&
                Objects.equals(this.leaderReplicas, that.leaderReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliveBrokers, leaderReplicas);
    }

}

final class PartitionReplicas {
    private final int partitionId;
    private final List<Integer> brokerIds;

    PartitionReplicas(int partitionId, List<Integer> brokerIds) {
        this.partitionId = partitionId;
        this.brokerIds = brokerIds;
    }

    private PartitionReplicas() {
        this(0, Collections.EMPTY_LIST); //for jackson
    } // Default constructorpublic int partitionId(){ return partitionId; }

    public List<Integer> brokerIds() {
        return brokerIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PartitionReplicas) obj;
        return this.partitionId == that.partitionId &&
                Objects.equals(this.brokerIds, that.brokerIds);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<Integer> getBrokerIds() {
        return brokerIds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, brokerIds);
    }

    @Override
    public String toString() {
        return "PartitionReplicas[" +
                "partitionId=" + partitionId + ", " +
                "brokerIds=" + brokerIds + ']';
    }


}

final class Broker {
    private final int id;
    private final String host;
    private final int port;

    Broker(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    private Broker() { //for jackson
        this(-1, "", -1);
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Broker) obj;
        return this.id == that.id &&
                Objects.equals(this.host, that.host) &&
                this.port == that.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port);
    }

    @Override
    public String toString() {
        return "Broker[" +
                "id=" + id + ", " +
                "host=" + host + ", " +
                "port=" + port + ']';
    }
}

public class ZookeeperClient { // Assuming ZookeeperClientInterface exists
    private static final Logger logger = Logger.getLogger(ZookeeperClient.class);

    public static final String BrokerTopicsPath = "/brokers/topics";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String ControllerPath = "/controller";
    public static final String ReplicaLeaderElectionPath = "/topics/replica/leader";

    private final ZkClient zkClient;
    private final Config config;

    public ZookeeperClient(Config config) {
        this.config = config;
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs(), new ZKStringSerializer());
        zkClient.subscribeStateChanges(new SessionExpireListener());
    }

    public void setPartitionReplicasForTopic(String topicName,
                                             List<PartitionReplicas> partitionReplicas) {
        String topicsPath = getTopicPath(topicName);
        String topicsData = JsonSerDes.toJson(partitionReplicas);
        createPersistentPath(zkClient, topicsPath, topicsData);
    }

    public Set<Integer> getAllBrokerIds() {
        Set<String> brokerIds = new HashSet<>(zkClient.getChildren(BrokerIdsPath));
        Set<Integer> integerBrokerIds = new HashSet<>();
        for (String id : brokerIds) {
            integerBrokerIds.add(Integer.parseInt(id));
        }
        return integerBrokerIds;
    }

    public Set<Broker> getAllBrokers() {
        Set<String> brokerIds = new HashSet<>(zkClient.getChildren(BrokerIdsPath));
        Set<Broker> brokers = new HashSet<>();
        for (String idString : brokerIds) {
            int id = Integer.parseInt(idString);
            String data = zkClient.readData(getBrokerPath(id));
            brokers.add(JsonSerDes.fromJson(data.getBytes(), Broker.class));
        }
        return brokers;
    }

    public Broker getBrokerInfo(int brokerId) {
        String data = zkClient.readData(getBrokerPath(brokerId));
        return JsonSerDes.fromJson(data.getBytes(), Broker.class);
    }

    public List<LeaderAndReplicas> getPartitionReplicaLeaderInfo(String topicName) {
        String leaderAndReplicasData = zkClient.readData(getReplicaLeaderElectionPath(topicName));
        return JsonSerDes.fromJson(leaderAndReplicasData.getBytes(),
                new TypeReference<List<LeaderAndReplicas>>() {
                });
    }

    public String getReplicaLeaderElectionPath(String topicName) {
        return ReplicaLeaderElectionPath + "/" + topicName;
    }

    public void registerSelf() {
        Broker broker = new Broker(config.getBrokerId(), config.getHostName(), config.getPort());
        registerBroker(broker);
    }

    public List<PartitionReplicas> getPartitionAssignmentsFor(String topicName) {
        String partitionAssignmentsData = zkClient.readData(getTopicPath(topicName));
        return JsonSerDes.deserialize(partitionAssignmentsData.getBytes(),
                new TypeReference<>() {
                });
    }
    public Optional<List<String>> subscribeTopicChangeListener(IZkChildListener listener) {
        List<String> result = zkClient.subscribeChildChanges(BrokerTopicsPath, listener);
        return Optional.ofNullable(result);
    }

    public Optional<List<String>> subscribeBrokerChangeListener(IZkChildListener listener) {
        List<String> result = zkClient.subscribeChildChanges(BrokerIdsPath, listener);
        return Optional.ofNullable(result).map(list -> list);
    }

    public void registerBroker(Broker broker) {
        String brokerData = JsonSerDes.toJson(broker);
        String brokerPath = getBrokerPath(broker.id());
        createEphemeralPath(zkClient, brokerPath, brokerData);
    }

    public List<String> getTopics() {
        return zkClient.getChildren(BrokerTopicsPath);
    }

    private void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);

        }
    }

    private void createPersistentPath(ZkClient client, String path,
                                      String data) {
        try {
            client.createPersistent(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);

        }
    }

    private String getBrokerPath(int id) {
        return BrokerIdsPath + "/" + id;
    }

    private String getTopicPath(String topicName) {
        return BrokerTopicsPath + "/" + topicName;
    }

    private void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (!parentDir.isEmpty()) {
            client.createPersistent(parentDir, true);
        }
    }

    public void setPartitionLeaderForTopic(String topicName,
                                           List<LeaderAndReplicas> leaderAndReplicas) {
        String leaderAndReplicasData = JsonSerDes.toJson(leaderAndReplicas);
        String path = getReplicaLeaderElectionPath(topicName);

        try {
            createPersistentPath(zkClient, path, leaderAndReplicasData);
        } catch (Throwable e) {
            throw new RuntimeException("Exception while writing data to " +
                    "partition leader data", e);
        }
    }



    public void subscribeControllerChangeListener(ZkController controller) {
       zkClient.subscribeDataChanges(ControllerPath,
               new ControllerChangeListener(controller));
    }

    public void tryCreatingControllerPath(int controllerId) throws ControllerExistsException {
        try {
            createEphemeralPath(zkClient, ControllerPath,
                    String.valueOf(controllerId));

        } catch (ZkNodeExistsException e) {
            String existingControllerId = zkClient.readData(ControllerPath);
            throw new ControllerExistsException(Integer.parseInt(existingControllerId));
        }
    }

    public Map<String, List<PartitionReplicas>> getAllTopics() throws Exception {
        List<String> topics = zkClient.getChildren(BrokerTopicsPath); // Assuming zkClient is available
        Map<String, List<PartitionReplicas>> topicPartitionMap = new HashMap<>();
        for (String topicName : topics) {
            String partitionAssignments = zkClient.readData(getTopicPath(topicName));
            List<PartitionReplicas> partitionReplicas = JsonSerDes.deserialize(partitionAssignments.getBytes(), new TypeReference<List<PartitionReplicas>>() {});
            topicPartitionMap.put(topicName, partitionReplicas);
        }
        return topicPartitionMap;
    }

    public void close() {
        zkClient.close();
    }

    class SessionExpireListener implements IZkStateListener {

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
            // do nothing, since zkclient will do reconnect for us.
        }

        @Override
        public void handleNewSession() throws Exception
        {
            logger.error("re-registering broker info in ZK for broker " + config.getBrokerId());
            registerSelf();
            logger.info("done re-registering broker");
            logger.info("Subscribing to " + BrokerTopicsPath + " path to watch for new topics");
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) {
            logger.debug(error.getMessage());
        }
    }

    class ControllerChangeListener implements IZkDataListener {
        private final ZkController controller;

        public ControllerChangeListener(ZkController controller) {
            this.controller = controller;
        }

        @Override
        public void handleDataChange(String dataPath, Object data) {
            String existingControllerId = zkClient.readData(dataPath);
            controller.setCurrent(Integer.parseInt(existingControllerId));
        }

        @Override
        public void handleDataDeleted(String dataPath) {
            controller.elect();
            if (controller.getCurrentLeaderId() == controller.getBrokerId()) {
                controller.electNewLeaderForPartition();
            }
        }
    }
}