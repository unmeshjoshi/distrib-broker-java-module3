package com.dist.simplekafka;

import com.dist.common.JsonSerDes;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ZkController {
    private final ZookeeperClient zookeeperClient;
    private final int brokerId;
    private final BrokerNetworkHandler brokerClient;
    private final AtomicInteger correlationId = new AtomicInteger(0);
    private Set<Broker> liveBrokers = new HashSet<>();
    private int currentLeader = -1;

    public ZkController(ZookeeperClient zookeeperClient, int brokerId, BrokerNetworkHandler socketServer) {
        this.zookeeperClient = zookeeperClient;
        this.brokerId = brokerId;
        this.brokerClient = socketServer;
    }

    public void startup() {
        zookeeperClient.subscribeControllerChangeListener(this);
        elect();
    }

    public void electNewLeaderForPartition() {
        List<String> topics = zookeeperClient.getTopics();
        List<LeaderAndReplicas> newLeaderAndReplicaList = new ArrayList<>();
        List<PartitionReplicas> partitionReplicas = new ArrayList<>();

        for (String topicName : topics) {
            List<LeaderAndReplicas> leaderAndReplicaList =
                    zookeeperClient.getPartitionReplicaLeaderInfo(topicName);
            boolean leaderChanged = false;

            for (int i = 0; i < leaderAndReplicaList.size(); i++) {
                LeaderAndReplicas leaderAndReplica = leaderAndReplicaList.get(i);
                PartitionInfo partitionInfo = leaderAndReplica.partitionStateInfo();
                Broker partitionCurrentLeader = partitionInfo.getLeaderBroker();

                partitionReplicas.addAll(zookeeperClient.getPartitionAssignmentsFor(topicName));

                if (!liveBrokers.contains(partitionCurrentLeader)) {
                    for (Broker replica : partitionInfo.allBrokers()) {
                        if (!partitionCurrentLeader.equals(replica)) {
                            partitionCurrentLeader = replica;
                            PartitionInfo newPartitionInfo =
                                    new PartitionInfo(partitionCurrentLeader.id(),
                                            partitionInfo.allBrokers());
                            LeaderAndReplicas leaderReplica = new LeaderAndReplicas(leaderAndReplica.topicPartition(), newPartitionInfo);
                            leaderAndReplicaList.set(i, leaderReplica);
                            leaderChanged = true;
                            break;
                        }
                    }
                }
            }

            if (leaderChanged) {
                zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicaList);
                newLeaderAndReplicaList.addAll(leaderAndReplicaList);
            }
        }

        if (!newLeaderAndReplicaList.isEmpty()) {
            sendUpdateMetadataRequestToAllLiveBrokers(newLeaderAndReplicaList);
            sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(newLeaderAndReplicaList, partitionReplicas);
        }
    }

    public void shutdown() {
        // Implementation not provided in the original code
    }

    public void elect() {
        try {
            zookeeperClient.tryCreatingControllerPath(brokerId);
            this.currentLeader = brokerId;
            onBecomingLeader();
        } catch (ControllerExistsException e) {
            this.currentLeader = e.getControllerId();
        }
    }

    private void onBecomingLeader() {
        liveBrokers.addAll(zookeeperClient.getAllBrokers());
        zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, this));
        zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListener(this));
    }

    /**
     * Handles the creation of a new topic by:
     * 1. Selecting leaders and followers for each partition
     * 2. Persisting the leader/follower information to ZooKeeper
     * 3. Notifying relevant brokers about their leader/follower roles
     * 4. Updating all brokers with the new metadata
     *
     * @param topicName         Name of the new topic being created
     * @param partitionReplicas List of partition assignments with their replica brokers
     */

    public void handleNewTopic(String topicName, List<PartitionReplicas> partitionReplicas) {
        List<LeaderAndReplicas> leaderAndReplicas = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas);
        zookeeperClient.setPartitionLeaderForTopic(topicName,
                leaderAndReplicas);
        //Assignment send leader follower information to individual brokers.

//        sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas);

//        Assignment send all the metadata information to individual brokers.
//        sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas);
    }

    /**
     * Assigns leader and follower brokers for each partition of a topic.
     * <p>
     * The partitionReplicas input contains broker assignments that were already
     * carefully determined during partition assignment, considering:
     * - Even distribution of partitions across brokers
     * - Rack awareness (Not implemented)
     * - Broker load balancing etc.. (Not implemented)
     * <p>
     * Therefore, selecting the first broker from each partition's replica list as leader is valid because:
     * 1. The replica list order was already optimized during partition assignment
     * 2. This ensures deterministic leader assignment (same input always yields same leader)
     * 3. Leaders will be naturally distributed across brokers due to the balanced partition assignment
     * <p>
     * Example:
     * Input partitionReplicas: [
     * Partition 0 => brokers [1,2,3],
     * Partition 1 => brokers [2,3,1],
     * Partition 2 => brokers [3,1,2]
     * ]
     * <p>
     * Results in leaders:
     * - Partition 0 => Leader: 1, Followers: 2,3
     * - Partition 1 => Leader: 2, Followers: 3,1
     * - Partition 2 => Leader: 3, Followers: 1,2
     *
     * @param topicName         Name of the topic
     * @param partitionReplicas List of partition assignments with their replica brokers
     * @return List of leader and replica assignments for each partition
     */
    private List<LeaderAndReplicas> selectLeaderAndFollowerBrokersForPartitions(String topicName, List<PartitionReplicas> partitionReplicas) {
        //Assignment assign leader and follower to partitions.
        return partitionReplicas.stream().map(p -> {
            int leaderBrokerId = p.brokerIds().get(0); //mark first one as
            // broker.
            List<Broker> replicaBrokers = p.brokerIds().stream().map(this::getBroker).collect(Collectors.toList());
            return new LeaderAndReplicas(new TopicAndPartition(topicName,
                    p.getPartitionId()), new PartitionInfo(leaderBrokerId,
                    replicaBrokers));
        }).collect(Collectors.toList());
    }

    private Broker getBroker(int brokerId) {
        return liveBrokers.stream().filter(b -> b.id() == brokerId).findFirst().orElseThrow();
    }


    private void sendUpdateMetadataRequestToAllLiveBrokers(List<LeaderAndReplicas> leaderAndReplicas) {
        for (Broker broker : liveBrokers) {
            UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest(new ArrayList<>(liveBrokers), leaderAndReplicas);
            RequestOrResponse request = new RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet());
            brokerClient.sendReceiveTcp(request, InetAddressAndPort.create(broker.host(), broker.port()));
        }
    }

    /**
     * Sends LeaderAndIsr requests to all involved brokers.
     * This is one of two critical messages (along with UpdateMetadata) sent during partition assignment.
     * 
     * Message Types in Partition Assignment:
     * 1. LeaderAndIsr: (This method)
     *    - Tells each broker its specific role (leader/follower) for partitions
     *    - Contains ISR (In-Sync Replicas) list
     *    - Triggers broker to start appropriate replication processes
     * 
     * 2. UpdateMetadata: (Sent separately)
     *    - Sent to ALL brokers
     *    - Contains complete cluster metadata
     *    - Helps brokers route client requests correctly
     * 
     * Example Scenario:
     * Topic "users" with 2 partitions, replication factor 3
     * 
     * LeaderAndIsr Messages:
     * ```
     * → To Broker 1:
     *   LeaderAndIsr Request {
     *     - Partition 0: {role: Leader, leader: 1, isr: [1,2,3]}
     *     - Partition 1: {role: Follower, leader: 2, isr: [2,3,1]}
     *   }
     * 
     * → To Broker 2:
     *   LeaderAndIsr Request {
     *     - Partition 0: {role: Follower, leader: 1, isr: [1,2,3]}
     *     - Partition 1: {role: Leader, leader: 2, isr: [2,3,1]}
     *   }
     * 
     * → To Broker 3:
     *   LeaderAndIsr Request {
     *     - Partition 0: {role: Follower, leader: 1, isr: [1,2,3]}
     *     - Partition 1: {role: Follower, leader: 2, isr: [2,3,1]}
     *   }
     * ```
     *
     * Broker Actions on Receiving LeaderAndIsr:
     * - Leader: Starts accepting writes, manages ISR
     * - Follower: Starts fetching from leader
     * 
     * @param leaderAndReplicas List of leader and replica assignments for each partition
     * @param partitionReplicas List of partition assignments with their replica brokers
     */
    public void sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(
            List<LeaderAndReplicas> leaderAndReplicas, 
            List<PartitionReplicas> partitionReplicas) {
        
        // Group assignments by broker to send one LeaderAndIsr request per broker
        Map<Broker, List<LeaderAndReplicas>> brokerToLeaderIsrRequest = new HashMap<>();

        // For each partition's leader/replica assignment
        for (LeaderAndReplicas lr : leaderAndReplicas) {
            // Get all brokers involved (both leader and followers)
            for (Broker broker : lr.partitionStateInfo().allBrokers()) {
                // Add this partition assignment to broker's list
                brokerToLeaderIsrRequest.computeIfAbsent(broker, k -> new ArrayList<>()).add(lr);
            }
        }

        // Send LeaderAndIsr request to each broker
        for (Map.Entry<Broker, List<LeaderAndReplicas>> entry : brokerToLeaderIsrRequest.entrySet()) {
            Broker broker = entry.getKey();
            List<LeaderAndReplicas> leaderAndReplicasList = entry.getValue();
            
            // Create LeaderAndIsr request with all partition assignments for this broker
            LeaderAndReplicaRequest leaderAndReplicaRequest = new LeaderAndReplicaRequest(leaderAndReplicasList);
            
            // Create network request with LeaderAndIsr key
            RequestOrResponse request = new RequestOrResponse(
                RequestKeys.LeaderAndIsrKey,  // Identifies this as LeaderAndIsr request
                JsonSerDes.serialize(leaderAndReplicaRequest), 
                correlationId.getAndIncrement());
            
            // Send LeaderAndIsr request to broker
            brokerClient.sendReceiveTcp(request, 
                InetAddressAndPort.create(broker.host(), broker.port()));
        }
    }

    /**
     * Message Structure Examples:
     * 
     * 1. LeaderAndIsr Request:
     * {
     *   requestKey: RequestKeys.LeaderAndIsrKey,
     *   partitions: [
     *     {
     *       topic: "users",
     *       partition: 0,
     *       leader: 1,
     *       isr: [1,2,3],
     *       replicas: [1,2,3]
     *     },
     *     {
     *       topic: "users",
     *       partition: 1,
     *       leader: 2,
     *       isr: [2,3,1],
     *       replicas: [2,3,1]
     *     }
     *   ]
     * }
     * 
     * 2. UpdateMetadata Request: (sent separately)
     * {
     *   requestKey: RequestKeys.UpdateMetadataKey,
     *   brokers: [
     *     {id: 1, host: "host1", port: 9092},
     *     {id: 2, host: "host2", port: 9092},
     *     {id: 3, host: "host3", port: 9092}
     *   ],
     *   partitionStates: [
     *     // Same as LeaderAndIsr partitions
     *   ]
     * }
     */

    public void addBroker(Broker broker) {
        liveBrokers.add(broker);
    }

    public void onBrokerStartup(List<Integer> brokerIds) {
        // We do not do anything, assuming all topics are created after all the brokers are up and running
    }

    public void setCurrent(int existingControllerId) {
        this.currentLeader = existingControllerId;
    }

    public int getBrokerId() {
        return this.brokerId;
    }

    public Set<Broker> getLiveBrokers() {
        return liveBrokers;
    }

    public int getCurrentLeaderId() {
        return currentLeader;
    }

    void processRemovedBrokers(Set<Integer> removedBrokerIds) {
        Iterator<Broker> iterator = liveBrokers.iterator();
        while (iterator.hasNext()) {
            Broker broker = iterator.next();
            if (removedBrokerIds.contains(broker.id())) {
                iterator.remove();
                System.out.println("Removing broker = " + broker);

                onBrokerRemoved(broker);  // Additional action when a broker is removed
            }
        }
    }


    private void onBrokerRemoved(Broker broker) {
        // Perform any additional actions needed when a broker is removed
        // For example: logging, notifying other components, cleanup, etc.
    }

    void processNewlyAddedBrokers(Set<Integer> newBrokerIds) {
        for (int newBrokerId : newBrokerIds) {
            Broker broker = zookeeperClient.getBrokerInfo(newBrokerId);
            System.out.println("Adding broker = " + broker);
            addBroker(broker);
        }
    }

    Set<Integer> getCurrentLiveBrokerIds() {
        return getLiveBrokers().stream().map(Broker::id).collect(Collectors.toSet());
    }

}
