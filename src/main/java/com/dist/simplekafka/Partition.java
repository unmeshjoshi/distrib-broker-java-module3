package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.JsonSerDes;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.dist.simplekafka.FetchIsolation.FetchLogEnd;

public class Partition {
    Logger logger = LogManager.getLogger(Partition.class);

    private final Config config;
    private final TopicAndPartition topicAndPartition;
    private final ReentrantLock lock = new ReentrantLock(); //lock for
    // high-watermark updates.
    private final Map<Integer, Long> replicaOffsets = new HashMap<>();
    private long highWatermark = 0L;
    private static final String LogFileSuffix = ".log";
    private final File logFile;
    private final Log log;

    public Partition(Config config, TopicAndPartition topicAndPartition) throws IOException {
        this.config = config;
        this.topicAndPartition = topicAndPartition;
        this.logFile = new File(config.getLogDirs().get(0),
                topicAndPartition.topic() + "-" + topicAndPartition.partition() + LogFileSuffix);
        this.log = new Log(logFile);
    }

    public long append(String key, String message) {
        try {
            return log.append(key.getBytes(), message.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Log.Message> read(long startOffset, int replicaId,
                                  FetchIsolation isolation) {
        try {
            System.out.println("Reading partition " + topicAndPartition + " for fetchIsolation " + isolation);
            long maxOffset;
            if (isolation == FetchIsolation.FetchHighWatermark) {
                maxOffset = highWatermark;
            } else {
                maxOffset = log.lastOffset();
            }

            if (startOffset > maxOffset) {
                return new ArrayList<>();
            }

            List<Log.Message> messages = log.read(startOffset, maxOffset);
            updateLastReadOffsetAndHighWaterMark(replicaId, startOffset - 1);
            return messages;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public File getLogFile() {
        return logFile;
    }

    public void makeFollower(Broker leader) {
        addFetcher(this.topicAndPartition, log.lastOffset(), leader);
    }

    public void makeLeader() {

    }

    public long highWatermark() {
        return highWatermark;
    }

    private final Map<BrokerAndFetcherId, ReplicaFetcherThread> fetcherThreadMap = new HashMap<>();

    public void addFetcher(TopicAndPartition topicAndPartition, long initialOffset, Broker leaderBroker) {
        ReplicaFetcherThread fetcherThread = null;
        BrokerAndFetcherId key = new BrokerAndFetcherId(leaderBroker, getFetcherId(topicAndPartition));

        if (fetcherThreadMap.containsKey(key)) {
            fetcherThread = fetcherThreadMap.get(key);
        } else {
            fetcherThread = createFetcherThread(key.fetcherId, leaderBroker);
            fetcherThreadMap.put(key, fetcherThread);
            fetcherThread.start();
        }

        fetcherThread.addPartition(topicAndPartition, initialOffset);

        logger.info(String.format("Adding fetcher for partition [%s,%d], initOffset %d to broker %d with fetcherId %d",
                topicAndPartition.topic(), topicAndPartition.partition(),
                initialOffset, leaderBroker.id(), key.fetcherId));
    }



    private int getFetcherId(TopicAndPartition topicAndPartition) {
        return (topicAndPartition.topic().hashCode() + 31 * topicAndPartition.partition()); // % numFetchers
    }

    private ReplicaFetcherThread createFetcherThread(int fetcherId, Broker leaderBroker) {
        return new ReplicaFetcherThread(
                String.format("ReplicaFetcherThread-%d-%d", fetcherId,
                        leaderBroker.id()),
                leaderBroker,
                this,
                config
        );
    }

    private Map<Integer, Long> remoteReplicasMap = new HashMap<>();

    public void updateLastReadOffsetAndHighWaterMark(int replicaId, long offset) {
        lock.lock();
        try {
            remoteReplicasMap.put(replicaId, offset);

            List<Long> values = new ArrayList<>(remoteReplicasMap.values());
            Collections.sort(values);

            if (highWatermark < values.get(0)) {
                highWatermark = values.get(0);
                System.out.println("Updated highwatermark to " + highWatermark + " for " + this.topicAndPartition + " on " + config.getBrokerId());
            }

        } finally {
            lock.unlock();
        }
    }

    public static class BrokerAndFetcherId {
        private final Broker broker;
        private final int fetcherId;

        public BrokerAndFetcherId(Broker broker, int fetcherId) {
            this.broker = broker;
            this.fetcherId = fetcherId;
        }
    }


    class ReplicaFetcherThread extends Thread {
        private static final Logger logger =
                LogManager.getLogger(ReplicaFetcherThread.class.getName());

        private final String name;
        private final Broker leaderBroker;
        private final Partition partition;
        private final Config config;

        private List<TopicAndPartition> topicPartitions = new ArrayList<>();

        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicInteger correlationId = new AtomicInteger(0);
        private final SocketClient socketClient = new SocketClient();

        public ReplicaFetcherThread(String name, Broker leaderBroker, Partition partition, Config config) {
            this.name = name;
            this.leaderBroker = leaderBroker;
            this.partition = partition;
            this.config = config;
        }

        public void addPartition(TopicAndPartition topicAndPartition, long initialOffset) {
            topicPartitions.add(topicAndPartition);
        }

        private void doWork() {
            try {
                if (!topicPartitions.isEmpty()) {
                    TopicAndPartition topicPartition = topicPartitions.get(0); // expect only one for now.
                    ConsumeRequest consumeRequest = new ConsumeRequest(topicPartition, FetchLogEnd.toString(),
                            partition.lastOffset() + 1, config.getBrokerId());
                    RequestOrResponse request = new RequestOrResponse(RequestKeys.FetchKey,
                            JsonSerDes.serialize(consumeRequest), correlationId.getAndIncrement());
                    RequestOrResponse response = null;

                    response = socketClient.sendReceiveTcp(request,
                            InetAddressAndPort.create(leaderBroker.host(), leaderBroker.port()));

                    ConsumeResponse consumeResponse = JsonSerDes.deserialize(response.getMessageBodyJson(),
                            ConsumeResponse.class);
                    for (Map.Entry<String, String> m :
                            consumeResponse.getMessages().entrySet()) {
                        logger.info(String.format("Replicating message %s for topic partition %s in broker %d",
                                m, topicPartition, config.getBrokerId()));
                        partition.append(m.getKey(), m.getValue());
                    }
                }
            } catch (IOException e) {
                logger.error(e);
            }
        }

        @Override
        public void run() {
            logger.info("Starting ");
            try {
                while (isRunning.get()) {
                    doWork();
                }
            } catch (Throwable e) {
                if (isRunning.get()) {
                    logger.error("Error due to " + e.getMessage());
                }
            }
            logger.info("Stopped ");
        }
    }

    private long lastOffset() {
        return log.lastOffset();
    }
}
