package com.dist.simplekafka;

import com.dist.common.Config;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class Partition {
    private final Config config;
    private final TopicAndPartition topicAndPartition;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Long> replicaOffsets = new HashMap<>();
    private long highWatermark = 0L;
    private static final String LogFileSuffix = ".log";
    private final File logFile;
    private final SequenceFile sequenceFile;
    private final SequenceFile.Reader reader;
    private final SequenceFile.Writer writer;

    public Partition(Config config, TopicAndPartition topicAndPartition) throws IOException {
        this.config = config;
        this.topicAndPartition = topicAndPartition;
        this.logFile = new File(config.getLogDirs().get(0),
                topicAndPartition.topic() + "-" + topicAndPartition.partition() + LogFileSuffix);
        this.sequenceFile = new SequenceFile(config);
        this.reader = sequenceFile.new Reader(logFile.getAbsolutePath());
        this.writer = sequenceFile.new Writer(logFile.getAbsolutePath());
    }

    public int append(String key, String message)  {
        return 0;
        //Assignment: Implement append;
//        long currentPos = writer.getCurrentPosition();
//        try {
//            return writer.append(key, message);
//        } catch (IOException e) {
//            try {
//                writer.seek(currentPos);
//            } catch (IOException seekException) {
//                // Handle seek exception
//            }
//            throw new RuntimeException(e);
//        }
    }

    public List<Row> read(long offset, int replicaId, FetchIsolation isolation)  {
        return Collections.emptyList();
        //Assignment: Implement read.

//        lock.lock();
//        try {
//            System.out.println("Reading partition " + topicAndPartition + " for fetchIsolation " + isolation);
//            long maxOffset;
//            if (isolation == FetchIsolation.FetchHighWatermark) {
//                maxOffset = highWatermark;
//            } else {
//                maxOffset = sequenceFile.lastOffset();
//            }
//
//            if (offset > maxOffset) {
//                return new ArrayList<>();
//            }
//
//            List<Row> result = new ArrayList<>();
//            Set<Long> offsets = sequenceFile.getAllOffSetsFrom(offset, maxOffset);
//            for (Long currentOffset : offsets) {
//                long filePosition = sequenceFile.getFilePosition(currentOffset);
//                ByteArrayOutputStream ba = new ByteArrayOutputStream();
//                DataOutputStream baos = new DataOutputStream(ba);
//                reader.seekToOffset(filePosition);
//                reader.next(baos);
//                DataInputStream bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray()));
//                try {
//                    result.add(Row.deserialize(bais));
//                } catch (Exception e) {
//                    // Handle deserialization exception
//                }
//            }
//            return result;
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            lock.unlock();
//        }
    }

    public File getLogFile() {
        return logFile;
    }

    public void makeFollower(Broker leader) {

    }

    public void makeLeader() {

    }

    public long highWatermark() {
        return highWatermark;
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


    public static class Row {
        private final String key;
        private final String value;

        public Row(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public static void serialize(Row row, DataOutputStream dos) throws IOException {
            dos.writeUTF(row.key);
            dos.writeInt(row.value.getBytes().length);
            dos.write(row.value.getBytes()); //TODO: as of now only supporting string writes.
        }

        public static Row deserialize(DataInputStream dis) throws IOException {
            String key = dis.readUTF();
            int dataSize = dis.readInt();
            byte[] bytes = new byte[dataSize];
            dis.readFully(bytes);
            String value = new String(bytes); //TODO:As of now supporting only string values
            return new Row(key, value);
        }

        public String getValue() {
            return value;
        }

        public String getKey() {
            return key;
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
}
