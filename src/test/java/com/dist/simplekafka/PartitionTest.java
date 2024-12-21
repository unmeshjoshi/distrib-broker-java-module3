package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.Networks;
import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class PartitionTest  {

    @Test
    public void shouldAppendMessagesToFileAndReturnOffset() throws Exception {
        Partition p = new Partition(testConfig(),
                new TopicAndPartition("topic1", 0));
        assertTrue(p.getLogFile().exists());

        long offset = p.append("k1", "m1");
        assertEquals(1, offset);

        List<Log.Message> messages = p.read(offset, -1, FetchIsolation.FetchLogEnd);
        assertEquals(1, messages.size());
        assertEquals("m1", new String(messages.get(0).value));
    }

    @Test
    public void shouldAppendToTheEndOfTheFileEvenIfReadBeforeWrite() throws Exception {
        Partition p = new Partition(testConfig(),
                new TopicAndPartition("topic1", 0));
        assertTrue(p.getLogFile().exists());

        long offset1 = p.append("k1", "m1");
        long offset2 = p.append("k2", "m2");
        long offset3 = p.append("k3", "m3");
        assertEquals(3, offset3);

        List<Log.Message> messages = p.read(offset2, -1,
                FetchIsolation.FetchLogEnd);
        assertEquals(2, messages.size());
        assertEquals("m2", new String(messages.get(0).value));
        assertEquals("m3", new String(messages.get(1).value));

        long offset4 = p.append("k4", "m4");
        assertEquals(4, offset4);
    }

    @Test
    public void testShouldWriteAndReadMessagesInPartition() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        long offset1 = partition.append("key1", "message1");
        long offset2 = partition.append("key2", "message2");

        List<Log.Message> messages = partition.read(offset1,
                -1, FetchIsolation.FetchLogEnd);

        assertEquals(2, messages.size());
        assertEquals("key1", new String(messages.get(0).key));
        assertEquals("key2", new String(messages.get(1).key));
        assertEquals("message1", new String(messages.get(0).value));
        assertEquals("message2", new String(messages.get(1).value));
    }

    @Test
    public void testShouldReadMessagesFromSpecificOffset() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        long offset1 = partition.append("key1", "message1");
        long offset2 = partition.append("key2", "message2");
        long offset3 = partition.append("key3", "message3");
        List<Log.Message> messages = partition.read(offset2,
                -1, FetchIsolation.FetchLogEnd);
        assertEquals(2, messages.size());
        assertEquals("key2", new String(messages.get(0).key));
        assertEquals("message2", new String(messages.get(0).value));
        assertEquals("key3", new String(messages.get(1).key));
        assertEquals("message3", new String(messages.get(1).value));
    }

    /**
     * Test scenario:
     * 1. Append messages M0-M5 to the log
     * 2. Update replica offsets to simulate replication:
     *    - Replica 1 (Leader): Offset 5
     *    - Replica 2: Offset 3
     *    - Replica 3: Offset 2
     * 3. High watermark should be 2 (minimum of all replicas)
     * 4. Verify reads respect high watermark isolation
     */
    @Test
    public void testReadRespectsHighWatermark() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        // Append 6 messages
        for (int i = 0; i < 6; i++) {
            partition.append("key" + i, "message" + i);
        }

        // Simulate replica progress
        partition.updateLastReadOffsetAndHighWaterMark(2, 3); // Follower 1
        partition.updateLastReadOffsetAndHighWaterMark(3, 2); // Follower 2

        // Read with high watermark isolation
        List<Log.Message> messages = partition.read(1, -1, FetchIsolation.FetchHighWatermark);

        // Should only get messages up to high watermark (offset 2)
        assertEquals("Should only read messages up to high watermark",3, messages.size());

        // Verify message contents
        for (int i = 0; i < messages.size(); i++) {
            Log.Message message = messages.get(i);
            assertEquals("Key should match", "key" + i, new String(message.key));
            assertEquals("Message should match", "message" + i, new String(message.value));
        }
    }

    /**
     * Test scenario:
     * Same setup as above, but read with FetchLogEnd isolation
     * Should return all messages regardless of high watermark
     */
    @Test
    public void testReadWithLogEndIsolation() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        // Append 6 messages
        for (int i = 0; i < 6; i++) {
            partition.append("key" + i, "message" + i);
        }

        // Simulate replica progress
        partition.updateLastReadOffsetAndHighWaterMark(1, 5);
        partition.updateLastReadOffsetAndHighWaterMark(2, 3);
        partition.updateLastReadOffsetAndHighWaterMark(3, 2);

        // Read with log end isolation
        List<Log.Message> messages = partition.read(1, -1, FetchIsolation.FetchLogEnd);

        // Should get all messages
        assertEquals("Should read all messages with FetchLogEnd", 6, messages.size());
    }

    /**
     * Test scenario:
     * Verify that reads starting from an offset beyond high watermark
     * return empty list
     */
    @Test
    public void testReadBeyondHighWatermark() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        // Append 6 messages
        for (int i = 0; i < 6; i++) {
            partition.append("key" + i, "message" + i);
        }

        // Set high watermark to 2
        partition.updateLastReadOffsetAndHighWaterMark(1, 5);
        partition.updateLastReadOffsetAndHighWaterMark(2, 3);
        partition.updateLastReadOffsetAndHighWaterMark(3, 2);

        // Try to read from offset 3 (beyond high watermark)
        List<Log.Message> messages = partition.read(3, -1, FetchIsolation.FetchHighWatermark);

        // Should get empty list
        assertTrue("Reading beyond high watermark should return empty list", messages.isEmpty());
    }

    /**
     * Test scenario:
     * Verify that replica reads update the replica's offset correctly
     */
    @Test
    public void testReplicaReadUpdatesOffset() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        // Append 3 messages
        for (int i = 0; i < 3; i++) {
            partition.append("key" + i, "message" + i);
        }

        // Simulate replica read
        int replicaId = 2;
        partition.read(1, replicaId, FetchIsolation.FetchLogEnd);

        // High watermark should be updated for this replica
        assertEquals( "High watermark should be updated after replica read", 2, partition.highWatermark());
    }


    protected Config testConfig() {
        return new Config(1, new Networks().hostname(),
                TestUtils.choosePort(), ZookeeperTestHarness.zkConnectAddress,
                Collections.singletonList(TestUtils.tempDir().getAbsolutePath()));
    }

}