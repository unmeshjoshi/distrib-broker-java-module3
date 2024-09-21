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


    protected Config testConfig() {
        return new Config(1, new Networks().hostname(),
                TestUtils.choosePort(), ZookeeperTestHarness.zkConnectAddress,
                Collections.singletonList(TestUtils.tempDir().getAbsolutePath()));
    }

}