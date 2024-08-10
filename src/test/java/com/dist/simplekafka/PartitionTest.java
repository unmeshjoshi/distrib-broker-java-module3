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

        int offset = p.append("k1", "m1");
        assertEquals(1, offset);

        List<Partition.Row> messages = p.read(offset, -1, FetchIsolation.FetchLogEnd);
        assertEquals(1, messages.size());
        assertEquals("m1", messages.get(0).getValue());
    }

    @Test
    public void shouldAppendToTheEndOfTheFileEvenIfReadBeforeWrite() throws Exception {
        Partition p = new Partition(testConfig(),
                new TopicAndPartition("topic1", 0));
        assertTrue(p.getLogFile().exists());

        int offset1 = p.append("k1", "m1");
        int offset2 = p.append("k2", "m2");
        int offset3 = p.append("k3", "m3");
        assertEquals(3, offset3);

        List<Partition.Row> messages = p.read(offset2, -1,
                FetchIsolation.FetchLogEnd);
        assertEquals(2, messages.size());
        assertEquals("m2", messages.get(0).getValue());
        assertEquals("m3", messages.get(1).getValue());

        int offset4 = p.append("k4", "m4");
        assertEquals(4, offset4);
    }

    @Test
    public void testShouldWriteAndReadMessagesInPartition() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        long offset1 = partition.append("key1", "message1");
        long offset2 = partition.append("key2", "message2");

        List<Partition.Row> messages = partition.read(offset1,
                -1, FetchIsolation.FetchLogEnd);

        assertEquals(2, messages.size());
        assertEquals("key1", messages.get(0).getKey());
        assertEquals("key2", messages.get(1).getKey());
        assertEquals("message1", messages.get(0).getValue());
        assertEquals("message2", messages.get(1).getValue());
    }

    @Test
    public void testShouldReadMessagesFromSpecificOffset() throws IOException {
        Config config1 = testConfig();
        Partition partition = new Partition(config1, new TopicAndPartition("topic1", 0));
        long offset1 = partition.append("key1", "message1");
        long offset2 = partition.append("key2", "message2");
        long offset3 = partition.append("key3", "message3");
        List<Partition.Row> messages = partition.read(offset2,
                -1, FetchIsolation.FetchLogEnd);
        assertEquals(2, messages.size());
        assertEquals("key2", messages.get(0).getKey());
        assertEquals("message2", messages.get(0).getValue());
        assertEquals("key3", messages.get(1).getKey());
        assertEquals("message3", messages.get(1).getValue());
    }


    protected Config testConfig() {
        return new Config(1, new Networks().hostname(),
                TestUtils.choosePort(), ZookeeperTestHarness.zkConnectAddress,
                Collections.singletonList(TestUtils.tempDir().getAbsolutePath()));
    }

}