package com.dist.simplekafka;

import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import com.dist.net.InetAddressAndPort;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class ProducerConsumerTest extends ZookeeperTestHarness {

    @Test
    public void shouldProduceAndConsumeMessages() throws IOException {
        Server broker1 = newBroker(1);
        Server broker2 = newBroker(2);
        Server broker3 = newBroker(3);

        broker1.startup();
        broker2.startup();
        broker3.startup();

        TestUtils.waitUntilTrue(()-> {
            return broker1.controller.getCurrentLiveBrokerIds().size() == 3;
        }, "Waiting for all brokers to be discovered by the controller");


        CreateTopicCommand createTopicCommand = new CreateTopicCommand(zookeeperClient,
                new ReplicaAssigner(new Random()));

        createTopicCommand.createTopic("topic1", 2, 3);

        TestUtils.waitUntilTrue(() -> {
            return liveBrokersIn(broker1) == 3 && liveBrokersIn(broker2) == 3 && liveBrokersIn(broker3) == 3;
        }, "waiting till topic metadata is propogated to all the servers");

        InetAddressAndPort bootstrapBroker = InetAddressAndPort.create(broker2.getConfig().getHostName(), broker2.getConfig().getPort());
        SimpleProducer simpleProducer = new SimpleProducer(bootstrapBroker);


        long offset1 = simpleProducer.produce("topic1", "key1", "message1");
        assertEquals("First offset should be 1", 1, offset1 );

        long offset2 = simpleProducer.produce("topic1", "key2", "message2");
        assertEquals( "First offset on different partition should be 1", 1,
                offset2);

        long offset3 = simpleProducer.produce("topic1", "key3", "message3");
        assertEquals("Offset on first partition should be 2", 2, offset3);

        SimpleConsumer consumer = new SimpleConsumer(bootstrapBroker);
        Map<String, String> messages = consumer.consume("topic1",
                FetchIsolation.FetchLogEnd);

        assertEquals(3, messages.size());
        assertEquals("message1", messages.get("key1"));
        assertEquals("message2", messages.get("key2"));
        assertEquals("message3", messages.get("key3"));


    }

    private int liveBrokersIn(Server broker1) {
        int size = broker1.aliveBrokers().size();
        System.out.println("size = " + size + " in " + broker1.getConfig().getBrokerId());
        return size;
    }

}