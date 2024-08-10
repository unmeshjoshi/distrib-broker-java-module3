package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.Networks;
import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.*;

public class ServerTest extends ZookeeperTestHarness {
    @Test
    public void shouldRegisterItselfToZookeeperOnStartup() {
        Server server = newBroker(1);
        server.startup();

        Set<Integer> allBrokerIds = zookeeperClient.getAllBrokerIds();
        assertEquals(Set.of(1), allBrokerIds);
    }

    @Test
    public void shouldRegisterControllerWithZookeeperAtStartup() {
        Server server1 = newBroker(1);
        server1.startup();

        Server server2 = newBroker(2);
        server2.startup();

        assertTrue(server1.isController());
        assertFalse(server2.isController());
    }



}