package com.dist.simplekafka;

import com.dist.common.Config;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public class Server {
    private static final Logger logger = LogManager.getLogger(Server.class);
    private static SimpleKafkaApi kafkaApis;

    private final Config config;
    private final ZookeeperClient zookeeperClient;
    final ZkController controller;
    private final BrokerNetworkHandler socketServer;

    public Server(Config config, ZookeeperClient zookeeperClient, ZkController controller, BrokerNetworkHandler brokerNetworkHandler) {
        this.config = config;
        this.zookeeperClient = zookeeperClient;
        this.controller = controller;
        this.socketServer = brokerNetworkHandler;
    }

    public static Server create(Config config) {
        kafkaApis = new SimpleKafkaApi(config, new ReplicaManager(config));
        var brokerNetworkHandler =
                new BrokerNetworkHandler(config.getBrokerId(),
                        config.getHostName(), config.getPort(),
                        kafkaApis);

        ZookeeperClient zookeeperClient = new ZookeeperClient(config);

        return new Server(config, zookeeperClient,
                new ZkController(zookeeperClient, config.getBrokerId(), brokerNetworkHandler),
                brokerNetworkHandler);
    }

    public void startup() {
        zookeeperClient.registerSelf();
        controller.startup();
        socketServer.startup();
    }

    public void shutdown() {
        zookeeperClient.close();
        socketServer.shutdown();
    }

    public boolean isController() {
        return controller.getCurrentLeaderId() == config.getBrokerId();
    }

    public Map leaderCache() {
        return kafkaApis.getLeaderCache();
    }

    public List<Broker> aliveBrokers() {
        return kafkaApis.getAliveBrokers();
    }

    public Config getConfig() {
        return config;
    }
}
