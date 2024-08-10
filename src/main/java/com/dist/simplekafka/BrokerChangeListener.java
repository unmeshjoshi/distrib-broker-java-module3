package com.dist.simplekafka;
import java.util.*;
import java.util.stream.Collectors;


import org.I0Itec.zkclient.IZkChildListener;
import org.apache.log4j.Logger;

public class BrokerChangeListener implements IZkChildListener {

    static final Logger logger =
            Logger.getLogger(BrokerChangeListener.class);

    public BrokerChangeListener(ZkController controller) {
        this.controller = controller;
    }
    private final ZkController controller;

    @Override
    public void handleChildChange(String parentPath, List<String> liveBrokerIds)  {
        logger.info("Broker change listener fired for path "  + parentPath +
                " with children " + String.join(",", liveBrokerIds));
        synchronizeBrokerListWithController(
                liveBrokerIds.stream().map((i -> Integer.parseInt(i))).collect(Collectors.toSet()));
    }

    void synchronizeBrokerListWithController(Set<Integer> newLiveBrokerIds) {
        try {
            Set<Integer> existingLiveBrokerIds = controller.getCurrentLiveBrokerIds();
            Set<Integer> addedBrokerIds = findNewlyAddedBrokerIds(existingLiveBrokerIds, newLiveBrokerIds);
            Set<Integer> removedBrokerIds = findRemovedBrokerIds(existingLiveBrokerIds, newLiveBrokerIds);
            controller.processNewlyAddedBrokers(addedBrokerIds);
            controller.processRemovedBrokers(removedBrokerIds);

        } catch (Throwable e) {
            logger.error("Error while synchronizing broker list", e);
        }
    }

    private Set<Integer> findNewlyAddedBrokerIds(Set<Integer> currentBrokerIds, Set<Integer> liveBrokerIds) {
        Set<Integer> newBrokerIds = new HashSet<>(liveBrokerIds);
        newBrokerIds.removeAll(currentBrokerIds);
        return newBrokerIds;
    }

    private Set<Integer> findRemovedBrokerIds(Set<Integer> currentBrokerIds, Set<Integer> liveBrokerIds) {
        Set<Integer> removedBrokerIds = new HashSet<>(currentBrokerIds);
        removedBrokerIds.removeAll(liveBrokerIds);
        return removedBrokerIds;
    }

}
