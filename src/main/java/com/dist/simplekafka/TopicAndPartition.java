package com.dist.simplekafka;

import java.util.Objects;

public final class TopicAndPartition {
    private final String topic;
    private final int partition;

    public TopicAndPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    private TopicAndPartition() {
        this("", -1);//for jackson
    }

    @Override
    public String toString() {
        return String.format("[%s,%d]", topic, partition);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (TopicAndPartition) obj;
        return Objects.equals(this.topic, that.topic) &&
                this.partition == that.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

}
