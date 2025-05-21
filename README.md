# Distributed Broker Java Module 3

This repository contains a simplified implementation of a Kafka-like distributed messaging system. The project includes several assignments to help you understand and implement key components of the system.

## Assignments

1. **Connect Controller Handling of New Topic**
   - File: `src/main/java/com/dist/simplekafka/TopicChangeHandler.java`
   - Task: Connect the controller to handle the creation of new topics.

2. **Assign Leader and Follower to Partitions**
   - File: `src/main/java/com/dist/simplekafka/ZkController.java`
   - Task: Implement the logic to assign leaders and followers to partitions.

3. **Send Leader Follower Information to Individual Brokers**
   - File: `src/main/java/com/dist/simplekafka/ZkController.java`
   - Task: Implement the mechanism to send leader and follower information to individual brokers.

4. **Complete Leader/Follower Initialization Logic**
   - File: `src/main/java/com/dist/simplekafka/SimpleKafkaApi.java`
   - Task: Complete the initialization logic for leaders and followers.

5. **Implement Producer**
   - File: `src/main/java/com/dist/simplekafka/SimpleProducer.java`
   - Task: Implement the producer functionality to send messages to the broker.

6. **Implement Consumer**
   - File: `src/main/java/com/dist/simplekafka/SimpleConsumer.java`
   - Task: Implement the consumer functionality to retrieve messages from the broker.

## Getting Started

To get started with the assignments, clone this repository and import it into your favorite Java IDE. Each assignment is marked with a comment in the respective file.

## Running Tests

The repository includes tests that can help you verify your implementations. You can run the tests using Gradle:

```bash
./gradlew test
