# Guide to use Golang connect to Kafka
1. Download and install Kafka in local machine
Refer to link: https://www.sohamkamani.com/blog/2017/11/22/how-to-install-and-run-kafka/

Kafka System Architecture
There are a bunch of processes that we need to start to run our cluster :

Zookeeper : Which is used by Kafka to maintain state between the nodes of the cluster.
Kafka brokers : The “pipes” in our pipeline, which store and emit data.
Producers : That insert data into the cluster.
Consumers : That read data from the cluster.

What order should we start each component:
Zookeeper (1) -> Kafka brokers (multiple) -> Producers (multiple) -> Consumers (multiple)

2. Guide to start Kafka environment
Step 1: Open cmd
Step 2: cd ./kafka_2.11-1.0.0
Step 3: bin/zookeeper-server-start.sh config/zookeeper.properties
Step 4: mkdir /tmp/kafka-logs1
		mkdir /tmp/kafka-logs2
		mkdir /tmp/kafka-logs3
Step 5: bin/kafka-server-start.sh config/server.1.properties
		bin/kafka-server-start.sh config/server.2.properties
		bin/kafka-server-start.sh config/server.3.properties
Step 6: bin/kafka-topics.sh --create --topic my-kafka-topic --zookeeper localhost:2181 --partitions 3 --replication-factor 2
Step 7: bin/kafka-console-producer.sh --broker-list localhost:9093,localhost:9094,localhost:9095 --topic my-kafka-topic
Step 8: bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic my-kafka-topic --from-beginning

3. Guide to use Golang connect to Kafka producer and consumer
Step 1: Opend cmd
Step 2: Type go run main.go
![pic2](pic/pic2.png)
Step 3: Input data to producer (includes key ID and value) then consumer display what was input
![pic3](pic/pic3.png)