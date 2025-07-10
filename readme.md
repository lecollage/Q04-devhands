# Q04 // Queues - Devhands.io //

## Test Kafka 
Repository contains source code to run kafka locally and test it via running differen programs written in go 

## How to run Kafka on Ubuntu
1. Install Java
```bash 
apt install openjdk-11-jdk -y
```
2. Download and unpack Kakfa archive from https://kafka.apache.org/downloads
```bash 
curl 'https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz' -o kafka.tgz
tar zxf kafka.tgz --strip 1
```

## Configure and run three node setup with quorum (kraft)
Copy to `kafka/config`, change the paths of the destination
```bash
cp /kafka-test/kafka-raft-properties/kafka-raft-1.properties ./kafka/config/kafka-raft-1.properties
cp /kafka-test/kafka-raft-properties/kafka-raft-2.properties ./kafka/config/kafka-raft-2.properties
cp /kafka-test/kafka-raft-properties/kafka-raft-3.properties ./kafka/config/kafka-raft-3.properties
```

### Generate UUID
```bash
./bin/kafka-storage.sh random-uuid
```

### Initialize data directories (use newly generated uuid)
```bash
./bin/kafka-storage.sh format -t ZfYmDSLVRqaswjRc1tqffg -c config/kafka-raft-1.properties
./bin/kafka-storage.sh format -t ZfYmDSLVRqaswjRc1tqffg -c config/kafka-raft-2.properties
./bin/kafka-storage.sh format -t ZfYmDSLVRqaswjRc1tqffg -c config/kafka-raft-3.properties
```

### Run 3 instances
```bash
./bin/kafka-server-start.sh config/kafka-raft-1.properties
```
```bash
./bin/kafka-server-start.sh config/kafka-raft-2.properties
```
```bash
./bin/kafka-server-start.sh config/kafka-raft-3.properties
```

### Create a check topic
```bash
./bin/kafka-topics.sh --create --topic parted --bootstrap-server localhost:19092,localhost:29092,localhost:39092
./bin/kafka-topics.sh --describe --topic parted --bootstrap-server localhost:19092,localhost:29092,localhost:39092
```

## Run consumers and producers
In different terminal windows run consumers

```bash
go run monitoring_consumer/monitoring_consumer.go
```

```bash
go run go run analytics_consumer/analytics_consumer.go
```

In different terminal windows run producers
```bash
go run purchase_producer/purchase_producer.go
```

```bash
go run page_view_producer/page_view_producer.go
```

```bash
go run click_producer/click_producer.go
```
