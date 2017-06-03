# Kafka Notes
Apache Kafka is a publish/subscribe messaging system.
Often described as a "distributed commit log" or "distributing streaming platform" designed to provide a durable record of all transactions so that they can be replayed to consistently build the state of the system.
Data within Kafka is stored durably, in order, and can be read deterministically.


A single unit of data within Kafka is called a message, similar to a row or record.
A message is simply an array of bytes, so the data contained within it does not have a specific format.
The key of a message are used when messages are to be written to partitions in a more controlled manner.


Kafka process messages in batches. A batch is just a collection of messages, all of which are being produced to the same topic and partition. The larger the batches, the more messages that can be handled per unit of time, but the longer is takes an individual message to propagate.


Many Kafka developers favor the use of Apache Avro, which is a serialization framework originally developed for Hadoop.
Avro provides a compact serialization format, schemas that are separate from the message payloads and that do not require generated code when they change, as well as strong data typing and schema evolution, with both backwards and forwards compatibility.

A consistent data format is important in Kafka, as it allows writing and reading messages to be decoupled.

## Topics and Partitions
Messages in Kafka are categorized into topics.
The closest analogy for a topic is a database table, or a folder in a filesystem.
Topics are additionally broken down into a number of partitions.
A partition is a single log. Messages are written to it in an append-only fashion, and are read in order from beginning to end.
There is no guarantee of time-ordering of messages accross the entire topic, just within a single partition.
Partitions are also the way that Kafka provides redundancy and scalability.
Each partition can be hosted on a different server, which means that a single topic can be scaled horizontally across multiple servers to provide for performance far beyond the ability of a single server.

## Producers
Producers create messages. A message will be produced to a specific topic.
By default, the producer doesn't care what partition a specific message is written to and will balance messages over all partitions of a topic evenly. In some cases, the producer will direct messages to specific partitions using the message key and a partitioner with a given key will get written to the same partition.

## Consumers
Consumers read messages. The consumer subscribes to one or more topics and reads the messages in the order they were produced. The consumer keeps track of which messages it has already consumed by keeping track of the offset of messages. Each message as an incrementing offset and each message within a given partition has a unique offset. By storing the offset of the last consumed message for each partition, either in zookeeper or in Kafka itself, a consumer can stop and restart without losing its place.
