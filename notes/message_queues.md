# Message Queues

Streaming platforms have three key capabilities:
1. It lets you publish and subscribe to streams of records.
2. It lets you store streams of records in a fault-tolerant way.
3. It lets you process streams of records as they occur.


Two classes of application:
1. Building real-time streaming data pipelines that reliably get data between systems or applications
2. Building real-time streaming applications that transform or react to the streams of data


- Typically runs as a cluster on one or more servers
- Stores streams of *records* in categories called topics.
- Each record consists of a key, a value, and a timestamp.


Four core APIs:
1. Producer API
   - allows an application to publish a stream of records to one or more Kafka topics.
2. Consumer API
   - allows an application to subscribe to one or more topics and process the stream of records produced to them.
3. Streams API
   - allows an application to act as a stream processor, consuming an input stream from one or more topics and producing an output stream to one or more output topics, effectively transforming the input streams to output streams.
4. Connector API
   - allows building and running reusable producers or consumers that connect Kafka topics to existing applications or data systems. For example, a connector to a relational database might capture every change to a table.


## Topics and Logs
A topic is a category or feed name to which records are published.
Topics in Kafka are always multi-subscriber; that is, a topic can have zero or more consumers that subscribe to the data written to it.
Each topic has a partitioned log.
Each partition is an ordered, immutable sequence of records that is continually appended to.
Each record of a partition has a unique sequential id called the *offset*


The cluster retains all published records - whether or not they have been consumed - using a configurable retention period.
After the retention period, the data will be purged to make space.
The offset is controlled by the Consumer and it can consume records in any order it likes.


Partitions allow the log to scale beyond a size that will fit on a single server.
Each individual partition must fit on the servers that host it, but a topic may have many partitions so it can handle an arbitrary amount of data.
They also act as the unit of parallelism.

## Distribution
