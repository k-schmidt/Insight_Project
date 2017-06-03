# The CAP Theorem

- CAP Theorem was originally proposed by Eric Brewer in 2000.
- **Given the three properties of Consistency, Availability, and Partition tolerance, you can only achieve two.**
- Availability
    - If you can talk to a node in the cluster, it can read and write data.
- Partition Tolerance
    - The cluster can survive communication breakages in the cluster that separate the cluster into multiple partitions unable to communicate with each other.
- A single-server system is the obvious example of a CA system - a system that has Consistency and Availability but not Partition Tolerance. A single machine can't partition.
- Although the CAP theorem if often stated as "you can only get two out of three," in practice what it's saying is that in a system that may suffer partitions, as distributed systems do, you have to trade off consistency versus availability.
- In each specific use case you need to consider different tolerances for Consistency and Availability. A news website might be able to tolerate older news on their site than a high freqency trading firm.

## CAP Theorem
- Eric Brewer
- Consistency
    - When I read from it, I will always see what I've most recently written.
- Availability
    - When I ask, I will always get an answer.
- Partition Tolerance
    - Nodes could be cutoff from the rest of the cluster.
    - Broken network cable
    - Power going down.
    - Database is able to heal if there is a split.


If we have a distributed system then we have to have Partition Tolerance.
