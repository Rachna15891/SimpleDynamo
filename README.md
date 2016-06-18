# SimpleDynamo
Replicated Key-Value Storage (Dynamo-style)

This application is a very simplified version of Dynamo. There are three main pieces that have been implemented:
```sh
1. Partitioning
2. Replication
3. Failure handling
```
The main goal is to provide both availability and linearizability at the same time. In other words, the implementation will always perform read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value. Partitioning and replication are done exactly the way Dynamo does.

Just like Simple DHT, it supports and implements all storage functionalities i.e. Create server and client threads, open sockets, and respond to incoming requests.

There is support for insert/query/delete operations. Also, there is support for the @ and * queries.

There are always 5 nodes in the system. Adding/removing nodes from the system is not implemented.

There is support for at most 1 node failure at any given time.

All failures are temporary.

When a node recovers, it copies all the object writes it missed during the failure. This is done by asking the right nodes and then copy from them.

It supports concurrent read/write operations.

It handles a failure happening at the same time with read/write operations.

Replication is done exactly the same way as Dynamo does. In other words, a (key, value) pair is replicated over three consecutive partitions, starting from the partition that the key belongs to.

Unlike Dynamo, there are two things which have not been implemented.

- Virtual nodes: In this implementation we use physical nodes rather than virtual nodes, i.e., all partitions are static and fixed.
- Hinted handoff: This implementation does not implement hinted handoff. This means that when there is a failure, replication takes place only on two nodes.

All replicas store the same value for each key. This is “per-key” consistency. There is no consistency guarantee across keys. More formally, per-key linearizability is implemented.

Each content provider instance has a node id derived from its emulator port. This node id is obtained by applying the hash function (i.e., genHash()) to the emulator port. For example, the node id of the content provider instance running on emulator-5554 should be,
```sh
node_id = genHash(“5554”);
```
This is necessary to find the correct position of each node in the Dynamo ring.

Use the python scripts to create, run and set the ports of the AVD’s, by using the following commands
```sh
python create_avd.py 5
python run_avd.py 5
python set_redir.py 10000
```
The redirection ports for the AVD’s will be-
```sh
emulator-5554: “5554” - 11108
emulator-5556: “5556” - 11112
emulator-5558: “5558” - 11116
emulator-5560: “5560” - 11120
emulator-5562: “5562” - 11124
```
The app opens one server socket that listens on 10000.

Based on the design of Amazon Dynamo, the following features have been implemented:
### Membership

Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.
### Request routing

Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node. Under no failures, all requests are directly forwarded to the coordinator, and the coordinator should be in charge of serving read/write operations.
### Chain replication

This replication strategy provides linearizability. In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write. A read operation always comes to the last partition and reads the value from the last partition.
### Failure handling

Handling failures should be done very carefully because there can be many corner cases to consider and cover. Just as the original Dynamo, each request is used to detect a node failure. For this purpose, a timeout for a socket read is used; and if a node does not respond within the timeout, it is considered as a failed node. When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.
