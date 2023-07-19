# TreeZAB
Based on https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1.tar.gz

基于tree的ZAB集群性能优化。

ZAB cluster performance optimization based on tree.

<img width="927" alt="tree-cluster" src="https://github.com/Howie9494/TreeZAB/assets/97783236/1f355d3c-28d1-47a9-91c6-8c3dcee353c0">

### 介绍
基于树结构的事务提交可以通过zoo.cfg设置是否开启（默认开启）。
```
isTreeCnxEnabled=false
```
1. 相比于原结构可用性有所降低。当集群节点=5时最坏的情况为2个节点宕机，则集群无法正常运行，开始进行recovery；而原结构在两个节点的宕机的情况下仍能正常运行（超过半数）。当集群节点增加时，在最坏的情况下导致集群无法正常工作仍然只需要2个节点。

2. 相比于原结构多数节点性能有所提升，更稳定，更晚出现因节点故障导致的Recovery。Zookeeper在写操作的过程中Leader承受大量负担，而Follower大多数时间处于等待状态。leader会因为负担过多而崩溃，导致重新进行leader选举，集群无法正常工作。当使用树结构优化原本的Zookeeper结构，让Follower分担部分Leader的负载。并且增加Follower提交机制可以大幅降低结构改变造成的影响，以应对写操作负载较低的常营。

3. 相比于原结构受网络影响更为严重。优化后的TreeZAB结构因为需要进行消息的二次转发和多次ACK等待，所以仅能在Tree-FriendLy-NetWork中保持和原结构一样的性能，而在Tree-Unfriendly-NetWork中由于网络原因性能下降的现象更严重。

### Intro
Tree based transaction commit can be set to be on or off via zoo.cfg (it is on by default).
```
isTreeCnxEnabled=false
```
1. The availability of the cluster is reduced when compared to the original structure. If there are 5 cluster nodes and 2 nodes go down, the cluster cannot function properly and will need to start recovery. However, in the original structure, even with 2 nodes down, the cluster can still work properly, albeit with reduced efficiency. If the number of cluster nodes increases, the worst-case scenario still only requires 2 nodes to be operational.

2. In comparison to the original structure, most of the nodes in the optimized structure have improved performance, increased stability, and faster recovery from node failure. In the original structure, the zookeeper bears a significant burden during write operations, while followers mostly wait. This can cause the leader to collapse and result in a new leader election, disrupting the cluster's normal functioning. In the optimized Zookeeper structure, the leader's burden is partially shared by the follower, and the addition of the Follower submission mechanism can significantly reduce the impact of structural changes to cope with low write operation loads in the regular camp.

3. The optimized TreeZAB structure is more seriously affected by the network when compared to the original structure. In Tree-FriendLy-NetWork, the performance remains the same as the original structure. However, in Tree-Unfriendly-NetWork, the network-related performance degradation is more severe due to the need to forward messages twice and wait for multiple ACKs.

## 请求处理链
<img width="1030" alt="Chain of Responsibility2 0" src="https://github.com/Howie9494/TreeZAB/assets/97783236/06558a16-ea40-480e-b5cb-b5586fa58fb5">


