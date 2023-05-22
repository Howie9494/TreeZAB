# TreeZAB
Based on https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1.tar.gz

基于tree的ZAB集群性能优化。

ZAB cluster performance optimization based on tree.

### 介绍
基于树结构的事务提交可以通过zoo.cfg设置是否开启（默认开启）。
```
isTreeCnxEnabled=false
```
1. 相比于原结构可用性有所降低。当集群节点=5时最坏的情况为2个节点宕机，则集群无法正常运行，开始进行recovery；而原结构在两个节点的宕机的情况下仍能正常运行（超过半数）。当集群节点增加时，在最坏的情况下导致集群无法正常工作仍然只需要2个节点。

2. 相比于原结构在写请求量增加后性能有所提升，更稳定，更晚出现因节点故障导致的Recovery。Zookeeper在写操作的过程中Leader承受大量负担，而Follower大多数时间处于等待状态。leader会因为负担过多而崩溃，导致重新进行leader选举，集群无法正常工作。当使用树结构优化原本的Zookeeper结构，让Follower分担部分Leader的负载，尽管会导造成短暂的性能下降，但当写请求增加时优化后的性能更稳定且更晚出现节点故障的问题。

3. 相比于原结构受网络影响更为严重。优化后的TreeZAB结构因为需要进行消息的二次转发和多次ACK等待，所以仅能在Tree-FriendLy-NetWork中保持和原结构一样的性能，而在Tree-Unfriendly-NetWork中由于网络原因性能下降的现象更严重。

### Intro
Tree based transaction commit can be set to be on or off via zoo.cfg (it is on by default).
```
isTreeCnxEnabled=false
```
1. compared to the original structure availability has been reduced。 when the cluster node = 5 the worst case is 2 nodes down, then the cluster can not work properly, and start recovery; while the original structure in the case of two nodes down can still work properly (more than half). When the cluster nodes are added, in the worst case scenario resulting in the cluster not working properly still only 2 nodes are needed.

2. The Zookeeper leader is heavily burdened during writing operations, while the Follower spends most of its time in a waiting state. The cluster does not work properly. When the original Zookeeper structure is optimised using the tree structure, the Follower shares some of the Leader's load, which causes a temporary performance degradation, but the optimised performance is more stable and less prone to node failure when the number of write requests increases.

3. more severely impacted by the network than the original structure. The optimised TreeZAB structure only maintains the same performance as the original structure in Tree-FriendLy-NetWork due to the need for secondary forwarding of messages and multiple ACK waits, while in Tree-Unfriendly-NetWork the performance degradation is more severe due to the network.
