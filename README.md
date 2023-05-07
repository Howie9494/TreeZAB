# TreeZAB
Based on https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1.tar.gz

基于tree的ZAB集群性能优化。

ZAB cluster performance optimization based on tree.

### 介绍
基于树结构的事务提交可以通过zoo.cfg设置是否开启（默认开启）。
```
isTreeCnxEnabled=false
```
1. 相比于原结构可用性有所降低，当集群节点=5时最坏的情况为2个节点宕机，则集群无法正常运行，开始进行recovery；而原结构在两个节点的宕机的情况下仍能正常运行（超过半数）。

2. 当集群节点增加时，在最坏的情况下导致集群无法正常工作仍然只需要2个节点。

3. 某种程度上更容易导致Recovery的发生，而Recovery耗时长。在集群正常运行的情况下树结构将Leader的压力分解给Follower，当事务请求量大时使吞吐量可以提升，但需要更加注意避免Recovery的发生。

### Intro
Tree based transaction commit can be set to be on or off via zoo.cfg (it is on by default).
```
isTreeCnxEnabled=false
```
1. Compared to the original structure the availability is reduced, when the cluster node = 5 the worst case scenario is that 2 nodes are down, then the cluster cannot run normally and recovery begins; whereas the original structure can still run normally (more than half) in the case of two nodes being down.

2. When the cluster nodes are added, in the worst case scenario resulting in the cluster not working properly still only 2 nodes are needed.

3. It is somewhat more likely to lead to Recovery, which is time-consuming. The tree structure breaks down the pressure from the Leader to the Follower in the normal operation of the cluster, allowing throughput to increase when there is a high volume of transaction requests, but more attention needs to be paid to avoiding Recovery.
