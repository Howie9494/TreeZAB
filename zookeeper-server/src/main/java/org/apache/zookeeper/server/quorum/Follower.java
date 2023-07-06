/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Optional;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

import javax.security.sasl.SaslException;

/**
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner implements ChildMaster{

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;
    
    private static final boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");

    // the child acceptor thread
    volatile ChildCnxAcceptor cnxAcceptor = null;

    ObserverMaster om;

    private boolean parentIsLeader = true;
    private int childNum;
    private int level;

    private ConcurrentHashMap<Long,ArrayList<Long>> treeAckMap = new ConcurrentHashMap<Long,ArrayList<Long>>();

    private final List<ServerSocket> serverSockets = new LinkedList<>();

    Follower(final QuorumPeer self, final FollowerZooKeeperServer zk) throws IOException {
        this.self = Objects.requireNonNull(self);
        this.fzk = Objects.requireNonNull(zk);

        if (self.getIsTreeCnxEnabled()) {
            Set<InetSocketAddress> addresses;
            if (self.getQuorumListenOnAllIPs()) {
                addresses = self.getTreeAddress().getWildcardAddresses();
            } else {
                addresses = self.getTreeAddress().getAllAddresses();
            }

            addresses.stream()
                    .map(address -> createServerSocket(address, self.shouldUsePortUnification(), self.isSslQuorum()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(serverSockets::add);

            if (serverSockets.isEmpty()) {
                throw new IOException("Follower failed to initialize any of the sockets: " + addresses);
            }
        }

        this.zk = zk;
    }

    Optional<ServerSocket> createServerSocket(InetSocketAddress address, boolean portUnification, boolean sslQuorum) {
        ServerSocket serverSocket;
        try {
            if (portUnification || sslQuorum) {
                serverSocket = new UnifiedServerSocket(self.getX509Util(), portUnification);
            } else {
                serverSocket = new ServerSocket();
            }
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);
            return Optional.of(serverSocket);
        } catch (IOException e) {
            LOG.error("Couldn't bind to {}", address.toString(), e);
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:").append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    void followLeader() throws InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        ServerMetrics.getMetrics().ELECTION_TIME.add(electionTimeTaken);
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - {} {}", electionTimeTaken, QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);

        long connectionTime = 0;
        boolean completedSync = false;

        try {
            self.setZabState(QuorumPeer.ZabState.DISCOVERY);
            QuorumServer leaderServer = findLeader();
            try {
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                connectionTime = System.currentTimeMillis();
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);
                if(self.getIsTreeCnxEnabled()){
                    QuorumPacket qp = new QuorumPacket();
                    readPacket(qp);
                    if(qp.getType() == Leader.BuildTreeCnx){
                        Long parentSid = ByteBuffer.wrap(qp.getData()).getLong(0);
                        childNum = ByteBuffer.wrap(qp.getData()).getInt(8);
                        level = ByteBuffer.wrap(qp.getData()).getInt(12);
                        LOG.info("The parent sid of the CnxTree received for connection is {},The number of child nodes is {},level is {}",parentSid,childNum,level);
                        // Start thread that waits for connection requests from
                        // new followers.
                        if(childNum != 0){
                            cnxAcceptor = new Follower.ChildCnxAcceptor();
                            cnxAcceptor.start();
                        }
                        if(parentSid != leaderServer.getId()){
                            parentIsLeader = false;
                            QuorumServer parentServer = findCnxFollower(parentSid);
                            connectToParent(parentServer.treeAddr,parentServer.hostname);
                        }
                    }
                }
                if (self.isReconfigStateChange()) {
                    throw new Exception("learned about role change");
                }
                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch "
                             + ZxidUtils.zxidToString(newEpochZxid)
                             + " is less than our accepted epoch "
                             + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                long startTime = Time.currentElapsedTime();
                self.setLeaderAddressAndId(leaderServer.addr, leaderServer.getId());
                self.setZabState(QuorumPeer.ZabState.SYNCHRONIZATION);
                syncWithLeader(newEpochZxid,self.getIsTreeCnxEnabled(),childNum);
                self.setZabState(QuorumPeer.ZabState.BROADCAST);
                completedSync = true;
                long syncTime = Time.currentElapsedTime() - startTime;
                ServerMetrics.getMetrics().FOLLOWER_SYNC_TIME.add(syncTime);
                if (self.getObserverMasterPort() > 0) {
                    LOG.info("Starting ObserverMaster");

                    om = new ObserverMaster(self, fzk, self.getObserverMasterPort());
                    om.start();
                } else {
                    om = null;
                }
                if (!parentIsLeader) {
                    //Start a thread that accepts follower packet
                    new FollowerPacketProcess(fzk).start();
                }
                // create a reusable packet to reduce gc impact
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    readPacket(qp);
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                closeSocket();

                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            if (om != null) {
                om.stop();
            }
            zk.unregisterJMX(this);

            if (connectionTime != 0) {
                long connectionDuration = System.currentTimeMillis() - connectionTime;
                LOG.info(
                    "Disconnected from leader (with address: {}). Was connected for {}ms. Sync state: {}",
                    leaderAddr,
                    connectionDuration,
                    completedSync);
                messageTracker.dumpToLog(leaderAddr.toString());
            }
        }
    }

    public boolean getParentIsLeader() {
        return parentIsLeader;
    }

    public int getChildNum() {
        return childNum;
    }

    public ArrayList<Long> getTreeAckMap(Long zxid) {
        return treeAckMap.getOrDefault(zxid, null);
    }

    public void setTreeAckMap(Long zxid,Long sid) {
        if(!treeAckMap.containsKey(zxid)){
            ArrayList<Long> sidList = new ArrayList<>();
            sidList.add(sid);
            treeAckMap.put(zxid,sidList);
        }else{
            treeAckMap.get(zxid).add(sid);
        }
        ackListCheck();
    }

    public void removeTreeAckMap(Long zxid){
        treeAckMap.remove(zxid);
    }

    synchronized void closeSockets() {
        for (ServerSocket serverSocket : serverSockets) {
            if (!serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    LOG.warn("Ignoring unexpected exception during close {}", serverSocket, e);
                }
            }
        }
    }

    // list of all the learners, including followers and observers
    private final HashSet<ChildHandler> childs = new HashSet<ChildHandler>();

    // beans for all learners
    private final ConcurrentHashMap<ChildHandler, ChildHandlerBean> connectionBeans = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long,Integer> tryCommitMap = new ConcurrentHashMap<>();

    private int commitNum = -1;

    @Override
    public void tryToFollowerCommit(Long zxid,int ackNum) {
        if(commitNum == -1){
            commitNum = self.getView().size() >> 1;
        }

        if(zxid <= fzk.lastCommitZxid){
            return;
        }

        synchronized (fzk){
            if(ackNum == -1){
                tryCommitMap.put(zxid,ackNum);
            }
            if(tryCommitMap.containsKey(zxid)){
                Integer commitInfo = tryCommitMap.get(zxid);
                if(commitInfo > 0){
                    tryCommitMap.put(zxid,tryCommitMap.get(zxid) + ackNum);
                }
            }else{
                tryCommitMap.put(zxid,level + ackNum);
            }

            if (fzk.pendingTxns.isEmpty()){
                return;
            }
            if(fzk.pendingTxns.size() > 0){
                long firstElementZxid = fzk.pendingTxns.element().zxid;
                if(tryCommitMap.containsKey(firstElementZxid) &&
                        (tryCommitMap.get(firstElementZxid) == -1 || tryCommitMap.get(firstElementZxid) > commitNum)){
                    LOG.debug("More than half of the nodes have received the proposal message, follower commit zxid {}", Long.toHexString(firstElementZxid));
                    if(self.getView().size() == 3 && tryCommitMap.get(firstElementZxid) == level && ackNum == 0){
                        setTreeAckMap(firstElementZxid,-1L);
                    }
                    fzk.forwardAndCommit(firstElementZxid);
                    tryCommitMap.remove(firstElementZxid);
                }
            }
        }
    }

    @Override
    public void registerChildHandlerBean(ChildHandler childHandler, Socket socket) {
        ChildHandlerBean bean = new ChildHandlerBean(childHandler, socket);
        if (zk.registerJMX(bean)) {
            connectionBeans.put(childHandler, bean);
        }
    }

    @Override
    public void unregisterChildHandlerBean(ChildHandler childHandler) {
        ChildHandlerBean bean = connectionBeans.remove(childHandler);
        if (bean != null) {
            MBeanRegistry.getInstance().unregister(bean);
        }
    }

    @Override
    public void addChildHandler(ChildHandler childHandler) {
        synchronized (childs) {
            childs.add(childHandler);
        }
    }

    @Override
    public void removeChildHandler(ChildHandler childHandler) {
        synchronized (childs) {
            childs.remove(childHandler);
        }
    }

    @Override
    public int getTickOfInitialAckDeadline() {
        return self.tick.get() + self.initLimit + self.syncLimit;
    }

    public void forwardProposal(Request request) {
        byte[] data = SerializeUtils.serializeRequest(request);
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);
        sendPacketToChildPeer(pp);
    }

    public void forwardCommit(Request request) {
        byte[] data = SerializeUtils.serializeRequest(request);
        QuorumPacket pp = new QuorumPacket(Leader.COMMIT, request.zxid, data, null);
        sendPacketToChildPeer(pp);
    }

    private void sendPacketToChildPeer(QuorumPacket pp) {
        synchronized (childs) {
            for (ChildHandler f : childs) {
                f.queuePacket(pp);
            }
        }
    }

    long lastAckZxid;

    public void sendAck(byte[] data,long zxid) {
        synchronized (this){
            if (zxid <= lastAckZxid){
                return;
            }
            lastAckZxid = zxid;
        }
        QuorumPacket qp;
        if(data == null){
            qp = new QuorumPacket(Leader.ACK,zxid,null,null);
        }else{
            ByteBuffer.wrap(data).putLong(childNum << 3,self.getId());
            qp = new QuorumPacket(Leader.ACK,zxid,data,null);
        }

        try {
            if(parentIsLeader){
                writePacket(qp,false);
            }else {
                writeFollowerPacket(qp,false);
            }
        } catch (IOException e) {
            LOG.error("Exception during packet send");
        }
    }

    class FollowerPacketProcess extends ZooKeeperCriticalThread{

        public FollowerPacketProcess(FollowerZooKeeperServer fzk) {
            super("FollowerPacketProcess:" + fzk.getServerId(), fzk.getZooKeeperServerListener());
        }

        @Override
        public void run() {
            try {
                QuorumPacket qp = new QuorumPacket();
                while (isRunning()) {
                    readFollowerPacket(qp);
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.error("Exception for reading follower information",e);
            }
        }
    }

    class ChildCnxAcceptor extends ZooKeeperCriticalThread {

        private final AtomicBoolean stop = new AtomicBoolean(false);
        private final AtomicBoolean fail = new AtomicBoolean(false);

        public ChildCnxAcceptor() {
            super("ChildCnxAcceptor-" + serverSockets.stream()
                            .map(ServerSocket::getLocalSocketAddress)
                            .map(Objects::toString)
                            .collect(Collectors.joining("|")),
                    zk.getZooKeeperServerListener());
        }

        @Override
        public void run() {
            if (!stop.get() && !serverSockets.isEmpty()) {
                ExecutorService executor = Executors.newFixedThreadPool(serverSockets.size());
                CountDownLatch latch = new CountDownLatch(serverSockets.size());

                serverSockets.forEach(serverSocket ->
                        executor.submit(new ChildCnxAcceptorHandler(serverSocket, latch)));

                try {
                    latch.await();
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while sleeping in ChildCnxAcceptor.", ie);
                } finally {
                    closeSockets();
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                            LOG.error("not all the ChildCnxAcceptorHandler terminated properly");
                        }
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while terminating ChildCnxAcceptor.", ie);
                    }
                }
            }
        }

        public void halt() {
            stop.set(true);
            closeSockets();
        }

        class ChildCnxAcceptorHandler implements Runnable {
            private ServerSocket serverSocket;
            private CountDownLatch latch;

            ChildCnxAcceptorHandler(ServerSocket serverSocket, CountDownLatch latch) {
                this.serverSocket = serverSocket;
                this.latch = latch;
            }

            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("ChildCnxAcceptorHandler-" + serverSocket.getLocalSocketAddress());

                    while (!stop.get()) {
                        acceptConnections();
                    }
                } catch (Exception e) {
                    LOG.warn("Exception while accepting follower", e);
                    if (fail.compareAndSet(false, true)) {
                        handleException(getName(), e);
                        halt();
                    }
                } finally {
                    latch.countDown();
                }
            }

            private void acceptConnections() throws IOException {
                Socket socket = null;
                boolean error = false;
                try {
                    socket = serverSocket.accept();

                    // start with the initLimit, once the ack is processed
                    // in LearnerHandler switch to the syncLimit
                    socket.setSoTimeout(self.tickTime * self.initLimit);
                    socket.setTcpNoDelay(nodelay);

                    BufferedInputStream is = new BufferedInputStream(socket.getInputStream());
                    ChildHandler ch = new ChildHandler(socket, is, Follower.this);
                    ch.start();
                } catch (SocketException e) {
                    error = true;
                    if (stop.get()) {
                        LOG.warn("Exception while shutting down acceptor.", e);
                    } else {
                        throw e;
                    }
                } catch (SaslException e) {
                    LOG.error("Exception while connecting to quorum child", e);
                    error = true;
                } catch (Exception e) {
                    error = true;
                    throw e;
                } finally {
                    // Don't leak sockets on errors
                    if (error && socket != null && !socket.isClosed()) {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            LOG.warn("Error closing socket: " + socket, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Examine the packet received in qp and dispatch based on its contents.
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws Exception {
        switch (qp.getType()) {
        case Leader.PING:
            ping(qp);
            break;
        case Leader.PROPOSAL:
            ServerMetrics.getMetrics().LEARNER_PROPOSAL_RECEIVED_COUNT.add(1);
            TxnLogEntry logEntry = SerializeUtils.deserializeTxn(qp.getData());
            TxnHeader hdr = logEntry.getHeader();
            Record txn = logEntry.getTxn();
            TxnDigest digest = logEntry.getDigest();
            if (hdr.getZxid() != lastQueued + 1) {
                LOG.warn(
                    "Got zxid 0x{} expected 0x{}",
                    Long.toHexString(hdr.getZxid()),
                    Long.toHexString(lastQueued + 1));
            }
            lastQueued = hdr.getZxid();

            if (hdr.getType() == OpCode.reconfig) {
                SetDataTxn setDataTxn = (SetDataTxn) txn;
                QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData(), UTF_8));
                self.setLastSeenQuorumVerifier(qv, true);
            }
            if(self.getIsTreeCnxEnabled()){
                fzk.forwardAndLogRequest(hdr, txn, digest);
                tryToFollowerCommit(hdr.getZxid(),0);
            }else{
                fzk.logRequest(hdr, txn, digest);
            }
            if (hdr != null) {
                /*
                 * Request header is created only by the leader, so this is only set
                 * for quorum packets. If there is a clock drift, the latency may be
                 * negative. Headers use wall time, not CLOCK_MONOTONIC.
                 */
                long now = Time.currentWallTime();
                long latency = now - hdr.getTime();
                if (latency >= 0) {
                    ServerMetrics.getMetrics().PROPOSAL_LATENCY.add(latency);
                }
            }
            if (om != null) {
                final long startTime = Time.currentElapsedTime();
                om.proposalReceived(qp);
                ServerMetrics.getMetrics().OM_PROPOSAL_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
            }
            break;
        case Leader.COMMIT:
            ServerMetrics.getMetrics().LEARNER_COMMIT_RECEIVED_COUNT.add(1);
            if(self.getIsTreeCnxEnabled()){
                LOG.debug("receive Commit msg , zxid :{}",Long.toHexString(qp.getZxid()));
                tryToFollowerCommit(qp.getZxid(),-1);
            }else{
                fzk.commit(qp.getZxid());
            }
            if (om != null) {
                final long startTime = Time.currentElapsedTime();
                om.proposalCommitted(qp.getZxid());
                ServerMetrics.getMetrics().OM_COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
            }
            break;

        case Leader.COMMITANDACTIVATE:
            // get the new configuration from the request
            Request request = fzk.pendingTxns.element();
            SetDataTxn setDataTxn = (SetDataTxn) request.getTxn();
            QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData(), UTF_8));

            // get new designated leader from (current) leader's message
            ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
            long suggestedLeaderId = buffer.getLong();
            final long zxid = qp.getZxid();
            boolean majorChange = self.processReconfig(qv, suggestedLeaderId, zxid, true);
            // commit (writes the new config to ZK tree (/zookeeper/config)
            fzk.commit(zxid);

            if (om != null) {
                om.informAndActivate(zxid, suggestedLeaderId);
            }
            if (majorChange) {
                throw new Exception("changes proposed in reconfig");
            }
            break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Follower started");
            break;
        case Leader.REVALIDATE:
            if (om == null || !om.revalidateLearnerSession(qp)) {
                revalidate(qp);
            }
            break;
        case Leader.SYNC:
            fzk.sync();
            break;
        default:
            LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
            break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        synchronized (fzk) {
            return fzk.getZxid();
        }
    }

    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    public Integer getSyncedObserverSize() {
        return om == null ? null : om.getNumActiveObservers();
    }

    public Iterable<Map<String, Object>> getSyncedObserversInfo() {
        if (om != null && om.getNumActiveObservers() > 0) {
            return om.getActiveObservers();
        }
        return Collections.emptySet();
    }

    public void resetObserverConnectionStats() {
        if (om != null && om.getNumActiveObservers() > 0) {
            om.resetObserverConnectionStats();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("shutdown Follower");
        super.shutdown();
    }

}
