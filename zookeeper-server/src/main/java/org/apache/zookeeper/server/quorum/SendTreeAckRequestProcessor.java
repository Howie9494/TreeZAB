package org.apache.zookeeper.server.quorum;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * In TreeCnx, if childNum=0 then send ack directly to parent,
 * if childNum>0 then send ack to parent after receiving all child ack
 */
public class SendTreeAckRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SendTreeAckRequestProcessor.class);

    private final FollowerZooKeeperServer zks;

    protected volatile boolean stoppedMainLoop = true;

    Learner learner;

    boolean parentIsLeader;

    private Follower follower;

    private int childNum;

    /**
     * Incoming requests.
     */
    protected LinkedBlockingQueue<Long> queuedRequests = new LinkedBlockingQueue<Long>();

    public SendTreeAckRequestProcessor(FollowerZooKeeperServer zks,Learner learner){
        super("SendTreeAckRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.learner = learner;
        this.follower = zks.getFollower();
        this.childNum = follower.getChildNum();
    }

    @Override
    public void run() {
        if(follower.self.getView().size() <= 5){
            sendTreeAckThreadForLessOrEqualsFive();
        }else{
            sendTreeAckThreadForMoreThanFive();
        }

    }

    private void sendTreeAckThreadForLessOrEqualsFive(){
        try {
            parentIsLeader = follower.getParentIsLeader();

            do {
                Long zxid =queuedRequests.poll();
                if(zxid == null){
                    zxid = queuedRequests.take();
                }

                byte[] data;
                if(follower.getParentIsLeader()){
                    data = new byte[(childNum) << 3];
                }else{
                    data = new byte[(childNum + 1) << 3];
                }

                if(childNum == 0){
                    LOG.debug("No child, direct reply ack. zxid : {}",Long.toHexString(zxid));
                    follower.sendAck(data,zxid);
                }else{
                    CopyOnWriteArrayList<Long> sidList = follower.getTreeAckMap(zxid);
                    if(sidList == null || sidList.size() != childNum){
                        synchronized (this){
                            while(sidList == null || sidList.size() != childNum){
                                wait();
                                sidList = follower.getTreeAckMap(zxid);
                            }
                        }
                    }
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    int index = 0;
                    for (Long sid : sidList) {
                        buffer.putLong(index,sid);
                        index += 8;
                    }
                    LOG.debug("Receive ack messages from all children, send ack to parent. zxid : {}",Long.toHexString(zxid));
                    follower.sendAck(data,zxid);
                }
                follower.removeTreeAckMap(zxid);
            }while(stoppedMainLoop);

        } catch (Exception e) {
            handleException(this.getName(), e);
        }
    }

    private final HashMap<Long,Integer> ackCountMap = new HashMap<>();

    private void processAck(Long zxid){
        if(ackCountMap.containsKey(zxid)){
            Integer ackNum = ackCountMap.get(zxid);
            if(ackNum + 1 == childNum){
                ackCountMap.remove(zxid);
                follower.removeTreeAckMap(zxid);
            }else{
                ackCountMap.put(zxid,ackNum + 1);
            }
        }else{
            ackCountMap.put(zxid,1);
        }
    }

    private void sendTreeAckThreadForMoreThanFive(){
        try {
            parentIsLeader = follower.getParentIsLeader();

            do {
                Long zxid =queuedRequests.poll();
                if(zxid == null){
                    zxid = queuedRequests.take();
                }

                byte[] data;
                data = new byte[8];

                if(childNum == 0){
                    ByteBuffer.wrap(data).putLong(follower.self.getId());
                    LOG.debug("No child, direct reply ack. zxid : {}",Long.toHexString(zxid));
                    follower.sendAck(data,zxid);
                }else{
                    CopyOnWriteArrayList<Long> sidList = follower.getTreeAckMap(zxid);
                    if(sidList == null || sidList.get(0) != 0){
                        synchronized (this){
                            while(sidList == null || sidList.get(0) != 0){
                                wait();
                                sidList = follower.getTreeAckMap(zxid);
                            }
                        }
                    }
                    Long sid = sidList.get(0);
                    ByteBuffer.wrap(data).putLong(sid);
                    follower.removeSidTreeAckMap(zxid,sid);
                    processAck(zxid);
                    LOG.debug("Receive ack messages from children {}, send ack to parent. zxid : {}",sid,Long.toHexString(zxid));
                    follower.sendAck(data,zxid);
                }
            }while(stoppedMainLoop);

        } catch (Exception e) {
            handleException(this.getName(), e);
        }
    }

    @Override
    public void processRequest(Request request){
        if (request.type != ZooDefs.OpCode.sync) {
            request.logLatency(ServerMetrics.getMetrics().PROPOSAL_ACK_CREATION_LATENCY);
            queuedRequests.add(request.getHdr().getZxid());
            wakeup();
        }
    }

    public void ackListCheck(){
        wakeup();
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private synchronized void wakeup() {
        notifyAll();
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");

        halt();
    }

    private void halt() {
        stoppedMainLoop = false;
        wakeup();
    }

    public void flush(){
        try {
            if(parentIsLeader){
                learner.writePacket(null, true);
            }else{
                learner.writeFollowerPacket(null,true);
            }
        } catch (IOException e) {
            LOG.warn("Closing connection to leader, exception during packet send", e);
            try {
                if (!learner.sock.isClosed()) {
                    learner.sock.close();
                }
            } catch (IOException e1) {
                // Nothing to do, we are shutting things down, so an exception here is irrelevant
                LOG.debug("Ignoring error closing the connection", e1);
            }
        }
    }

}
