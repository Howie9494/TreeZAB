package org.apache.zookeeper.server.quorum;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    /**
     * Incoming requests.
     */
    protected LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    public SendTreeAckRequestProcessor(FollowerZooKeeperServer zks,Learner learner){
        super("SendTreeAckRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.learner = learner;
    }

    @Override
    public void run() {
        try {
            //收到数量==childnum 把sid都取出来加上自己的打包发给parent
            int childNum = zks.getFollower().getChildNum();
            parentIsLeader = zks.getFollower().getParentIsLeader();

            Request request = queuedRequests.take();
            do {
                if(queuedRequests.size() < childNum){
                    synchronized (this){
                        while(queuedRequests.size() < childNum){
                            wait();
                        }
                    }
                }

                byte[] data = new byte[(childNum + 1) << 3];
                long zxid = request.getHdr().getZxid();
                zks.getFollower().sendAck(data,zxid);

            }while(stoppedMainLoop);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
    }

    @Override
    public void processRequest(Request request){
        LOG.debug("Processing request:: {}", request);
        queuedRequests.add(request);
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

    public void flush() throws IOException {
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
