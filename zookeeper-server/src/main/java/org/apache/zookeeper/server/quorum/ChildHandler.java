package org.apache.zookeeper.server.quorum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.util.MessageTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.apache.zookeeper.server.quorum.LearnerHandler.packetToString;

/**
 * There will be an instance of this class created by the Follower for each
 * learner. All communication with a child is handled by this
 * class.
 */
public class ChildHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(ChildHandler.class);

    protected final Socket sock;

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;
    private final BufferedInputStream bufferedInput;
    private BufferedOutputStream bufferedOutput;

    AtomicBoolean sockBeingClosed = new AtomicBoolean(false);

    protected final MessageTracker messageTracker;

    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this child should
     * be considered no longer "sync'd" with the parent. */
    volatile long tickOfNextAckDeadline;

    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();

    final ChildMaster childMaster;

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    String getRemoteAddress() {
        return sock == null ? "<null>" : sock.getRemoteSocketAddress().toString();
    }


    public ChildHandler(Socket sock, BufferedInputStream bufferedInput, ChildMaster childMaster) {
        super("ChildHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.bufferedInput = bufferedInput;

        this.childMaster = childMaster;
        this.messageTracker = new MessageTracker(MessageTracker.BUFFERED_MESSAGE_SIZE);
    }

    @Override
    public void run() {
        try {
            childMaster.addChildHandler(this);
            LOG.info("add child handler : {} to childs",sock.getRemoteSocketAddress());
            tickOfNextAckDeadline = childMaster.getTickOfInitialAckDeadline();

            ia = BinaryInputArchive.getArchive(new BufferedInputStream(bufferedInput));
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            childMaster.registerChildHandlerBean(this,sock);
            // Start thread that blast packets in the queue to child
            startSendingPackets();

            while(true){
                boolean loopRead = true;
                QuorumPacket qp = new QuorumPacket();
                while(loopRead){
                    try {
                        ia.readRecord(qp, "packet");
                        loopRead = false;
                    } catch (SocketTimeoutException e) {
                        //No processing required
                    }
                }
                messageTracker.trackReceived(qp.getType());
                LOG.debug("The ChildHandler receives a message from the child, type: {}",qp.getType());
                if(qp.getType() == Leader.ACK){
                    ByteBuffer wrap = ByteBuffer.wrap(qp.getData());
                    Long zxid = qp.getZxid();
                    for(int i = 0;i < qp.getData().length;i += 8){
                        childMaster.setTreeAckMap(zxid,wrap.getLong(i));
                    }
                    childMaster.tryToFollowerCommit(zxid,qp.getData().length >> 3);
                }else{
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("Unexpected exception in ChildHandler: ", e);
            childMaster.removeChildHandler(this);
            closeSocket();
        } finally {
            String remoteAddr = getRemoteAddress();
            LOG.warn("******* GOODBYE CHILD{} ********", remoteAddr);
            messageTracker.dumpToLog(remoteAddr);
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the child
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        while(true){
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                LOG.debug("ChildHandler sends a message to child.");
                oa.writeRecord(p, "packet");
                messageTracker.trackSent(p.getType());
            }catch (IOException e) {
                LOG.error("Exception while sending packets in ChildHandler", e);
                // this will cause everything to shutdown on
                // this child handler and will help notify
                // the learner/observer instantaneously
                closeSocket();
                break;
            }
        }
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    void closeSocket() {
        if (sock != null && !sock.isClosed() && sockBeingClosed.compareAndSet(false, true)) {
            LOG.info("Synchronously closing socket to child {}.", getSid());
            closeSockSync();
        }
    }

    void closeSockSync() {
        try {
            if (sock != null) {
                long startTime = Time.currentElapsedTime();
                sock.close();
                ServerMetrics.getMetrics().SOCKET_CLOSING_TIME.add(Time.currentElapsedTime() - startTime);
            }
        } catch (IOException e) {
            LOG.warn("Ignoring error closing connection to child {}", getSid(), e);
        }
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.clear();
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }

        closeSocket();

        this.interrupt();
        childMaster.removeChildHandler(this);
        childMaster.unregisterChildHandlerBean(this);
    }

}
