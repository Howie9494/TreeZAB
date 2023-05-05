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
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong queuedPacketsSize = new AtomicLong();

    protected final AtomicLong packetsReceived = new AtomicLong();
    protected final AtomicLong packetsSent = new AtomicLong();
    /**
     * Marker packets would be added to quorum packet queue after every
     * markerPacketInterval packets.
     * It is ok if packetCounter overflows.
     */
    private final int markerPacketInterval = 1000;
    private AtomicInteger packetCounter = new AtomicInteger();

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

            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            childMaster.registerChildHandlerBean(this,sock);

            // Start thread that blast packets in the queue to child
            startSendingPackets();

            while(true){
//                QuorumPacket qp = new QuorumPacket();
//                ia.readRecord(qp, "packet");
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            LOG.error("Unexpected exception in ChildHandler: ", e);
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

                ServerMetrics.getMetrics().LEARNER_HANDLER_QP_SIZE.add(Long.toString(this.sid), queuedPackets.size());
                if (p instanceof ChildHandler.MarkerQuorumPacket) {
                    ChildHandler.MarkerQuorumPacket m = (ChildHandler.MarkerQuorumPacket) p;
                    ServerMetrics.getMetrics().LEARNER_HANDLER_QP_TIME
                            .add(Long.toString(this.sid), (System.nanoTime() - m.time) / 1000000L);
                    continue;
                }

                queuedPacketsSize.addAndGet(-packetSize(p));

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }

                oa.writeRecord(p, "packet");
                packetsSent.incrementAndGet();
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
        // Add a MarkerQuorumPacket at regular intervals.
        if (shouldSendMarkerPacketForLogging() && packetCounter.getAndIncrement() % markerPacketInterval == 0) {
            queuedPackets.add(new ChildHandler.MarkerQuorumPacket(System.nanoTime()));
        }
        queuedPacketsSize.addAndGet(packetSize(p));
    }

    /**
     * Tests need not send marker packets as they are only needed to
     * log quorum packet delays
     */
    protected boolean shouldSendMarkerPacketForLogging() {
        return true;
    }

    static long packetSize(QuorumPacket p) {
        /* Approximate base size of QuorumPacket: int + long + byte[] + List */
        long size = 4 + 8 + 8 + 8;
        byte[] data = p.getData();
        if (data != null) {
            size += data.length;
        }
        return size;
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

    private static class MarkerQuorumPacket extends QuorumPacket {

        long time;
        MarkerQuorumPacket(long time) {
            this.time = time;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ChildHandler.MarkerQuorumPacket that = (ChildHandler.MarkerQuorumPacket) o;
            return time == that.time;
        }

    }
}
