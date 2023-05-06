package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class ParentSender extends ZooKeeperCriticalThread {

    private static final Logger LOG = LoggerFactory.getLogger(ParentSender.class);

    private final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<>();
    private final QuorumPacket proposalOfDeath = new QuorumPacket();

    Learner learner;

    public ParentSender(Learner learner) {
        super("ParentSender:" + learner.zk.getServerId(), learner.zk.getZooKeeperServerListener());
        this.learner = learner;
    }

    @Override
    public void run() {
        while (true) {
            try {
                QuorumPacket p = queuedPackets.poll();
                if (p == null) {
                    learner.parentBufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }

                learner.messageTracker.trackSent(p.getType());
                learner.parentOs.writeRecord(p, "packet");
            } catch (IOException e) {
                handleException(this.getName(), e);
                break;
            } catch (InterruptedException e) {
                handleException(this.getName(), e);
                break;
            }
        }

        LOG.info("ParentSender exited");
    }

    public void queuePacket(QuorumPacket pp) {
        queuedPackets.add(pp);
    }

    public void shutdown() {
        LOG.info("Shutting down ParentSender");
        queuedPackets.clear();
        queuedPackets.add(proposalOfDeath);
    }
}
