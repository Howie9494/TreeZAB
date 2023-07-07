package org.apache.zookeeper.server.quorum;


import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards the proposal to the child
 */
public class ForwardProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardProposalRequestProcessor.class);

    private final FollowerZooKeeperServer zks;
    private final SyncRequestProcessor nextProcessor;

    public ForwardProposalRequestProcessor(FollowerZooKeeperServer zks, SyncRequestProcessor nextProcessor){
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void processRequest(Request request) {
        if (zks.getFollower().getChildNum() > 0){
            LOG.debug("Forward proposal package to child, Zxid :{}",Long.toHexString(request.zxid));
            zks.getFollower().forwardProposal(request);
        }
        nextProcessor.processRequest(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");

        nextProcessor.shutdown();
    }
}
