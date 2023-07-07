package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards the commit to the child
 */
public class ForwardCommitRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardCommitRequestProcessor.class);

    private final FollowerZooKeeperServer zks;
    private final CommitProcessor nextProcessor;

    public ForwardCommitRequestProcessor(FollowerZooKeeperServer zks, CommitProcessor nextProcessor){
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void processRequest(Request request) {
        if(zks.getFollower().getChildNum() > 0){
            LOG.debug("Forward commit package to child. zxid :{}",Long.toHexString(request.zxid));
            zks.getFollower().forwardCommit(request);
        }
        nextProcessor.commit(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");

        nextProcessor.shutdown();
    }
}
