package org.apache.zookeeper.server.quorum;

import java.net.Socket;
import java.util.ArrayList;

public interface ChildMaster {

    /**
     * registers the handler's bean
     * @param childHandler handler
     * @param socket connection to learner
     */
    abstract void registerChildHandlerBean(ChildHandler childHandler, Socket socket);

    /**
     * unregisters the child's bean
     * @param childHandler handler
     */
    abstract void unregisterChildHandlerBean(ChildHandler childHandler);

    /**
     * start tracking a learner handler
     * @param childHandler to track
     */
    abstract void addChildHandler(ChildHandler childHandler);

    /**
     * stop tracking a child handler
     * @param childHandler to drop
     */
    abstract void removeChildHandler(ChildHandler childHandler);

    /**
     * next deadline tick marking observer sync (steady state)
     * @return next deadline tick marking observer sync (steady state)
     */
    abstract int getTickOfInitialAckDeadline();

    abstract void setTreeAckMap(Long zxid,Long sid);

    abstract ArrayList<Long> getTreeAckMap(Long zxid);

    abstract void removeTreeAckMap(Long zxid);
}
