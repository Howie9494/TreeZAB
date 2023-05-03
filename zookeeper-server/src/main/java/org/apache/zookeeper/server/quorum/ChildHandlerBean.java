package org.apache.zookeeper.server.quorum;


import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ChildHandlerBean implements LearnerHandlerMXBean, ZKMBeanInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ChildHandlerBean.class);

    private final ChildHandler childHandler;
    private final String remoteAddr;

    public ChildHandlerBean(final ChildHandler childHandler, final Socket socket) {
        this.childHandler = childHandler;
        InetSocketAddress sockAddr = (InetSocketAddress) socket.getRemoteSocketAddress();
        if (sockAddr == null) {
            this.remoteAddr = "Unknown";
        } else {
            this.remoteAddr = sockAddr.getAddress().getHostAddress() + ":" + sockAddr.getPort();
        }
    }

    @Override
    public String getName() {
        return MBeanRegistry.getInstance()
                .makeFullPath(
                        "Child_Connections",
                        ObjectName.quote(remoteAddr),
                        String.format("\"id:%d\"", childHandler.getSid()));
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public void terminateConnection() {
        LOG.info("terminating learner handler connection on demand {}", toString());
        childHandler.shutdown();
    }

    @Override
    public String toString() {
        return "ChildHandlerBean{remoteIP=" + remoteAddr + ",ServerId=" + childHandler.getSid() + "}";
    }
}
