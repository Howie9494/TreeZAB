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

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SendProAckRequestProcessor implements RequestProcessor, Flushable {

    private static final Logger LOG = LoggerFactory.getLogger(SendProAckRequestProcessor.class);

    Follower follower;

    int viewSize;
    int childNum;

    SendProAckRequestProcessor(Learner peer) {
        if(peer instanceof Follower){
            this.follower = (Follower)peer;
            viewSize = follower.getViewSize();
            childNum = follower.getChildNum();
        }else{
            LOG.error("SendProAckRequestProcessor receive wrong param");
        }
    }

    public void processRequest(Request si) {
        if (si.type != OpCode.sync) {
            if(viewSize > 5 && childNum == 0){
                LOG.debug("zookeeper cluster greater than 5, no child nodes, send ack directly to parent.zxid:{}",Long.toHexString(si.getHdr().getZxid()));
                QuorumPacket qp = new QuorumPacket(Leader.ACK, si.getHdr().getZxid(), null, null);
                try {
                    si.logLatency(ServerMetrics.getMetrics().PROPOSAL_ACK_CREATION_LATENCY);
                    follower.writeFollowerPacket(qp, true);
                } catch (IOException e) {
                    LOG.warn("Closing connection to follower, exception during packet send", e);
                    try {
                        if (!follower.sock.isClosed()) {
                            follower.sock.close();
                        }
                    } catch (IOException e1) {
                        // Nothing to do, we are shutting things down, so an exception here is irrelevant
                        LOG.debug("Ignoring error closing the connection", e1);
                    }
                }
            }else if (viewSize <= 5 && follower.getParentIsLeader()){
                LOG.debug("zookeeper cluster less than or equal to 5, parent is leader, send ack directly to leader.zxid:{}",Long.toHexString(si.getHdr().getZxid()));
                QuorumPacket qp = new QuorumPacket(Leader.ACK, si.getHdr().getZxid(), null, null);
                try {
                    si.logLatency(ServerMetrics.getMetrics().PROPOSAL_ACK_CREATION_LATENCY);
                    follower.writePacket(qp, true);
                } catch (IOException e) {
                    LOG.warn("Closing connection to leader, exception during packet send", e);
                    try {
                        if (!follower.sock.isClosed()) {
                            follower.sock.close();
                        }
                    } catch (IOException e1) {
                        // Nothing to do, we are shutting things down, so an exception here is irrelevant
                        LOG.debug("Ignoring error closing the connection", e1);
                    }
                }
            }
        }
    }

    public void processAck(Long zxid,Long sid){
        LOG.debug("zookeeper cluster is greater than 5, process the ack of the child node and send it directly to the leader.zxid:{}",Long.toHexString(zxid));
        byte[] data = new byte[8];
        ByteBuffer.wrap(data).putLong(sid);
        QuorumPacket qp = new QuorumPacket(Leader.ACK, zxid, data, null);
        try {
            if (follower.getParentIsLeader()){
                follower.writePacket(qp,true);
            }else{
                follower.writeFollowerPacket(qp,true);
            }
        } catch (IOException e) {
            LOG.warn("Closing connection, exception during packet send", e);
            try {
                if (!follower.sock.isClosed()) {
                    follower.sock.close();
                }
            } catch (IOException e1) {
                // Nothing to do, we are shutting things down, so an exception here is irrelevant
                LOG.debug("Ignoring error closing the connection", e1);
            }
        }
    }

    public void flush() throws IOException {
        try {
            if(follower.getParentIsLeader() || !follower.self.getIsTreeCnxEnabled()){
                follower.writePacket(null,true);
            }else{
                follower.writeFollowerPacket(null,true);
            }
        } catch (IOException e) {
            LOG.warn("Closing connection, exception during packet send", e);
            try {
                if (!follower.sock.isClosed()) {
                    follower.sock.close();
                }
            } catch (IOException e1) {
                // Nothing to do, we are shutting things down, so an exception here is irrelevant
                LOG.debug("Ignoring error closing the connection", e1);
            }
        }
    }

    public void shutdown() {
        // Nothing needed
    }

}
