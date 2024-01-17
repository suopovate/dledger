/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineRequest;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于发起请求
 */
public class DLedgerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerClient.class);
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    private final String group;
    private String leaderId;
    private final DLedgerClientRpcService dLedgerClientRpcService;

    private final MetadataUpdater metadataUpdater = new MetadataUpdater("MetadataUpdater", LOGGER);

    public DLedgerClient(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        dLedgerClientRpcService = new DLedgerClientRpcNettyService();
        dLedgerClientRpcService.updatePeers(peers);
        // 随便找一个当成 leaderId来发？
        leaderId = peerMap.keySet().iterator().next();
    }

    public AppendEntryResponse append(byte[] body) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            AppendEntryResponse response = dLedgerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.append(appendEntryRequest).get();
                }
            }
            return response;
        } catch (Exception e) {
            // 异常-归为内部错误
            needFreshMetadata();
            LOGGER.error("Append error", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLedgerClientRpcService.get(request).get();
            if (response.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLedgerClientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception t) {
            needFreshMetadata();
            LOGGER.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
            getEntriesResponse.setCode(DLedgerResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }

    public LeadershipTransferResponse leadershipTransfer(String curLeaderId, String transfereeId, long term) {

        try {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setGroup(group);
            request.setRemoteId(curLeaderId);
            request.setTransferId(curLeaderId);
            request.setTransfereeId(transfereeId);
            request.setTerm(term);
            return dLedgerClientRpcService.leadershipTransfer(request).get();
        } catch (Exception t) {
            needFreshMetadata();
            LOGGER.error("leadershipTransfer to {} error", transfereeId, t);
            return new LeadershipTransferResponse().code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
        }
    }

    public <T extends UserDefineRequest, V extends UserDefineResponse> V invokeUserDefineRequest(T request, Class<V> aClass, boolean onlyForLeader) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (onlyForLeader && leaderId == null) {
                V response = aClass.newInstance();
                response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                return response;
            }
            request.setGroup(group);
            request.setRemoteId(leaderId);
            V resp = dLedgerClientRpcService.invokeUserDefineRequest(request, aClass).get();
            if (resp.getCode() == DLedgerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, false);
                if (onlyForLeader && leaderId == null) {
                    V response = aClass.newInstance();
                    response.setCode(DLedgerResponseCode.METADATA_ERROR.getCode());
                    return response;
                }
                request.setGroup(group);
                request.setRemoteId(leaderId);
                resp = dLedgerClientRpcService.invokeUserDefineRequest(request, aClass).get();
            }
            return resp;
        } catch (Exception e) {
            needFreshMetadata();
            LOGGER.error("invoke user define request error, request: {}", request, e);
            try {
                V response = aClass.newInstance();
                response.code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
                return response;
            } catch (Exception exception) {
                LOGGER.error("get instance {} error", aClass, exception);
                return null;
            }
        }
    }

    public void startup() {
        this.dLedgerClientRpcService.startup();
        this.metadataUpdater.start();
    }

    public void shutdown() {
        this.dLedgerClientRpcService.shutdown();
        this.metadataUpdater.shutdown();
    }

    private void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            String nodeId = peerInfo.split("-")[0];
            peerMap.put(nodeId, peerInfo.substring(nodeId.length() + 1));
        }
    }

    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
    }

    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (DLedgerUtils.elapsed(start) < maxWaitMs && leaderId == null) {
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private class MetadataUpdater extends ShutdownAbleThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }

        private void getMetadata(String peerId, boolean isLeader) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                CompletableFuture<MetadataResponse> future = dLedgerClientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        peerMap.putAll(response.getPeers());
                        dLedgerClientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                if (isLeader) {
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override
        public void doWork() {
            try {
                if (leaderId == null) {
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (DLedgerClient.this) {
                                DLedgerClient.this.notifyAll();
                            }
                            DLedgerUtils.sleep(1000);
                            break;
                        }
                    }
                } else {
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                DLedgerUtils.sleep(1000);
            }
        }
    }

}
