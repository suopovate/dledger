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

package io.openmessaging.storage.dledger.protocol.protocol;

import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotRequest;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;

import java.util.concurrent.CompletableFuture;

public interface DLedgerRaftProtocol {

    /**
     * 成为candidata时：请求其他节点为自己投票
     */
    CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception;

    /**
     * 成为leader时：向其他节点维持心跳
     */
    CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception;

    /**
     * 这个，应该没用到吧，好像也不是raft论文里的做法，因为主节点不会去从节点拉数据的，主节点数据永远比从节点新！
     * 而从节点，也不应该主动向主节点拉取数据，而是主节点会定时心跳，传递数据。
     * 以上言论待定，应该也是可以从节点去拉数据的。
     */
    CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception;

    /**
     * 主节点向从节点推送数据
     */
    CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception;

    /**
     * 快照机制，暂时还不理解
     */
    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) throws Exception;

}
