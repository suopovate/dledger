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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

/**
 * raft中的 Log 模块
 * 存储引擎
 * 1. 提供数据的存储
 * 2. 提供数据的读取、清除、管理
 * 3. 提供follower、leader不同角色的数据存储能力。
 */
public abstract class DLedgerStore {

    public abstract MemberState getMemberState();

    /**
     * 作为主节点，写数据
     */
    public abstract DLedgerEntry appendAsLeader(DLedgerEntry entry);

    /**
     * 作为从节点，写数据
     */
    public abstract DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId);

    /**
     * 获取数据
     */
    public abstract DLedgerEntry get(Long index);

    /**
     * 获取最尾数据的term
     */
    public abstract long getLedgerEndTerm();

    /**
     * 获取最尾数据的index
     */
    public abstract long getLedgerEndIndex();

    /**
     * 获取首条数据 之前的index
     */
    public abstract long getLedgerBeforeBeginIndex();

    /**
     * 获取首条数据 之前的term
     */
    public abstract long getLedgerBeforeBeginTerm();

    protected void updateLedgerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLedgerIndexAndTerm(getLedgerEndIndex(), getLedgerEndTerm());
        }
    }

    public abstract void flush();

    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    /**
     * truncate all entries in [truncateIndex ..]
     * @param truncateIndex truncate process since where
     * @return after truncate, store's end index
     */
    public abstract long truncate(long truncateIndex);

    /**
     * reset store's first entry, clear all entries in [.. beforeBeginIndex], make beforeBeginIndex + 1 to be first entry's index
     * @param beforeBeginIndex after reset process, beforeBegin entry's index
     * @param beforeBeginTerm after reset process, beforeBegin entry's  term
     * @return after reset, store's first log index
     */
    public abstract long reset(long beforeBeginIndex, long beforeBeginTerm);

    public abstract void resetOffsetAfterSnapshot(DLedgerEntry entry);

    public abstract void updateIndexAfterLoadingSnapshot(long lastIncludedIndex, long lastIncludedTerm);

    /**
     * 获取某个term的最末尾数据
     */
    public abstract DLedgerEntry getFirstLogOfTargetTerm(long targetTerm, long endIndex);

    public abstract void startup();

    public abstract void shutdown();

}
