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

package io.openmessaging.storage.dledger.protocol;

import static io.openmessaging.storage.dledger.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    public enum RESULT {
        UNKNOWN,
        ACCEPT,
        /**
         * 如果这个leaderId未存在于当前节点的peer池中
         */
        REJECT_UNKNOWN_LEADER,
        /**
         * 很奇怪的异常...如果发起投票的不是当前节点自己，但是请求中的leaderId又是当前节点自己的id就抛这个
         * tips: 也就是同一个集群组中 出现了两个节点 是同一个id
         */
        REJECT_UNEXPECTED_LEADER,
        /**
         * 候选者的term比当前节点term更低
         * tips: 这个投票过期了，已经有节点抢先了
         */
        REJECT_EXPIRED_VOTE_TERM,
        /**
         * 投给别的候选者了
         */
        REJECT_ALREADY_VOTED,
        /**
         * 本term已经选出了leader
         */
        REJECT_ALREADY_HAS_LEADER,
        /**
         * 当前节点的term还没跟候选者的term保持一致(主要指候选者的term>本节点的term)
         */
        REJECT_TERM_NOT_READY,
        /**
         * 候选者的term 小于当前节点 最后一条日志的term，说明候选者已经out了
         * tips: 可能是网络分区的原因，本节点已经处于一个选出了新leader的新term中，且写入了新日志。
         */
        REJECT_TERM_SMALL_THAN_LEDGER,
        /**
         * 候选者最后一条日志的term 小于 当前节点最后一条日志的term，说明候选者数据不够新
         */
        REJECT_EXPIRED_LEDGER_TERM,
        /**
         *
         */
        REJECT_SMALL_LEDGER_END_INDEX,
        REJECT_TAKING_LEADERSHIP;
    }

    public enum ParseResult {
        /**
         * 重新选举，不会升级term
         */
        WAIT_TO_REVOTE,
        /**
         * 立即重新选举
         */
        REVOTE_IMMEDIATELY,
        /**
         * 选举完成
         */
        PASSED,
        /**
         * 等待进行一次新的选举，会升级term
         */
        WAIT_TO_VOTE_NEXT;
    }

    @Override
    public String toString() {
        return "VoteResponse{" +
                "group='" + group + '\'' +
                ", remoteId='" + remoteId + '\'' +
                ", localId='" + localId + '\'' +
                ", code=" + code +
                ", leaderId='" + leaderId + '\'' +
                ", term=" + term +
                ", voteResult=" + voteResult +
                '}';
    }
}
