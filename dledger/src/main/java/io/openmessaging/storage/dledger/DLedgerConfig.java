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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.metrics.MetricsExporterType;
import io.openmessaging.storage.dledger.snapshot.SnapshotEntryResetStrategy;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;

import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DLedgerConfig {

    public static final String MEMORY = "MEMORY";
    public static final String FILE = "FILE";
    public static final String MULTI_PATH_SPLITTER = System.getProperty("dLedger.multiPath.Splitter", ",");
    private String configFilePath;  // 配置文件路径

    private String group = "default";  // 分组名称，默认为"default"

    private String selfId = "n0";  // 当前节点的标识符，默认为"n0"

    private String peers = "n0-localhost:20911";  // 节点和对应主机地址的映射关系，默认为"n0-localhost:20911"

    private String storeBaseDir = File.separator + "tmp" + File.separator + "dledgerstore";  // 存储基础目录路径，默认为当前目录下的"tmp\dledgerstore"

    private String readOnlyDataStoreDirs = null;  // 仅读数据存储目录，默认为null

    private int peerPushThrottlePoint = 300 * 1024 * 1024;  // 节点间推送数据的限制点，默认为300MB

    private int peerPushQuota = 20 * 1024 * 1024;  // 节点间推送数据的配额，默认为20MB

    private String storeType = FILE; //FILE, MEMORY  // 存储类型，默认为FILE，表示文件存储

    private String dataStorePath;  // 数据存储路径

    private int maxPendingRequestsNum = 10000;  // 最大待处理请求数，默认为10000

    private int maxWaitAckTimeMs = 2500;  // 最大等待确认时间，默认为2500ms

    private int maxPushTimeOutMs = 1000;  // 最大推送超时时间，默认为1000ms

    /**
     * 是否开启leader选举功能，即当前节点具备选举和角色切换的能力，不开启的话，本节点就不会主动切换角色，或者维护当前角色。
     * false: 估计就是永远都是follow?
     */
    private boolean enableLeaderElector = true;  // 是否启用leader选举，默认为true

    private int heartBeatTimeIntervalMs = 2000;  // 心跳间隔时间，默认为2000ms

    private int maxHeartBeatLeak = 3;  // 最大心跳泄漏数，默认为3

    private int minVoteIntervalMs = 300;  // 最小选举间隔时间，默认为300ms

    private int maxVoteIntervalMs = 1000;  // 最大选举间隔时间，默认为1000ms

    private int fileReservedHours = 72;  // 保留文件的时间长度，默认为72小时

    private String deleteWhen = "04";  // 文件删除的时间，格式为HH，默认为"04"表示4点

    private float diskSpaceRatioToCheckExpired = Float.parseFloat(System.getProperty("dledger.disk.ratio.check", "0.70"));  // 检查过期数据的磁盘空间比例，默认为System.getProperty("dledger.disk.ratio.check")的值，若不存在则默认为0.70

    private float diskSpaceRatioToForceClean = Float.parseFloat(System.getProperty("dledger.disk.ratio.clean", "0.85"));  // 强制清理数据的磁盘空间比例，默认为System.getProperty("dledger.disk.ratio.clean")的值，若不存在则默认为0.85

    private boolean enableDiskForceClean = true;  // 是否启用强制清理数据，默认为true

    private long flushFileInterval = 10;  // 刷新文件的间隔时间，默认为10ms

    private long checkPointInterval = 3000;  // 检查点的间隔时间，默认为3000ms

    private int mappedFileSizeForEntryData = 1024 * 1024 * 1024;  // 数据条目数据的映射文件大小，默认为1GB

    private int mappedFileSizeForEntryIndex = DLedgerMmapFileStore.INDEX_UNIT_SIZE * 5 * 1024 * 1024;  // 数据条目索引的映射文件大小，默认为DLedgerMmapFileStore.INDEX_UNIT_SIZE * 5 * 1024 * 1024

    private boolean enablePushToFollower = true;  // 是否启用将数据推送到follower节点，默认为true

    private String preferredLeaderIds;  // 偏好的leader节点ID

    private long maxLeadershipTransferWaitIndex = 1000;  // 最大领导权转移等待索引，默认为1000

    private int minTakeLeadershipVoteIntervalMs = 30;  // 最小获取领导权的选举间隔时间，默认为30ms

    private int maxTakeLeadershipVoteIntervalMs = 100;  // 最大获取领导权的选举间隔时间，默认为100ms

    private boolean isEnableBatchAppend = false;  // 是否启用批量追加数据，默认为false

    // max size in bytes for each append request
    private int maxBatchAppendSize = 4 * 1024;  // 每个追加请求的最大大小，默认为4KB

    private long leadershipTransferWaitTimeout = 1000;  // 领导权转移等待超时时间，默认为1000ms

    private boolean enableSnapshot = false;  // 是否启用快照，默认为false

    private SnapshotEntryResetStrategy snapshotEntryResetStrategy = SnapshotEntryResetStrategy.RESET_ALL_SYNC;  // 快照条目重置策略，默认为RESET_ALL_SYNC

    private int snapshotThreshold = 1000;  // 快照阈值，默认为1000

    private int resetSnapshotEntriesDelayTime = 5 * 1000;  // 重置快照条目延迟时间，默认为5000ms

    /**
     * 重置快照条目但保留最后的条目数。
     * 例如，当从属于快照的最后包含索引等于100时，我们将删除（..., 90]
     */
    private int resetSnapshotEntriesButKeepLastEntriesNum = 10;  // 保留的最后条目数，默认为10

    private int maxSnapshotReservedNum = 3;  // 保留的最大快照数，默认为3

    // max interval in ms for each append request
    private int maxBatchAppendIntervalMs = 1000;  // 每个追加请求的最大间隔时间，默认为1000ms

    /**
     * 当节点从候选人角色转变为领导者角色时，可能一直保持旧的提交索引，即使此索引的条目已经复制到超过一半的节点（
     * 它将保持到当前任期中的新条目被追加）。
     * 这种情况发生的原因是领导者无法提交前一个任期中的条目。
     */
    private boolean enableFastAdvanceCommitIndex = false;  // 是否启用快速推进提交索引，默认为false

    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;  // 指标导出类型，默认为DISABLE

    private String metricsGrpcExporterTarget = "";  // 指标gRPC导出目标

    private String metricsGrpcExporterHeader = "";  // 指标gRPC导出头部信息

    private long metricGrpcExporterTimeOutInMills = 3 * 1000;  // 指标gRPC导出超时时间，默认为3000ms

    private long metricGrpcExporterIntervalInMills = 60 * 1000;  // 指标gRPC导出间隔时间，默认为60000ms

    private long metricLoggingExporterIntervalInMills = 10 * 1000;  // 指标日志导出间隔时间，默认为10000ms

    private int metricsPromExporterPort = 5557;  // 指标Prometheus导出端口号，默认为5557

    private String metricsPromExporterHost = "";  // 指标Prometheus导出主机地址，默认为空

    public String getDefaultPath() {
        return storeBaseDir + File.separator + "dledger-" + selfId;
    }

    public String getDataStorePath() {
        if (dataStorePath == null) {
            return getDefaultPath() + File.separator + "data";
        }
        return dataStorePath;
    }

    public void setDataStorePath(String dataStorePath) {
        this.dataStorePath = dataStorePath;
    }

    public String getSnapshotStoreBaseDir() {
        return getDefaultPath() + File.separator + "snapshot";
    }

    public String getIndexStorePath() {
        return getDefaultPath() + File.separator + "index";
    }

    public int getMappedFileSizeForEntryData() {
        return mappedFileSizeForEntryData;
    }

    public void setMappedFileSizeForEntryData(int mappedFileSizeForEntryData) {
        this.mappedFileSizeForEntryData = mappedFileSizeForEntryData;
    }

    public int getMappedFileSizeForEntryIndex() {
        return mappedFileSizeForEntryIndex;
    }

    public void setMappedFileSizeForEntryIndex(int mappedFileSizeForEntryIndex) {
        this.mappedFileSizeForEntryIndex = mappedFileSizeForEntryIndex;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public String getStoreBaseDir() {
        return storeBaseDir;
    }

    public void setStoreBaseDir(String storeBaseDir) {
        this.storeBaseDir = storeBaseDir;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public boolean isEnableLeaderElector() {
        return enableLeaderElector;
    }

    public void setEnableLeaderElector(boolean enableLeaderElector) {
        this.enableLeaderElector = enableLeaderElector;
    }

    //for builder semantic
    public DLedgerConfig group(String group) {
        this.group = group;
        return this;
    }

    public DLedgerConfig selfId(String selfId) {
        this.selfId = selfId;
        return this;
    }

    public DLedgerConfig peers(String peers) {
        this.peers = peers;
        return this;
    }

    public DLedgerConfig storeBaseDir(String dir) {
        this.storeBaseDir = dir;
        return this;
    }

    public boolean isEnablePushToFollower() {
        return enablePushToFollower;
    }

    public void setEnablePushToFollower(boolean enablePushToFollower) {
        this.enablePushToFollower = enablePushToFollower;
    }

    public int getMaxPendingRequestsNum() {
        return maxPendingRequestsNum;
    }

    public void setMaxPendingRequestsNum(int maxPendingRequestsNum) {
        this.maxPendingRequestsNum = maxPendingRequestsNum;
    }

    public int getMaxWaitAckTimeMs() {
        return maxWaitAckTimeMs;
    }

    public void setMaxWaitAckTimeMs(int maxWaitAckTimeMs) {
        this.maxWaitAckTimeMs = maxWaitAckTimeMs;
    }

    public int getMaxPushTimeOutMs() {
        return maxPushTimeOutMs;
    }

    public void setMaxPushTimeOutMs(int maxPushTimeOutMs) {
        this.maxPushTimeOutMs = maxPushTimeOutMs;
    }

    public int getHeartBeatTimeIntervalMs() {
        return heartBeatTimeIntervalMs;
    }

    public void setHeartBeatTimeIntervalMs(int heartBeatTimeIntervalMs) {
        this.heartBeatTimeIntervalMs = heartBeatTimeIntervalMs;
    }

    public int getMinVoteIntervalMs() {
        return minVoteIntervalMs;
    }

    public void setMinVoteIntervalMs(int minVoteIntervalMs) {
        this.minVoteIntervalMs = minVoteIntervalMs;
    }

    public int getMaxVoteIntervalMs() {
        return maxVoteIntervalMs;
    }

    public void setMaxVoteIntervalMs(int maxVoteIntervalMs) {
        this.maxVoteIntervalMs = maxVoteIntervalMs;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public float getDiskSpaceRatioToCheckExpired() {
        return diskSpaceRatioToCheckExpired;
    }

    public void setDiskSpaceRatioToCheckExpired(float diskSpaceRatioToCheckExpired) {
        this.diskSpaceRatioToCheckExpired = diskSpaceRatioToCheckExpired;
    }

    public float getDiskSpaceRatioToForceClean() {
        if (diskSpaceRatioToForceClean < 0.50f) {
            return 0.50f;
        } else {
            return diskSpaceRatioToForceClean;
        }
    }

    public void setDiskSpaceRatioToForceClean(float diskSpaceRatioToForceClean) {
        this.diskSpaceRatioToForceClean = diskSpaceRatioToForceClean;
    }

    public float getDiskFullRatio() {
        float ratio = diskSpaceRatioToForceClean + 0.05f;
        if (ratio > 0.95f) {
            return 0.95f;
        }
        return ratio;
    }

    public int getFileReservedHours() {
        return fileReservedHours;
    }

    public void setFileReservedHours(int fileReservedHours) {
        this.fileReservedHours = fileReservedHours;
    }

    public long getFlushFileInterval() {
        return flushFileInterval;
    }

    public void setFlushFileInterval(long flushFileInterval) {
        this.flushFileInterval = flushFileInterval;
    }

    public boolean isEnableDiskForceClean() {
        return enableDiskForceClean;
    }

    public void setEnableDiskForceClean(boolean enableDiskForceClean) {
        this.enableDiskForceClean = enableDiskForceClean;
    }

    public int getMaxHeartBeatLeak() {
        return maxHeartBeatLeak;
    }

    public void setMaxHeartBeatLeak(int maxHeartBeatLeak) {
        this.maxHeartBeatLeak = maxHeartBeatLeak;
    }

    public int getPeerPushThrottlePoint() {
        return peerPushThrottlePoint;
    }

    public void setPeerPushThrottlePoint(int peerPushThrottlePoint) {
        this.peerPushThrottlePoint = peerPushThrottlePoint;
    }

    public int getPeerPushQuota() {
        return peerPushQuota;
    }

    public void setPeerPushQuota(int peerPushQuota) {
        this.peerPushQuota = peerPushQuota;
    }

    public long getCheckPointInterval() {
        return checkPointInterval;
    }

    public void setCheckPointInterval(long checkPointInterval) {
        this.checkPointInterval = checkPointInterval;
    }

    @Deprecated
    public String getPreferredLeaderId() {
        return preferredLeaderIds;
    }

    @Deprecated
    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderIds = preferredLeaderId;
    }

    public String getPreferredLeaderIds() {
        return preferredLeaderIds;
    }

    public void setPreferredLeaderIds(String preferredLeaderIds) {
        this.preferredLeaderIds = preferredLeaderIds;
    }

    public long getMaxLeadershipTransferWaitIndex() {
        return maxLeadershipTransferWaitIndex;
    }

    public void setMaxLeadershipTransferWaitIndex(long maxLeadershipTransferWaitIndex) {
        this.maxLeadershipTransferWaitIndex = maxLeadershipTransferWaitIndex;
    }

    public int getMinTakeLeadershipVoteIntervalMs() {
        return minTakeLeadershipVoteIntervalMs;
    }

    public void setMinTakeLeadershipVoteIntervalMs(int minTakeLeadershipVoteIntervalMs) {
        this.minTakeLeadershipVoteIntervalMs = minTakeLeadershipVoteIntervalMs;
    }

    public int getMaxTakeLeadershipVoteIntervalMs() {
        return maxTakeLeadershipVoteIntervalMs;
    }

    public void setMaxTakeLeadershipVoteIntervalMs(int maxTakeLeadershipVoteIntervalMs) {
        this.maxTakeLeadershipVoteIntervalMs = maxTakeLeadershipVoteIntervalMs;
    }

    public boolean isEnableBatchAppend() {
        return isEnableBatchAppend;
    }

    public void setEnableBatchAppend(boolean enableBatchAppend) {
        isEnableBatchAppend = enableBatchAppend;
    }

    public int getMaxBatchAppendSize() {
        return maxBatchAppendSize;
    }

    public void setMaxBatchAppendSize(int maxBatchAppendSize) {
        this.maxBatchAppendSize = maxBatchAppendSize;
    }

    public long getLeadershipTransferWaitTimeout() {
        return leadershipTransferWaitTimeout;
    }

    public void setLeadershipTransferWaitTimeout(long leadershipTransferWaitTimeout) {
        this.leadershipTransferWaitTimeout = leadershipTransferWaitTimeout;
    }

    public String getReadOnlyDataStoreDirs() {
        return readOnlyDataStoreDirs;
    }

    public void setReadOnlyDataStoreDirs(String readOnlyDataStoreDirs) {
        this.readOnlyDataStoreDirs = readOnlyDataStoreDirs;
    }

    private String selfAddress;

    // groupId#selfIf -> address
    private Map<String, String> peerAddressMap;

    private final AtomicBoolean inited = new AtomicBoolean(false);

    public void init() {
        if (inited.compareAndSet(false, true)) {
            initSelfAddress();
            initPeerAddressMap();
        }
    }

    private void initSelfAddress() {
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            if (this.selfId.equals(peerSelfId)) {
                this.selfAddress = peerAddress;
                return;
            }
        }
        // can't find itself
        throw new IllegalArgumentException("[DLedgerConfig] fail to init self address, config: " + this);
    }

    private void initPeerAddressMap() {
        Map<String, String> peerMap = new HashMap<>();
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            peerMap.put(DLedgerUtils.generateDLedgerId(this.group, peerSelfId), peerAddress);
        }
        this.peerAddressMap = peerMap;
    }

    public String getSelfAddress() {
        return this.selfAddress;
    }


    public Map<String, String> getPeerAddressMap() {
        return this.peerAddressMap;
    }

    public int getSnapshotThreshold() {
        return snapshotThreshold;
    }

    public void setSnapshotThreshold(int snapshotThreshold) {
        this.snapshotThreshold = snapshotThreshold;
    }

    public int getMaxSnapshotReservedNum() {
        return maxSnapshotReservedNum;
    }

    public void setMaxSnapshotReservedNum(int maxSnapshotReservedNum) {
        this.maxSnapshotReservedNum = maxSnapshotReservedNum;
    }

    public boolean isEnableSnapshot() {
        return enableSnapshot;
    }

    public void setEnableSnapshot(boolean enableSnapshot) {
        this.enableSnapshot = enableSnapshot;
    }

    public int getMaxBatchAppendIntervalMs() {
        return maxBatchAppendIntervalMs;
    }

    public SnapshotEntryResetStrategy getSnapshotEntryResetStrategy() {
        return snapshotEntryResetStrategy;
    }

    public void setSnapshotEntryResetStrategy(
        SnapshotEntryResetStrategy snapshotEntryResetStrategy) {
        this.snapshotEntryResetStrategy = snapshotEntryResetStrategy;
    }

    public void setMaxBatchAppendIntervalMs(int maxBatchAppendIntervalMs) {
        this.maxBatchAppendIntervalMs = maxBatchAppendIntervalMs;
    }

    public int getResetSnapshotEntriesDelayTime() {
        return resetSnapshotEntriesDelayTime;
    }

    public void setResetSnapshotEntriesDelayTime(int resetSnapshotEntriesDelayTime) {
        this.resetSnapshotEntriesDelayTime = resetSnapshotEntriesDelayTime;
    }

    public int getResetSnapshotEntriesButKeepLastEntriesNum() {
        return resetSnapshotEntriesButKeepLastEntriesNum;
    }

    public void setResetSnapshotEntriesButKeepLastEntriesNum(int resetSnapshotEntriesButKeepLastEntriesNum) {
        this.resetSnapshotEntriesButKeepLastEntriesNum = resetSnapshotEntriesButKeepLastEntriesNum;
    }

    public boolean isEnableFastAdvanceCommitIndex() {
        return enableFastAdvanceCommitIndex;
    }

    public void setEnableFastAdvanceCommitIndex(boolean enableFastAdvanceCommitIndex) {
        this.enableFastAdvanceCommitIndex = enableFastAdvanceCommitIndex;
    }

    public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
        this.metricsExporterType = metricsExporterType;
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
        this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
    }

    public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
        this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
    }

    public void setMetricGrpcExporterTimeOutInMills(long metricGrpcExporterTimeOutInMills) {
        this.metricGrpcExporterTimeOutInMills = metricGrpcExporterTimeOutInMills;
    }

    public void setMetricGrpcExporterIntervalInMills(long metricGrpcExporterIntervalInMills) {
        this.metricGrpcExporterIntervalInMills = metricGrpcExporterIntervalInMills;
    }

    public void setMetricLoggingExporterIntervalInMills(long metricLoggingExporterIntervalInMills) {
        this.metricLoggingExporterIntervalInMills = metricLoggingExporterIntervalInMills;
    }

    public void setMetricsPromExporterPort(int metricsPromExporterPort) {
        this.metricsPromExporterPort = metricsPromExporterPort;
    }

    public void setMetricsPromExporterHost(String metricsPromExporterHost) {
        this.metricsPromExporterHost = metricsPromExporterHost;
    }

    public String getMetricsGrpcExporterTarget() {
        return metricsGrpcExporterTarget;
    }

    public String getMetricsGrpcExporterHeader() {
        return metricsGrpcExporterHeader;
    }

    public long getMetricGrpcExporterTimeOutInMills() {
        return metricGrpcExporterTimeOutInMills;
    }

    public long getMetricGrpcExporterIntervalInMills() {
        return metricGrpcExporterIntervalInMills;
    }

    public long getMetricLoggingExporterIntervalInMills() {
        return metricLoggingExporterIntervalInMills;
    }

    public int getMetricsPromExporterPort() {
        return metricsPromExporterPort;
    }

    public String getMetricsPromExporterHost() {
        return metricsPromExporterHost;
    }
}
