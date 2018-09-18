package com.zhengxuetao;

import kafka.javaapi.PartitionMetadata;

public class PartitionVO {
    private PartitionMetadata partitionMetadata;
    private long fromOffset;

    public PartitionVO() {

    }

    public PartitionVO(PartitionMetadata partitionMetadata, long fromOffset) {
        this.partitionMetadata = partitionMetadata;
        this.fromOffset = fromOffset;
    }

    public PartitionMetadata getPartitionMetadata() {
        return partitionMetadata;
    }

    public void setPartitionMetadata(PartitionMetadata partitionMetadata) {
        this.partitionMetadata = partitionMetadata;
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public void setFromOffset(long fromOffset) {
        this.fromOffset = fromOffset;
    }


}
