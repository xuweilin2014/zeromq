package com.xu.zeromq.consumer;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class ClustersRelation {

    private String clusterId;

    private ConsumerCluster cluster;

    ClustersRelation() {
    }

    ClustersRelation(String clusterId, ConsumerCluster cluster) {
        this.cluster = cluster;
        this.clusterId = clusterId;
    }

    public ConsumerCluster getCluster() {
        return cluster;
    }

    public void setCluster(ConsumerCluster cluster) {
        this.cluster = cluster;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj != null && ClustersRelation.class.isAssignableFrom(obj.getClass())) {
            ClustersRelation clusters = (ClustersRelation) obj;
            result = new EqualsBuilder().append(clusterId, clusters.getClusterId()).isEquals();
        }
        return result;
    }
}
