package com.xu.zeromq.consumer;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class ClustersState {

    public static final int ERROR = 1;
    public static final int SUCCESS = 0;
    public static final int NETWORKERR = -1;
    private String clusters;
    private int state;

    ClustersState() {
    }

    ClustersState(String clusters, int state) {
        this.clusters = clusters;
        this.state = state;
    }

    public String getClusters() {
        return clusters;
    }

    public void setClusters(String clusters) {
        this.clusters = clusters;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj != null && ClustersState.class.isAssignableFrom(obj.getClass())) {
            ClustersState clusters = (ClustersState) obj;
            result = new EqualsBuilder().append(clusters, clusters.getClusters()).isEquals();
        }
        return result;
    }
}
