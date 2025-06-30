package org.example.zookeeperCoordination;

import java.util.List;

public class ChildrenCache {

    List<String> children;

    public ChildrenCache() {
        this.children = null;
    }

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public List<String> removeAndSet(List<String> children) {
        return null;
    }

    public List<String> addedAndSet(List<String> children) {
        return null;
    }

    List<String> getList(){
        return null;
    }
}
