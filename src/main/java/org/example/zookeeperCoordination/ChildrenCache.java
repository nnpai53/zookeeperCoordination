package org.example.zookeeperCoordination;

import java.util.ArrayList;
import java.util.List;

public class ChildrenCache {

    List<String> children;

    public ChildrenCache() {
        this.children = null;
    }

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public List<String> removeAndSet(List<String> newChildren) {
        List<String> diff = null;
        if(children != null){
            for(String s: children) {
                if(! newChildren.contains(s)){
                    if(diff == null) {
                        diff = new ArrayList<>();
                    }
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        return diff;
    }

    public List<String> addedAndSet(List<String> newChildren) {
        List<String> diff = null;
        if(children == null) {
            diff = new ArrayList<>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains(s)) {
                    if(diff == null) {
                        diff = new ArrayList<>();
                    }
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        return diff;
    }

    List<String> getList(){
        return children;
    }
}
