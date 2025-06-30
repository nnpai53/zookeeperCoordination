package org.example.zookeeperCoordination.recovery;

import org.apache.zookeeper.ZooKeeper;

public class RecoveredAssignments {

    ZooKeeper zk;

    public RecoveredAssignments(ZooKeeper zk) {
        this.zk = zk;
    }

    public void recover(RecoveryCallback recoveryCallback) {
        Map<String, List> testMap = new HashMap<>();
        testMap.putIfAbsent();
    }

}
