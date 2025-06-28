package org.example.zookeeperCoordination;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class Worker implements Watcher {

    private static Logger LOG = LoggerFactory.getLogger(Worker.class);

    ZooKeeper zk;
    String hostPort;
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }



    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
