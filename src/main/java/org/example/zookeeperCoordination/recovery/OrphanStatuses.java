package org.example.zookeeperCoordination.recovery;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OrphanStatuses {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanStatuses.class);

    private List<String> tasks;
    private List<String> statuses;
    private ZooKeeper zk;

    public OrphanStatuses(ZooKeeper zk) {
        this.zk = zk;
    }

    private void cleanUp(){
        getTasks();
    }

    private void getTasks(){
        zk.getChildren("/tasks", false,tasksCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    getTasks();
                    break;

                case OK:
                    tasks = list;
                    getStatuses();
                    break;

                default:
                    LOG.error("getChildren failed " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void getStatuses(){
        zk.getChildren("/status", false, statusCallback, null);
    }

    AsyncCallback.ChildrenCallback statusCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;

                case OK:
                    statuses = list;
                    processTasks();
                    break;

                default:
                    LOG.error("getChildren failed " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void processTasks(){
        for(String s: tasks) {
            statuses.remove("status-"+s);
        }

        for(String s: statuses) {
            zk.delete("/status/" + s, -1, deleteStatusCallback, null);
        }
    }

    AsyncCallback.VoidCallback deleteStatusCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int i, String s, Object o) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    zk.delete(s, -1, deleteStatusCallback, null);
                    break;

                case OK:
                    LOG.info("Successfully deleted orphan status znode: " + s);
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost: " + args[0], 10000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("Received event: " + watchedEvent.getType());
            }
        });

        (new OrphanStatuses(zk)).cleanUp();
    }





}
