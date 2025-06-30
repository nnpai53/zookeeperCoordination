package org.example.zookeeperCoordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher {

    private static Logger LOG = LoggerFactory.getLogger(Worker.class);

    ZooKeeper zk;
    String hostPort;
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    boolean connected = false;
    boolean expired = false;
    String status;
    private int executionCount;
    ThreadPoolExecutor threadPoolExecutor;

    protected ChildrenCache assignedTasks = new ChildrenCache();

    public Worker(String hostPort) {
        this.hostPort = hostPort;
        threadPoolExecutor = new ThreadPoolExecutor(1,1,1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(200));
    }

    void starZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public boolean isExpired() {
        return expired;
    }

    public boolean isConnected() {
        return connected;
    }

    public void bootStrap() {
        createAssignNode();
    }

    void createAssignNode() {
        zk.create("/assign/worker-" +serverId, new byte[0], OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createAssignCallback, null);
    }

    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    createAssignNode();
                    break;

                case OK:
                    LOG.info("Assign node created");
                    break;

                case NODEEXISTS:
                    LOG.warn("Assign node already registered");
                    break;

                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void register(){
        zk.create("/workers/worker-" + serverId, "Idle".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback,null);
    }

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    updateStatus((String) o);
                    return;
            }
        }
    };

    synchronized private void updateStatus(String status){
        if(status == this.status) {
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    synchronized void changeExecutionCount(int countChange){
        executionCount += countChange;
        if(executionCount == 0 && countChange < 0){
            setStatus("Idle");
        }
        if(executionCount == 1 && countChange > 0) {
            setStatus("Working");
        }
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    register();
                    break;

                case OK:
                    LOG.info("Registered successfully: " + serverId);
                    break;

                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);
                    break;

                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void getTasks(){
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    Watcher newTaskWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                assert new String("/assign/worker-" + serverId).equals(watchedEvent.getPath());
                getTasks();
            }
        }
    };

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    getTasks();
                    break;

                case OK:
                    if(list != null){
                        threadPoolExecutor.execute(new Runnable() {
                            List<String> list;
                            DataCallback cb;

                            public Runnable init(List<String> list,
                                                 DataCallback cb) {
                                this.list = list;
                                this.cb = cb;
                                return this;
                            }
                            @Override
                            public void run() {
                                if(list == null)
                                    return;
                                LOG.info("Looping into tasks");
                                setStatus("Working");
                                for(String task: list) {
                                    LOG.trace("New task: {}", task);
                                    zk.getData("/assign/worker-" + serverId + "/" + task, false,cb, task);
                                }
                            }
                        }.init(assignedTasks.addedAndSet(list), taskDataCallback));
                    }
                    break;

                default:
                    System.out.println("getChildren failed: " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    zk.getData(s, false, taskDataCallback, null);
                    break;

                case OK:
                    threadPoolExecutor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;

                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;
                            return this;
                        }

                        @Override
                        public void run() {
                            LOG.info("Executing your task :" + new String(data));
                            zk.create("/status/" + (String) ctx, "done".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,taskStatusCreateCallback,null);
                            zk.delete("/assign/worker-" + serverId + "/" + (String) ctx, -1, taskVoidCallback, null);
                        }
                    });
                    break;

                default:
                    LOG.error("Failed to get task data: ", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    zk.create(s1 + "/status", "done".getBytes(), OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,taskStatusCreateCallback,null);
                    break;

                case OK:
                    LOG.info("Created status znode correctly: " + s1);
                    break;

                case NODEEXISTS:
                    LOG.warn("Node exists: " + s1);
                    break;

                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    AsyncCallback.VoidCallback taskVoidCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int i, String s, Object o) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    break;

                case OK:
                    LOG.info("Task correctly deleted: " + s);
                    break;

                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };



    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString() + "," + hostPort);
        if(watchedEvent.getType() == Event.EventType.None) {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    connected = true;
                    break;

                case Disconnected:
                    connected = false;
                    break;

                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expired");

                default:
                    break;
            }
        }
    }

    public void close(){
        LOG.info("Closing");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("Zookeeper interrupted while closing");
        }
    }

    public static void main(String[] args) throws Exception{
        Worker w = new Worker(args[0]);
        w.starZK();
        while(!w.isConnected()) {
            Thread.sleep(100);
        }
        w.bootStrap();
        w.register();
        w.getTasks();
        while (!w.isExpired()) {
            Thread.sleep(1000);
        }
    }
}
