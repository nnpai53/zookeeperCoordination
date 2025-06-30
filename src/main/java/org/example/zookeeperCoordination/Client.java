package org.example.zookeeperCoordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Client implements Watcher, Closeable {

    private static Logger LOG = LoggerFactory.getLogger(Client.class);

    ZooKeeper zk;
    String hostPort;
    ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<>();
    volatile boolean connected = false;
    volatile boolean expired = false;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isExpired() {
        return expired;
    }

    void submitTask(String task, TaskObject taskctx) {
        taskctx.setTask(task);
        zk.create("/tasks/task-", task.getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, taskctx);
    }

    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    submitTask(((TaskObject) o).getTask(), (TaskObject) o);
                    break;

                case OK:
                    LOG.info("My created task name: " + s1);
                    ((TaskObject) o).setTaskName(s1);
                    watchStatus("/status/" + s1.replace("/tasks/", ""),o);
                    break;

                default:
                    LOG.error("Something went wrong" + KeeperException.create(KeeperException.Code.get(i),s));

            }
        }
    };

    void watchStatus(String path, Object ctx){
        ctxMap.put(path, ctx);
        zk.exists(path, statusWatcher, existsCallback, ctx);
    }

    Watcher statusWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeCreated){
                assert watchedEvent.getPath().contains("/status/task-");
                zk.getData(watchedEvent.getPath(), false, getDataCallback,ctxMap.get(watchedEvent.getPath()));
            }
        }
    };

    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    watchStatus(s, o);
                    break;

                case OK:
                    if(stat != null) {
                        zk.getData(s, false, getDataCallback, null);
                    }
                    break;

                case NONODE:
                    break;

                default:
                    LOG.error("Something went wrong when checking if the status node exists: " + KeeperException.create(KeeperException.Code.get(i),s));
                    break;
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    zk.getData(s, false, getDataCallback, ctxMap.get(s));
                    return;

                case OK:
                    String taskResult = new String(bytes);
                    LOG.info("Task " + s + ", " + taskResult);
                    assert (o != null);
                    ((TaskObject)o).setStatus(taskResult.contains("done"));
                    zk.delete(s, -1, taskDeleteCallback, null);
                    ctxMap.remove(s);
                    break;

                case NONODE:
                    LOG.warn("Status node is gone");
                    return;

                default:
                    LOG.error("Something went wrong here, " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int i, String s, Object o) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    zk.delete(s, -1, taskDeleteCallback, null);
                    break;

                case OK:
                    LOG.info("Successfully deleted " + s);
                    break;

                default:
                    LOG.error("Something went wrong here, " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
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
                    System.out.println("Exiting due to session expiration");

                default:
                    break;
            }
        }
    }

    static class TaskObject {
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean successful = false;
        private CountDownLatch latch = new CountDownLatch(1);

        public String getTask() {
            return task;
        }

        public void setTask(String task) {
            this.task = task;
        }

        public String getTaskName() {
            return taskName;
        }

        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }

        void setStatus(boolean status){
            successful = status;
            done = true;
            latch.countDown();
        }

        void waitUntilDone(){
            try {
                latch.await();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted Exception while waiting for task to get done");
            }
        }

        synchronized boolean isDone() {
            return done;
        }

        synchronized boolean isSuccessful() {
            return successful;
        }

    }

    @Override
    public void close(){
        LOG.info("Closing");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("Zookeeper interrupted while closing");
        }
    }

    public static void main(String args[]) throws Exception{
        Client client = new Client(args[0]);
        client.startZK();

        while (!client.isConnected()) {
            Thread.sleep(100);
        }

        TaskObject taskObject1 = new TaskObject();
        TaskObject taskObject2 = new TaskObject();

        client.submitTask("Sample Task", taskObject1);
        client.submitTask("Another sample task", taskObject2);

        taskObject1.waitUntilDone();
        taskObject2.waitUntilDone();

    }
}
