package org.example.zookeeperCoordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.example.zookeeperCoordination.recovery.RecoveredAssignments;
import org.example.zookeeperCoordination.recovery.RecoveryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher{

    private static Logger LOG = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    volatile MasterStates state = MasterStates.RUNNING;
    volatile boolean connected = false;
    volatile boolean expired = false;
    ChildrenCache tasksCache;
    ChildrenCache workersCache;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    public MasterStates getState() {
        return state;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    void checkMaster() throws InterruptedException, KeeperException {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void runForMaster() throws InterruptedException, KeeperException {
        zk.create("/master",serverId.getBytes(),OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,masterCreateCallback, null);
    }

    void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    try {
                        checkMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    break;

                case NONODE:
                    try {
                        runForMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    break;

                case OK:
                    if(serverId.equals(new String(bytes))) {
                        state = MasterStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExists();
                    }

                    break;

                default:
                    LOG.error("Error while reading data.", KeeperException.create(KeeperException.Code.get(i),s));

            }
        }
    };

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    try {
                        checkMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    break;

                case OK:
                    state = MasterStates.ELECTED;
                    takeLeadership();
                    break;

                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;

                default:
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master.", KeeperException.create(KeeperException.Code.get(i),s));

            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    masterExists();
                    break;

                case OK:
                    if(stat == null) {
                        state = MasterStates.RUNNING;
                        try {
                            runForMaster();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (KeeperException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;

                default:
                    try {
                        checkMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    break;
            }
        }
    };

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals(watchedEvent.getPath());
                try {
                    runForMaster();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();

        (new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
            public void recoveryComplete(int i, List<String> list) {
                if(i == RecoveryCallback.FAILED){
                    LOG.error("Recovery of assigned tasks failed");
                } else {
                    LOG.info("Assigning recovered tasks");
                    getTasks();
                }
            }
        });
    }

    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                assert "/workers".equals(watchedEvent.getPath());
                getWorkers();
            }
        }
    };

    void getWorkers(){
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    getWorkers();
                    break;

                case OK:
                    LOG.info("Successfully got a list of workers: " + list.size() + " workers");
                    reassignAndSet(list);
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i), s));
            }
        }
    };

    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("Removing and Setting ");
            toProcess = workersCache.removeAndSet(children);
        }

        if(toProcess != null) {
            for(String worker: toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign" + worker, false, workerAssignmentCallback, null);
    }

    AsyncCallback.ChildrenCallback workerAssignmentCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS :
                    getAbsentWorkerTasks(s);
                    break;

                case OK:
                    LOG.info("Successfully got list of assignments: " + list.size() + " tasks");

                    for(String task: list) {
                        getDataReassign(s + "/" +task, task);
                    }
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void getDataReassign(String path, String task) {
        zk.getData(path, false, getDataReassignCallback, task);
    }

    AsyncCallback.DataCallback getDataReassignCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    getDataReassign(s, (String) o);
                    break;

                case OK:
                    recreateTask(new RecreateTaskContext(s, (String) o, bytes));
                    break;

                default:
                    LOG.error("Something went wrong when getting data ", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void recreateTask(RecreateTaskContext recreateTaskContext) {
        zk.create("/tasks/" + recreateTaskContext.getTask(), recreateTaskContext.getData(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, recreateTaskContext);
    }

    AsyncCallback.StringCallback recreateTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    recreateTask((RecreateTaskContext) o);
                    break;

                case OK:
                    deleteAssignment(((RecreateTaskContext) o).getPath());
                    break;

                case NODEEXISTS:
                    LOG.info("Node exists already, but if it hasn't been deleted then it will eventually, so we keep trying" + s);
                    recreateTask((RecreateTaskContext) o);
                    break;

                default:
                    LOG.error("Something went wrong while recreating task", KeeperException.create(KeeperException.Code.get(i)));
            }
        }
    };

    void deleteAssignment(String path) {
        zk.delete(path, -1, taskDeletionCallback, null);
    }

    AsyncCallback.VoidCallback taskDeletionCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int i, String s, Object o) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    deleteAssignment(s);
                    break;

                case OK:
                    LOG.info("Task correctly deleted: " + s);
                    break;

                default:
                    LOG.error("Failed to delete task data " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    Watcher tasksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                assert "/tasks".equals(watchedEvent.getPath());
                getTasks();
            }
        }
    };

    void getTasks() {
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;

                case OK:
                    List<String> toProcess;
                    if(tasksCache == null) {
                        tasksCache = new ChildrenCache(list);

                        toProcess = list;
                    } else {
                        toProcess = tasksCache.addedAndSet(list);
                    }
                    if(toProcess != null) {
                        assignTasks(toProcess);
                    }
                    break;

                default:
                    LOG.error("getChildren failed.", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for(String task: tasks) {
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    getTaskData((String) o);
                    break;

                case OK:
                    /*
                     * Choose a worker at Random
                     */
                    List<String> list = workersCache.getList();
                    int worker = random.nextInt(list.size());
                    String designatedWorker = list.get(worker);
                    /*
                     * Assign task to randomly chosen worker
                     */
                    String assignmentPath = "/assign" + designatedWorker + "/" + (String) o;
                    createAssignment(assignmentPath, bytes);
                    break;

                default:
                    LOG.error("Error when trying to get task data.", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void createAssignment(String path, byte[] data){
        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    AsyncCallback.StringCallback assignTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    createAssignment(s, (byte[]) o);
                    break;

                case OK:
                    LOG.info("Task assigned correctly: " + s1);
                    deleteTask(s1.substring(s1.lastIndexOf("/") + 1));
                    break;

                case NODEEXISTS:
                    LOG.warn("Task already assigned");
                    break;

                default:
                    LOG.error("Error when trying to assign task.", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void deleteTask(String name) {
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int i, String s, Object o) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    deleteTask(s);
                    break;

                case OK:
                    LOG.info("Successfully deleted " + s);
                    break;

                case NONODE:
                    LOG.info("Task has been deleted already");
                    break;

                default:
                    LOG.error("Something went wrong here, " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("Processing event: " + watchedEvent.toString());
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
                    LOG.error("Session expiration");
                    break;

                default:
                    break;
            }
        }
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    createParent(s, (byte[]) o);
                    break;

                case OK:
                    LOG.info("Parent created");
                    break;

                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + s);
                    break;

                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    public boolean isConnected() {
        return connected;
    }

    public boolean isExpired() {
        return expired;
    }

    public void close() {
        if(zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e){
                LOG.warn("Interrupted while closing Zookeeper session ");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();

        while(!m.isConnected()) {
            Thread.sleep(100);
        }

        m.bootstrap();

        m.runForMaster();

        while (!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZK();
    }
}
