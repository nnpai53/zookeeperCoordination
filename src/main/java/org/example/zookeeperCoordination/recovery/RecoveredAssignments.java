package org.example.zookeeperCoordination.recovery;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.example.zookeeperCoordination.RecreateTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


public class RecoveredAssignments {

    private static Logger LOG = LoggerFactory.getLogger(RecoveredAssignments.class);


    List<String> tasks;
    List<String> assignments;
    List<String> status;
    List<String> activeWorkers;
    List<String> assignedWorkers;
    RecoveryCallback cb;
    ZooKeeper zk;

    public RecoveredAssignments(ZooKeeper zk) {
        this.zk = zk;
        this.assignments = new ArrayList<>();
    }

    public void recover(RecoveryCallback recoveryCallback) {
        cb = recoveryCallback;
        getTasks();
    }

    private void getTasks(){
        zk.getChildren("/tasks", false, tasksCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    getTasks();
                    break;

                case OK:
                    LOG.info("Getting tasks for recovery");
                    tasks = list;
                    getAssignedWorkers();
                    break;

                default:
                    LOG.info("getChildren failed", KeeperException.create(KeeperException.Code.get(i),s));
                    cb.recoveryComplete(RecoveryCallback.FAILED,null);

            }
        }
    };

    private void getAssignedWorkers(){
        zk.getChildren("/assign", false,assignedWorkersCallback, null);
    }

    AsyncCallback.ChildrenCallback assignedWorkersCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    getAssignedWorkers();
                    break;

                case OK:
                    assignedWorkers = list;
                    getWorkers(list);
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i),s));
                    cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };

    private void getWorkers(Object ctx) {
        zk.getChildren("/workers", false, workersCallback, ctx);
    }

    AsyncCallback.ChildrenCallback workersCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    getWorkers(o);
                    break;

                case OK:
                    LOG.info("Getting worker assignments for recovery: " + list.size());
                    if(list.isEmpty()) {
                        LOG.warn("Empty list of workers, possibly just starting");
                        cb.recoveryComplete(RecoveryCallback.OK, new ArrayList<>());
                        break;
                    }

                    activeWorkers = list;
                    for(String worker: assignedWorkers) {
                        getWorkerAssignments("/assign/" + worker);
                    }
                    break;

                default:
                    LOG.error("getChildren failed ", KeeperException.create(KeeperException.Code.get(i),s));
                    cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };

    private void  getWorkerAssignments(String s){
        zk.getChildren(s, false, workerAssignmentsCallback, null);
    }

    AsyncCallback.ChildrenCallback workerAssignmentsCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    getWorkerAssignments(s);
                    break;

                case OK:
                    String worker = s.replace("/assign/", "");
                    if (activeWorkers.contains(worker)) {
                        assignments.addAll(list);
                    } else {
                        for (String task : list) {
                            if (!tasks.contains(task)) {
                                tasks.add(task);
                                getDataReassign(s, task);
                            } else {
                                deleteAssignment(s + "/" + task);
                            }
                            deleteAssignment(s);
                        }
                    }
                    assignedWorkers.remove(worker);
                    if(assignedWorkers.isEmpty()){
                        LOG.info("Getting statuses for recovery");
                        getStatuses();
                    }
                    break;

                case NONODE:
                    LOG.info("No such znode exists: " + s);
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i),s));
                    cb.recoveryComplete(RecoveryCallback.FAILED, null);

            }
        }
    };

    void getDataReassign(String path, String task) {
        zk.getData(path,false,getDataReassignCallback, task);
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
        zk.create("/tasks/" + recreateTaskContext.getTask(), recreateTaskContext.getData(),OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, recreateTaskContext);
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
                    LOG.warn("Node shouldn't exist: " + s);
                    break;

                default:
                    LOG.error("Something went wrong when recreating task",KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void deleteAssignment(String path){
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
                    LOG.info("Task correctly deleted: " +s);
                    break;

                default:
                    LOG.error("Failed to delete task data " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    void getStatuses(){
        zk.getChildren("/status", false, statusCallback, null);
    }

    AsyncCallback.ChildrenCallback statusCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int i, String s, Object o, List<String> list) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    getStatuses();
                    break;

                case OK:
                    LOG.info("Processing assignments for recovery");
                    status = list;
                    processAssignments();
                    break;

                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(i), s));
                    cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };

    private void processAssignments(){
        LOG.info("Size of tasks: " + tasks.size());
        for(String s:  assignments) {
            LOG.info("Assignment: " + s);
            deleteAssignment("/tasks/" + s);
            tasks.remove(s);
        }
        LOG.info("Size of tasks after assignment filtering: " + tasks.size());
        for(String s: status){
            LOG.info("Checking task: {}", s);
            deleteAssignment("/tasks/" + s);
            tasks.remove(s);
        }
        LOG.info("Size of tasks after status filtering: " + tasks.size());
        cb.recoveryComplete(RecoveryCallback.OK, tasks);
    }

}
