package org.example.zookeeperCoordination;

public class RecreateTaskContext {
    String path;
    String task;
    byte[] data;

    public RecreateTaskContext(String path, String task, byte[] data) {
        this.path = path;
        this.task = task;
        this.data = data;
    }

    public String getPath() {
        return path;
    }

    public String getTask() {
        return task;
    }

    public byte[] getData() {
        return data;
    }
}
