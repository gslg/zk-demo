package com.lg.zkdemo.masterworker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by liuguo on 2017/5/24.
 */
public class Client implements Watcher{

    private String hostPort;

    private ZooKeeper zk;

    private String name;

    Client(String hostPort){
        this.hostPort = hostPort;
    }


    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    /**
     * 创建任务
     * @param command
     * @return
     * @throws Exception
     */
    String queueCommand(String command) throws Exception {

        while (true) {
            try {
                name =  zk.create("/tasks/task-",command.getBytes(),OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (NodeExistsException e) {
               throw new RuntimeException(name+" already appears to be running");
            } catch (ConnectionLossException e) {

            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("event = [" + event + "]");
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.err.println("USAGE: hostPort command");
            System.exit(2);
        }

        Client client = new Client(args[0]);

        client.startZK();

        String name =  client.queueCommand(args[1]);

        System.out.println("Created " + name);

    }
}
