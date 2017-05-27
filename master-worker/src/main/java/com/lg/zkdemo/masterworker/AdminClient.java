package com.lg.zkdemo.masterworker;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by liuguo on 2017/5/25.
 */
public class AdminClient implements Watcher {

    String hostPort;

    ZooKeeper zk;

    AdminClient(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void listState() throws KeeperException, InterruptedException {
        System.out.println("********Master*************");
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master",false,stat);

            Date date = new Date(stat.getCtime());

            System.out.println("Master:"+new String(masterData)+" since "+date);

        } catch (NoNodeException e) {
            System.out.println("No Master");
        }

        System.out.println("************Workers**********");
        List<String> children = zk.getChildren("/workers", false);
        System.out.println("children:"+ Arrays.toString(children.toArray()));
        for(String w : children){
            byte[] data = zk.getData("/workers/"+w,false,null);
            System.out.println("\t" + w + ": " + new String(data));
        }

        System.out.println("***********Tasks*************");
        for (String t: zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 1){
            System.err.println("USAGE: hostPort");
            System.exit(2);
        }

        AdminClient adminClient = new AdminClient(args[0]);
        adminClient.startZK();

        adminClient.listState();
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println("event = [" + event + "]");
    }
}
