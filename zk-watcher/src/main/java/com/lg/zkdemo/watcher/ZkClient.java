package com.lg.zkdemo.watcher;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooKeeper.States;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuguo on 2017/5/18.
 */
public class ZkClient implements Watcher{

    private ZooKeeper zooKeeper;

    private String hosts;

    private int times;

    private CountDownLatch countDownLatch;


    public ZkClient(String hosts,int times){
        this.hosts = hosts;
        this.times = times;
        //countDownLatch = new CountDownLatch(1);

        init();
    }

    /**
     * 防止KeeperErrorCode = MarshallingError
     * @param states
     * @param countDownLatch
     */
  /*  private void waitUntilConnected(States states, CountDownLatch countDownLatch){
        if(states != States.CONNECTED){
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }*/

    public boolean init(){
        boolean init = false;
        try {
            this.zooKeeper = new ZooKeeper(this.hosts,this.times,this);
           // waitUntilConnected(zooKeeper.getState(),countDownLatch);
            init = true;
        } catch (IOException e) {
            init = false;
            e.printStackTrace();
        }

        return init;
    }

    public void process(WatchedEvent event) {
        System.out.println("event = [" + event + "]");
        if(event.getState() == Event.KeeperState.SyncConnected){
            //countDownLatch.countDown();
        }
    }


    byte[] getData(String path,boolean watch){
        if(zooKeeper == null){
            init();
        }

        try {
            return  zooKeeper.getData(path,watch,null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    String create(String path,byte[] data,CreateMode createMode){
        if(zooKeeper == null){
            init();
        }
        try {
           return zooKeeper.create(path,data,null, createMode);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err
                    .println("USAGE: Executor hostPort [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];

       /* try {
            ZooKeeper zooKeeper = new ZooKeeper(hostPort, 2000, new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("event = [" + event + "]");
                }
            });

            byte[] b= zooKeeper.getData("/zk_watcher",true,null);

        } catch (Exception e) {
            e.printStackTrace();
        }*/
        ZkClient zkClient = new ZkClient(hostPort,3000);
        //System.out.println(Arrays.toString(zkClient.getData("/zk_watcher",true)));

        zkClient.create("/zk_ephemeral","this is a ephemeral node".getBytes(),CreateMode.PERSISTENT);

        while (true){
            //让程序保持运行，测试getData收到NodeDataChanged监听事件
           /* try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

        }
    }

}
