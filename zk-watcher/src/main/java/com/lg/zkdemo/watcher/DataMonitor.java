package com.lg.zkdemo.watcher;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;

/**
 * Created by sclg1 on 2017/5/17.
 */
public class DataMonitor implements Watcher,StatCallback {

    ZooKeeper zk;

    String znode;

    Watcher chainedWatcher;

    boolean dead;

    DataMonitorListener listener;

    byte prevData[];

    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher, DataMonitorListener listener) {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        zk.exists(znode,true,this,null);
    }

    public void process(WatchedEvent event) {

        String path = event.getPath();
        if(event.getType() == Event.EventType.None){
            System.out.println("连接的状态改变了");
            //被告知连接的状态已经改变了
            switch (event.getState()){
                case SyncConnected:
                    break;
                case Expired:
                    System.out.println("It's all over");
                    dead = true;
                    listener.closing(Code.SESSIONEXPIRED.intValue());
                    break;

            }
        }else {
            if(path != null && path.equals(znode)){
                //在节点上发生了一些变化，让我们找出
                System.out.println("Something has changed on the node");
                zk.exists(znode,true,this,null);
            }
        }

        if(chainedWatcher != null){
            chainedWatcher.process(event);
        }
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {

        boolean exists=false;
        Code code = Code.get(rc);
        if(code == Code.OK){
            exists = true;
        }else if(code == Code.NONODE){
            exists = false;
        }else if(code == Code.SESSIONEXPIRED){

        }else if(code == Code.NOAUTH){
            dead = true;
            listener.closing(rc);
            return;
        }else {
            //Retry errors
            zk.exists(znode,true,this,null);
            return;
        }
        byte b[] = null;

        if(exists){
            try {
                b = zk.getData(znode,false,null);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            } catch (KeeperException e) {
                //我们现在不需要担心恢复。看回调将启动任何异常处理
                e.printStackTrace();
            }
        }
        if((b == null && b != prevData) || (b != null && !Arrays.equals(prevData,b))){
            System.out.println("存在的节点的状态已经改变");
            listener.exists(b);
            prevData = b;
        }

    }

    public interface DataMonitorListener{
        /**
         * The existence status of the node has changed.
         */
        void exists(byte data[]);

        /**
         * The ZooKeeper session is no longer valid.
         *
         * @param rc the ZooKeeper reason code
         */
        void closing(int rc);
    }
}
