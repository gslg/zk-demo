package com.lg.zkdemo.masterworker;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by liuguo on 2017/5/24.
 */
public class Worker implements Watcher{

    private Logger logger = Logger.getLogger(Worker.class);

    private String serverID = Integer.toHexString(new Random().nextInt());

    private String hostPort;

    private ZooKeeper zk;

    private String name;

    private String status;

    /**
     * 创建Worker回调
     */
    StringCallback createWorkerCallback = (rc,path,ctx,name) -> {
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                logger.info("Worker Registered Successfully:"+serverID+",name="+name);
                this.name = name;
                break;
            case NODEEXISTS:
                logger.warn("Worker Already Registered:"+serverID+",name="+name);
                this.name = name;
                break;
            default:
                logger.error("Something went wrong: "
                        + KeeperException.create(Code.get(rc), path));
        }
    };

    Worker(String hostPort){
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    /**
     * 注册worker Idle代表状态Status值
     * 临时节点
     */
    void register(){
        zk.create("/workers/worker-"+serverID,"Idle".getBytes(),OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,createWorkerCallback,null);
    }

    @Override
    public void process(WatchedEvent e) {
        logger.info("Watched Event:"+e.toString());
    }

    StatCallback statCallback = (rc,path,ctx,stat) -> {
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                return;
        }
    }; /*new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)){
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };*/

    /**
     * 更新状态
     * @param status
     */
    synchronized private void updateStatus(String status){
        if(status == this.status){
            zk.setData(name,status.getBytes(),-1,statCallback,status);
        }
    }

    /**
     * 设置并更新状态
     * @param status
     */
    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length == 0){
            System.err
                    .println("缺少ZK的hostPort");
            System.exit(2);
        }

        Worker worker = new Worker(args[0]);

        worker.startZk();

        worker.register();

        TimeUnit.SECONDS.sleep(30);
    }

}
