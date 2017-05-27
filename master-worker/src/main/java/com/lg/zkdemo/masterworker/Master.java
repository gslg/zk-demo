package com.lg.zkdemo.masterworker;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by liuguo on 2017/5/23.
 */
public class Master implements Watcher {

    private Logger logger = Logger.getLogger(Master.class);
    /**
     * 随机的服务ID
     */
    private String serverID = Integer.toHexString(new Random().nextInt());

    private ZooKeeper zk;

    private String hostPort;

    /**
     * 是否是leader
     */
    boolean isLeader;


    Master(String hostPort){
        this.hostPort = hostPort;
    }

    /**
     * 启动一个ZK
     */
    public void startZK() throws IOException {
       this.zk = new ZooKeeper(hostPort,15000,this);
        bootstrap();
    }

    /**
     * 关闭ZK.会明确告诉ZK server销毁session.
     * 如果不调用zk.close(),session会等过期时销毁.
     * @throws InterruptedException
     */
    public void stopZK() throws InterruptedException {
        this.zk.close();
    }

    /**
     * zk监听事件处理
     * @param event
     */
    public void process(WatchedEvent event) {
        System.out.println("event = [" + event + "]");
    }

    //return true if there is a master
    boolean checkMaster(){
        while (true){
            Stat stat = new Stat();
            try {
                byte[] data = zk.getData("/master",false,stat);
                isLeader = String.valueOf(data).equals(serverID);
                return true;
            } catch (NoNodeException e) {
                //no master,so try create again
                return false;
            }catch (ConnectionLossException e) {
                //丢失连接
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 同步方法.竞争master
     * 1.会同步创建/master节点,如果该znode已存在,则创建失败
     * 2.znode数据是随机生成的serverID,转为byte array
     * 3.使用OPEN_ACL_UNSAFE ACL
     * 4.CreateMode.EPHEMERAL表示临时节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    void runForMaster() throws InterruptedException {
        while (true){
            try {
                zk.create("/master",serverID.getBytes(),OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (NodeExistsException e) {
                isLeader = false;
                break;
            } catch (ConnectionLossException e){

            } catch (KeeperException e) {

            }

            if(checkMaster()) break;
        }
    }

    /**
     * 异步检查
     */
    void asyncCheckMaster(){
        zk.getData("/master",false,masterCheckCallback,null);
    }

    /**
     * 异步调用 竞争master
     */
    void asyncRunForMaster(){
        zk.create("/master",serverID.getBytes(),OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL,masterCreateCallback,null);
    }

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * 异步创建Master node回调
     */
    StringCallback masterCreateCallback = (rc,path,ctx,name) ->{
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case OK:
                isLeader = true;
                break;
            default:
                isLeader = false;
        }

        System.out.println("I'm " + (isLeader ? "" : "not ") +
                "the leader");
    };
    /* new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }

            System.out.println("I'm " + (isLeader ? "" : "not ") +
                    "the leader");
        }
    };*/

    /**
     * 异步获取znode回调
     */
    DataCallback masterCheckCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                asyncCheckMaster();
                return;
            case NONODE:
                asyncRunForMaster();
                return;
        }
    };
  /*   new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)){
                case CONNECTIONLOSS:
                    asyncCheckMaster();
                    return;
                case NONODE:
                    asyncRunForMaster();
                    return;
            }
        }
    };*/


    /**
     * 初始化启动程序,创建元数据节点，数据为空byte[]
     */
    void bootstrap(){
       this.createParent("/workers",new byte[0]);
       this.createParent("/tasks",new byte[0]);
       this.createParent("/assign",new byte[0]);
       this.createParent("/status",new byte[0]);
    }

    /**
     * 创建基本的元数据节点
     * @param path znode路劲
     * @param data znode数据
     */
    void createParent(String path,byte[] data){
        //将数据传入contex中，方便如果创建丢失连接在回调中获得该数据并继续创建
        zk.create(path,data,OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallback,data);
    }

    /**
     * 创建基本的元数据节点回调
     */
    StringCallback createParentCallback = (rc,path,ctx,name)->{
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                logger.info("Parent["+path+"]created");
                break;
            case NODEEXISTS:
                logger.warn("Parent already registered:"+path);
                break;
            default:
                logger.error("Something went wrong:"+KeeperException.create(Code.get(rc),path));
        }
    };

    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.err
                    .println("缺少ZK的hostPort");
            System.exit(2);
        }
        Master master = new Master(args[0]);
        master.startZK();

        //master.runForMaster();
        master.asyncRunForMaster();

        //暂停10s等待异步创建完毕，同步的创建不需要
        System.out.println("异步情况下我先打印");
        TimeUnit.SECONDS.sleep(10);

        if(master.isLeader()){
            //logic
            System.out.println("我是Master..");
        }else {
            System.out.println("其它是Master..");
        }

        TimeUnit.SECONDS.sleep(60);

        master.stopZK();
    }

}
