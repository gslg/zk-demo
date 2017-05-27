package com.lg.zkdemo.masterworker;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.KeeperException.Code;
import static org.apache.zookeeper.Watcher.Event.EventType;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by liuguo on 2017/5/26.
 */
public class NMaster implements Watcher {

    private final Logger logger = Logger.getLogger(NMaster.class);

    private final String serverID = Integer.toHexString(new Random(this.hashCode()).nextInt());

    private volatile MasterState state = MasterState.RUNNING;
    /**
     * zk实例
     */
    private ZooKeeper zk;

    /**
     * zk server 主机:port[主机2:port2...]
     */
    private String hostPort;
    /**
     * 是否已连接
     */
    private volatile boolean connected = false;
    /**
     * 是否过期
     */
    private volatile boolean expired = false;

    /**
     * 任务缓存
     */
    protected ChildrenCache tasksCache;

    /**
     * workers缓存
     */
    protected ChildrenCache worksCache;

    NMaster(String hostPort){
        this.hostPort = hostPort;
    }

    /**
     * 创建一个zk实例,开启一个session
     * @throws IOException
     */
    void startZK() throws IOException {
        this.zk = new ZooKeeper(hostPort,15000,this);
    }

    /**
     * 关闭 zk session
     * @throws InterruptedException
     */
    void stopZK() throws InterruptedException {
        this.zk.close();
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("处理事件:"+event);

        if(event.getType() == EventType.None){
            switch (event.getState()){
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    logger.error("Session 过期");
                default:
                    break;
            }

        }
    }

    /**
     * 在程序启动时创建一些节本的父节点
     * 只会调用一次
     */
    void bootstrap(){
        createParent("/workers",new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    /**
     * 创建节点，将data[]作为context传入，方便在回调中丢失连接时取得数据重新创建
     * @param path
     * @param data
     */
    void createParent(String path,byte[] data){
        this.zk.create(path,data,OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,createParentCallback,data);
    }

    /**
     * 创建父节点异步回调处理
     */
    StringCallback createParentCallback = (rc,path,ctx,name)-> {
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                //loss connection--just retry
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                logger.info("Parent created:"+path+",data="+new String((byte[]) ctx));
                break;
            case NODEEXISTS:
                logger.warn("Parent already registered:"+path);
                break;
            default:
                logger.error("Something went wrong:"+ KeeperException.create(Code.get(rc),path));
        }
    };

    /**
     * 检查client是否已经连接上
     * @return
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * 检查session是否过期
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /*new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)){
                case CONNECTIONLOSS:
                    //loss connection--just retry
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    logger.info("Parent created:"+path+",data="+new String((byte[]) ctx));
                    break;
                case NODEEXISTS:
                    logger.warn("Parent already registered:"+path);
                    break;
                default:
                    logger.error("Something went wrong:"+ KeeperException.create(Code.get(rc),path));
            }
        }
    }; */


    enum MasterState{
        RUNNING,ELECTED,NOTELECTED
    }

    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */

    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of.
     *
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again.
     *
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    /**
     * master节点是否存在监听
     */
    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeDeleted){
                //master节点被删除
                assert "/master".equals(event.getPath());
                runForMaster();
            }
        }
    };

    /**
     * master节点是否存在回调函数
     */
    StatCallback masterExistsCallback = (rc,path,ctx,stat)->{
      switch (Code.get(rc)){
          case CONNECTIONLOSS:
              masterExists();
              break;
          case OK:
              break;
          case NONODE:
              //可能没有master节点,那么尝试竞争获取master
              logger.info("It sounds like the previous master is gone, " +
                      "so let's run for master again.");
              state = MasterState.RUNNING;
              runForMaster();
              break;
          default:
              checkMaster();
              break;
      }
    }; /*new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {

        }
    };*/


    void masterExists(){
        this.zk.exists("/master",masterExistsWatcher,masterExistsCallback,null);
    }

    /**
     * master创建回调
     */
    StringCallback masterCreateCallback = (rc,path,ctx,name)->{
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                checkMaster();
                break;
            case OK:
                //成功创建master,当选master
                state = MasterState.ELECTED;
                //执行master权利
                takeLeadership();
                break;
            case NODEEXISTS:
                //master节点已经存在,监听该节点
                state = MasterState.NOTELECTED;
                masterExists();
                break;
            default:
                state = MasterState.NOTELECTED;
                logger.error("Something went wrong when running for master.",
                        KeeperException.create(Code.get(rc), path));
        }
        logger.info("I'm " + (state == MasterState.ELECTED ? "" : "not ") + "the leader " + serverID);
    }; /*new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {

        }
    };*/

    /**
     * 竞争创建Master
     */
    void runForMaster(){

        this.zk.create("/master", serverID.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback,null);
    }

    /**
     * master 检查回调
     */
    DataCallback checkMasterCallback = (rc,path,ctx,data,stat)->{
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                checkMaster();
                break;
            case NONODE:
                runForMaster();
                break;
            case OK:
                if(serverID.equals(new String(data))){
                    //当前client当选为Master,履行Master权利
                    state = MasterState.ELECTED;
                    takeLeadership();
                }else {
                    //当前client没有当选,需要监听Master节点的变化
                    state = MasterState.NOTELECTED;
                    masterExists();
                }
                break;
            default:
                logger.error("Error when reading data:"+KeeperException.create(Code.get(rc),path));
        }
    }; /*new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

        }
    };*/

    /**
     * Master的权利
     */
    void takeLeadership(){
        getWorkers();
    }

    /**
     * 检查Master节点
     */
    void checkMaster(){

        zk.getData("/master", false, checkMasterCallback,null);
    }


    /**
     * 获取wokers的size
     * @return
     */
    public int getWorkersSize(){
        return worksCache == null ? 0 : worksCache.getChildren().size();
    }
    /**
     * 子节点变化监听
     */
    Watcher workersChangeWatcher = event -> {
        if(event.getType() == EventType.NodeChildrenChanged){
            //说明发生了变化:
            //重新获取workers,顺便就设置了新的watcher
            assert "/workers".equals(event.getPath());
            getWorkers();
        }
    }; /*new Watcher() {
        @Override
        public void process(WatchedEvent event) {

        }
    };*/

    /**
     * 子节点获取回调
     */
    ChildrenCallback workersGetChildrenCallback = (rc,path,ctx,children)->{
        switch (Code.get(rc)){
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                logger.info("Succesfully got a list of workers:"
                        +children.size()
                        +" workers.");
                // TODO: 2017/5/26  重新分配设置任务
                break;
            default:
                logger.error("GetChildren failed:"+KeeperException.create(Code.get(rc),path));
        }
    }; /*new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {

        }
    };*/

    /**
     * 获取workers
     */
    void getWorkers(){
        zk.getChildren("/workers",workersChangeWatcher,workersGetChildrenCallback,null);
    }

}
