package com.lg.zkdemo.watcher;

import org.apache.zookeeper.*;
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

    private ZooKeeper zk;

    private String hostPort;
    /**
     * 随机的服务ID
     */
    private String serverID = Integer.toHexString(new Random().nextInt());

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
    }

    /**
     * 关闭ZK
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
     * 竞争master
     * 1.会创建/master节点,如果该znode已存在,则创建失败
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

    public static void main(String[] args) throws Exception {
        Master master = new Master("192.168.190.133:2181");
        master.startZK();

        TimeUnit.SECONDS.sleep(60);

        master.stopZK();
    }

}
