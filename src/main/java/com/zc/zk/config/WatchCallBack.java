package com.zc.zk.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * Title:  WatchCallBack
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-23 15:39
 **/
public class WatchCallBack implements Watcher, AsyncCallback.StatCallback,AsyncCallback.DataCallback {



    ZooKeeper zk;

    MyConf myConf;

    CountDownLatch cd = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
            case NodeDataChanged:
                zk.getData("/AppConf",this,this,"sdfs");
                break;
            case NodeDeleted:
                // 节点被删除- 容忍性，已经取过配置。

                myConf.setConf("");
                cd = new CountDownLatch(1);
                break;

            case NodeChildrenChanged:
                break;
            case DataWatchRemoved:
                break;
            case ChildWatchRemoved:
                break;
            case PersistentWatchRemoved:
                break;
        }

    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if(data != null){
            String s = new String(data);
            myConf.setConf(s);
            cd.countDown();
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if(stat != null){
            zk.getData("/AppConf",this,this,"sdfs");
        }
    }

    public void await(){
        zk.exists("/AppConf", this,this,"ABC");
        try {
            cd.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public MyConf getMyConf() {
        return myConf;
    }

    public void setMyConf(MyConf myConf) {
        this.myConf = myConf;
    }
}
