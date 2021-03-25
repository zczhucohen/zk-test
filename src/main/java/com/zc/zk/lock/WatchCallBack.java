package com.zc.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Title:  WatchCallBack
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-24 10:41
 **/
public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback,AsyncCallback.StatCallback {

    ZooKeeper zk;

    String threadName;

    CountDownLatch cd = new CountDownLatch(1);

    String pathName;


    public void tryLock(){
        try {
            zk.create("/lock",threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL,this,"bnafd");
            cd.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void unLock(){
        try {
            zk.delete(pathName,-1);
            System.out.println(threadName + " 释放锁成功！！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    @Override
    public void process(WatchedEvent event) {
        // 如果第一个节点，锁释放了，其实只有第二个收到了回调事件！！
        // 如果，不是第一个个们，某一个，挂了，也能造成他后边的收到这个通知，从而让他后边那个跟去watch挂掉这个个们前边的。
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                // 删除事件
                zk.getChildren("/",false,this,"sdf");
                break;
            case NodeDataChanged:
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
    public void processResult(int rc, String path, Object ctx, String name) {
        if(name != null){
            System.out.println(threadName+",createNode:"+name);
            pathName = name;
            zk.getChildren("/",false,this,"sdf");
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        // 一定能看到前面的。
        /*System.out.println(threadName+",Look,Lock...........");
        for (String child : children) {
            System.out.println("chile:"+child);
        }*/
        // 排序
        Collections.sort(children);
        int i = children.indexOf(pathName.substring(1));

        // System.out.println("pathName::"+pathName.substring(1)+",ch:"+children.get(i)+",i = "+i);
        // 是不是第一个
        if(i == 0){
            // yes
            System.out.println(threadName+" I am first......"+"I am "+pathName);
            try {
                zk.setData("/",threadName.getBytes(),-1);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cd.countDown();
        }else {
            // no
            // 盯着前面那个，一旦前面那个节点删除了，就要重新排序看看我是不是第一个。
            zk.exists("/"+children.get(i-1),this,this,"abd");
        }

    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {

    }
}
