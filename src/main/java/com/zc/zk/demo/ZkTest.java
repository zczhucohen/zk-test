package com.zc.zk.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * Title:  ZkTest
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-19 17:04
 **/
public class ZkTest {

    public static void main(String[] args) throws Exception {


        CountDownLatch count = new CountDownLatch(1);
        // watch 观察回调，
        // 第一类，是在new zk时候，传入的watch，这个watch，是session级别的，跟path，node没有关系。
        // Watch的回调方法
        // Watch的注册只发生在读类型调用，get，exites。
        ZooKeeper zk = new ZooKeeper("47.94.84.253:2181", 3000, watchedEvent -> {
            Watcher.Event.KeeperState state = watchedEvent.getState();
            Watcher.Event.EventType type = watchedEvent.getType();
            String path = watchedEvent.getPath();
            System.out.println("watchedEvent:" + watchedEvent.toString());
            switch (state) {
                case Unknown:
                    break;
                case Disconnected:
                    break;
                case NoSyncConnected:
                    break;
                case SyncConnected:
                    System.out.println("connect");
                    count.countDown();
                    break;
                case AuthFailed:
                    break;
                case ConnectedReadOnly:
                    break;
                case SaslAuthenticated:
                    break;
                case Expired:
                    break;
                case Closed:
                    break;
            }
            switch (type) {
                case None:
                    break;
                case NodeCreated:
                    break;
                case NodeDeleted:
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
        });
        count.await();
        ZooKeeper.States state = zk.getState();
        switch (state) {
            case CONNECTING:
                System.out.println("ing......");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                System.out.println("end......");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }

        String s = zk.create("/oopp", "opop".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat stat = new Stat();
        byte[] node = zk.getData("/oopp", watchedEvent -> {
            System.out.println("get:===" + watchedEvent.toString());
            try {
                // true default Watch 被重新注册，new zk的那个watch。
                zk.getData("/oopp",true,stat);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, stat);

        System.out.println(node);

        Stat stat1 = zk.setData("/oopp", "newoopp".getBytes(), 0);

        // 还会触发声明
        Stat stat2 = zk.setData("/oopp", "newoopp2222".getBytes(), stat1.getVersion());

        zk.getData("/oopp", false, (i, s1, o, bytes, stat3) -> {
            System.out.println("sync call back");
            System.out.println("rc:"+i+",path:"+s1+",ctx:"+o+",bytes:"+new String(bytes)+",stat:"+stat3.toString());
        },"abc");
        System.out.println("------async over-----");
        Thread.sleep(20000);
        //
    }
}
