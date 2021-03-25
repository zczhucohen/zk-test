package com.zc.zk.lock;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Title:  TestLock
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-24 10:35
 **/
public class TestLock {

    ZooKeeper zk;

    @Before
    public void conn(){
        zk = ZKUtils.getZK();
    }

    @After
    public void close(){
        try {
            zk.close();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void lock(){
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                WatchCallBack watchCallBack = new WatchCallBack();
                watchCallBack.setZk(zk);
                String threadName = Thread.currentThread().getName();
                watchCallBack.setThreadName(threadName);
                // 每一个线程
                // 抢锁
                watchCallBack.tryLock();;
                //干活
                System.out.println(threadName+" I am working...........");
               /* try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                // 释放锁
                watchCallBack.unLock();

            }).start();

        }
        while (true){

        }

    }




}