package com.zc.zk.config;


import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Title:  TestConfig
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-23 14:41
 **/
public class TestConfig {

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
    public void getConf(){
        WatchCallBack watchCallBack = new WatchCallBack();
        watchCallBack.setZk(zk);
        MyConf myConf = new MyConf();
        watchCallBack.setMyConf(myConf);

        watchCallBack.await();
        // 节点不存在
        // 节点存在

        while (true){
            if(StringUtils.isEmpty(myConf.getConf())){
                System.out.println("conf diu le......");
                watchCallBack.await();
            }else {
                System.out.println(myConf.getConf());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

}
