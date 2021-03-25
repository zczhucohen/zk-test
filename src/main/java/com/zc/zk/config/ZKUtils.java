package com.zc.zk.config;

import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Title:  ZKUtils
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-23 14:43
 **/
public class ZKUtils {

    private static ZooKeeper zk;

    // 只看testConf 不会看同级的目录。
    private static String address = "47.94.84.253:2181/testConf";

    private static DefaultWatch defaultWatch = new DefaultWatch();

    private static CountDownLatch init = new CountDownLatch(1);

    public static ZooKeeper getZK(){
        try {
            zk = new ZooKeeper(address,1000,defaultWatch);
            defaultWatch.setInit(init);
            init.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return zk;
    }
}
