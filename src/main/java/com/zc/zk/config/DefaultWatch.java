package com.zc.zk.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * Title:  DefaultWatch
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-23 14:48
 **/
public class DefaultWatch implements Watcher {



    CountDownLatch init;
    public CountDownLatch getInit() {
        return init;
    }

    public void setInit(CountDownLatch init) {
        this.init = init;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        Event.KeeperState state = watchedEvent.getState();
        switch (state) {
            case Unknown:
                break;
            case Disconnected:
                break;
            case NoSyncConnected:
                break;
            case SyncConnected:
                init.countDown();
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
        System.out.println("watchedEvent::"+watchedEvent.toString());
    }
}
