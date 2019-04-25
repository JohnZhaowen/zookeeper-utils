package zookeeper.lock;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class MyWatcher implements Watcher {
	
	private CountDownLatch latch;
	
	public MyWatcher(CountDownLatch latch) {
		this.latch = latch;
	}

	public void process(WatchedEvent event) {
		//System.out.println("接收到事件： " + event);
		if(event.getState().equals(KeeperState.SyncConnected)) {
			//System.out.println("连接成功...");
			latch.countDown();
		}
		if(event.getState().equals(KeeperState.Expired)) {
			//System.out.println("连接超时...");
			latch.countDown();
		}
		
		if(event.getState().equals(KeeperState.Disconnected)) {
			//System.out.println("连接断开...");
		}
		
		if(event.getType().equals(EventType.NodeCreated)) {
			//System.out.println("新建节点。。。。");
		}
		
	}

}
