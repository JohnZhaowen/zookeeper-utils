package zookeeper.lock;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooKeeper;

public class TestMain {
	
	private static final String lockName = "distribute";

	public static void main(String[] args) throws Exception {
		
		for(int i = 0; i < 10; i++) {
			new Thread(()->{
				try {
					ZooKeeper zk = getZk();
					ZkUtil.getLock(lockName, zk);
					Thread.sleep(2000);					
					ZkUtil.releaseLock(lockName, zk);
					zk.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}) .start();
		}		
	}
	
	
	public static ZooKeeper getZk() {
		CountDownLatch latch = new CountDownLatch(1);
		try {
			ZooKeeper zk = new ZooKeeper("192.168.139.128:2181/lock", 2000, new MyWatcher(latch));
			latch.await();
			return zk;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
