package zookeeper.lock;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeWatcher implements Watcher {
	
	private ZooKeeper zk;
	private String lockName;
	private String content;
	private CountDownLatch latch;
	
	public ZnodeWatcher(ZooKeeper zk, String lockName, String content, CountDownLatch latch) {
		this.zk = zk;
		this.lockName = lockName;
		this.content = content;
		this.latch = latch;
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getType().equals(EventType.NodeChildrenChanged)) {	
			
			String tname = Thread.currentThread().getName();
			ZnodeWatcher w = new ZnodeWatcher(zk, lockName, content, latch);
			try {
				List<String> c = zk.getChildren("/", null);
				if(c != null && !c.isEmpty()) {
					System.out.printf("线程[%s]检测到节点/lock新增子节点事件，不处理%n", tname);
					return;
				}
			} catch (KeeperException | InterruptedException e) {
				System.out.printf("线程[%s]获取/lock的子节点失败[%s], 无法获知是否删除节点信息%n", tname, e);
			}
			System.out.printf("线程[%s]检测到锁[%s]释放事件%n", tname, lockName);
			try {
				zk.create("/" + lockName, content.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.printf("===线程[%s]创建锁[%s]成功，锁的内容为[%s]%n", tname, lockName, content);
				latch.countDown();
			} catch (KeeperException | InterruptedException e) {
				try {
					zk.getChildren("/", w);
				} catch (Exception e1) {
					System.out.printf("线程[%s]为获取锁[%s]时，创建监听失败[%s]%n", tname, lockName, e);
				} 
				
			}
		}
	}
}
