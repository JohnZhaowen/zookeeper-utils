package zookeeper.lock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZkUtil {
	
	private static final String ZK_URL = "192.168.139.128:2181/lock";	
	private static final Integer SESSION_TIME_OUT = 2000;
	
	public static Boolean getLock(String lockName, ZooKeeper zk) {
		CountDownLatch latch = new CountDownLatch(1);
		String path = "/" + lockName;
		String tname = Thread.currentThread().getName();
		ZnodeWatcher w = null;
		try {
			
			String content = lockContent();
			w = new ZnodeWatcher(zk, lockName, content, latch);
			zk.create(path, content.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.printf("线程[%s]创建锁[%s]成功, 锁内容为[%s]%n", tname, lockName, content);
			latch.countDown(); //锁获取成功，同步结束
			
		} catch(UnknownHostException e) {//获取ip地址异常
			System.out.printf("线程[%s]创建锁[%s]时，获取host失败，无法生成锁内容[%s]%n", tname, lockName, e);
		} catch(KeeperException | InterruptedException e) {//zk创建锁节点异常，该节点已存在
			System.out.printf("线程[%s]创建锁[%s]失败[%s]%n", tname, lockName, e);
			
			//注册监听器，监听/lock的变化，如果子节点删除，则再次进行锁创建
			try {
				zk.getChildren("/", w);
			} catch (Exception e1) {
				System.out.printf("线程[%s]为获取锁[%s]时，创建监听失败[%s]%n", tname, lockName, e);
			}
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			System.out.printf("线程[%s]获取锁[%s]失败[%s]%n", tname, lockName, e);
			return false;
		}
		return true;
	}
	
	public static Boolean releaseLock(String lockName, ZooKeeper zk) {
		String tname = null;
		try {
			tname = Thread.currentThread().getName();
			String currentContent = lockContent(); //当前线程和ip拼装获得的节点内容
			//zk的对应锁节点中存储的节点内容
			String zkContent = new String(zk.getData("/" + lockName, true, null));
			
			if(currentContent.equals(zkContent)) {//如果是当前应用的线程加的锁，则释放
				zk.delete("/" + lockName, -1);	
				System.out.printf("线程[%s]释放锁[%s]成功, 锁内容为[%s]%n", tname, lockName, zkContent);
				return true;
			} else {
				System.out.printf("线程[%s]释放锁[%s]失败，当前线程获取的内容为[%s], 锁节点存储的内容为[%s], 两者不一致，不可释放%n", tname, lockName, currentContent, zkContent);
				return false;
			}
					
		}catch(Exception e) {
			System.out.printf("线程[%s]释放锁[%s]失败[%s]%n", tname, lockName, e);
			return false;
		}
	}
	
	/**
	 * 获取ZooKeeper，同步化处理
	 */
	public static ZooKeeper getZk() throws IOException, InterruptedException {	
		String tname = Thread.currentThread().getName();
		CountDownLatch latch = new CountDownLatch(1);
		ZooKeeper zk = new ZooKeeper(ZK_URL, SESSION_TIME_OUT, new MyWatcher(latch));
		latch.await();
		System.out.printf("[%s]获取zk[%s]成功%n", tname, zk.getSessionId());
		return zk;			
	}
	
	public static String lockContent() throws UnknownHostException {
			String tname = Thread.currentThread().getName();
			String ipAddress = InetAddress.getLocalHost().getHostAddress();
			return tname + ":" + ipAddress;
		
	}

}























