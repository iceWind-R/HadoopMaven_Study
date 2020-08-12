import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class ZookeeperApiDemo {
    @org.junit.Test
    public void createZnode() throws Exception {
        //1：定制一个重试策略
        /*
         * param1：重试的间隔时间，单位ms
         * param2：重试的最大次数
         * */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        //2：获取一个客户端对象
        /*
         * param1：要连接的Zookeeper服务器列表
         * param2:会话的超时时间
         * param3：连接的超时时间
         * param4：重试策略
         * */
        String connStr = "bigdata1:2181,bigdata2:2181,bigdata3:2181"; // 有多个主机时，每个主机之间用逗号连接，并且注意：不要为美观而乱加空格！
        CuratorFramework client = CuratorFrameworkFactory.newClient(connStr, 3000, 3000, retryPolicy);
        //3：开启客户端
        client.start();
        //4：创建结点
        /*
         * 客户端创建结点，creatingParentsIfNeeded表示：创建hello，但也会创建父节点Test
         * withMode表示类型，有四种
         * forPath表示结点路径和携带的数据，第二个参数为byte类型
         * */

        //创建永久结点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/Test/hello4", "JavaApiTest".getBytes());

        //创建临时性结点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/Test/tmp", "临时".getBytes());

        // 因为临时结点只在会话时有效，为保证看到效果，使程序休眠10s
        Thread.sleep(10000);

        // 修改结点数据
        client.setData().forPath("/Test/hello", "被修改".getBytes());

        //获取结点数据
        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));

        //5：关闭客户端
        client.close();

    }

    /*
     * 结点的watch机制
     * */
    @org.junit.Test
    public void watchZnode() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,1);
        String connStr = "bigdata1:2181,bigdata2:2181,bigdata3:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connStr, 8000, 8000, retryPolicy);
        client.start();

        // 创建一个TreeCache对象，指定要监控的结点路径
        TreeCache treeCache = new TreeCache(client, "/hello");

        // 自定义监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (data != null){
                    switch (treeCacheEvent.getType()){ //获取事件的触发类型
                        case NODE_ADDED: // 表示新增结点触发的watch
                            System.out.println("NODE_ADDED");
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED");
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED");
                            break;
                        default:
                            System.out.println("default");
                            break;
                    }
                }
            }
        });

        // 开始监听
        treeCache.start();

        Thread.sleep(100000);
        // 之后运行程序，在zookeeper中对指定的结点进行操作时，控制台便会打印相应信息。
    }
}
