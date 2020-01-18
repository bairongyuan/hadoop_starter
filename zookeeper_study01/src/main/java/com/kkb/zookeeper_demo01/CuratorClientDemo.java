package com.kkb.zookeeper_demo01;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

public class CuratorClientDemo {
    private static final String ZK_ADDRESS = "node01:2181,node02:2181,node03:2181";
    private static final String ZK_PATH = "/zk_test";

    static CuratorFramework client = null;

    public static void init() {
        RetryNTimes retryNTimes = new RetryNTimes(10, 5000);
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, retryNTimes);

        client.start();

        System.out.println("zk client start successfully!!!!");
    }

    public static void clean(){
        System.out.println("zk client close session");
        client.close();
    }

    public static void createPersistentZNode() throws Exception {
        String zNodeData = "火";
        System.out.println();
        client.create().creatingParentsIfNeeded().forPath("/kkb/test", zNodeData.getBytes());

    }

    public static void createEphemeralZNode() throws Exception {
        String zNodeDate = "水";

        System.out.println("create ephemeral node start");
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/kkb/test1", zNodeDate.getBytes());
        System.out.println("create ephemeral node end");
        Thread.sleep(5000);
        System.out.println("create ephemeral node over");
    }

    public static void queryNodeData() throws Exception {
        System.out.println("ls /");
        System.out.println( client.getChildren().forPath("/kkb") );

        if (client.getData().forPath("/kkb/test") != null) {
            byte[] bytes = client.getData().forPath("/kkb/test");
            String sb = new String(bytes);
//            for (int i =0 ; i < bytes.length;i++) {
//                sb.append(bytes[i]);
//            }
            System.out.println("bytes out start");
            System.out.println( sb );
            System.out.println("bytes out end");
        } else {
            System.out.println("node not exits");
        }
    }

    public static void watchZNode() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/kkb");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (data != null) {
                    switch (treeCacheEvent.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : " + data.getPath() + ", 数据 " + new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : " + data.getPath() + ", 数据 " + new String(data.getData()));
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : " + data.getPath() + ", 数据 " + new String(data.getData()));
                            break;
                        default:
                            break;
                    }
                }
            }
        });
        treeCache.start();
        Thread.sleep(30000);
        System.out.println("close treecach watch");
        treeCache.close();
    }

    public static void deleteZNode() throws Exception {
        System.out.println("delete start");
        client.delete().forPath("/kkb/test3");
        System.out.println("delete end");
    }

    public static void main(String[] args) throws Exception {
        init();
//        createPersistentZNode();
//        createEphemeralZNode();
//        queryNodeData();
//        watchZNode();
        deleteZNode();
        clean();
    }

}
