package cn.kimmking.cache;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisClusterOperationException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Description for this class.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2023/2/15 19:03
 */
public class TestNodes {

    public static void main(String[] args) throws IOException {

        List<HostAndPort> hosts = Arrays.asList(
                new HostAndPort("127.0.0.1", 7000),
                new HostAndPort("127.0.0.1", 7001),
                new HostAndPort("127.0.0.1", 7002));

        DefaultJedisClientConfig config = DefaultJedisClientConfig.builder().connectionTimeoutMillis(1000)
                .socketTimeoutMillis(1000).timeoutMillis(1000).build();

        JedisCluster cluster = new JedisCluster(new HashSet<>(hosts), config, 3, Duration.ofMillis(6000));

        int c = 0;
        while (c != 'q') {
            c = System.in.read();
            if (c == 's') {
                testSet(cluster);
            } else if(c == 'g') {
                testGet(cluster);
            } else if(c == 'c') {
                testCluster(cluster);
            } else if(c == 'u') {
                testUnavailable(cluster);
            }
        }

        cluster.close();

    }

    private static void testUnavailable(JedisCluster cluster) {
        System.out.println(cluster.getUnavailableNodes());
    }

    private static void testGet(JedisCluster cluster) {
        try {
            long ts = System.currentTimeMillis();
            String a1 = cluster.get("a" + ts);
            System.out.println("get a" + ts + "=" + a1);
        } catch (JedisClusterOperationException ex) {
            ex.printStackTrace();
        }
    }

    private static void testSet(JedisCluster cluster) {
        try{
            long ts = System.currentTimeMillis();
            String a1 = cluster.set("a" + ts, "K1");
            System.out.println("set a"+ts+":" +a1);
        } catch (JedisClusterOperationException ex) {
            ex.printStackTrace();
        }
    }

    private static void testCluster(JedisCluster cluster) {
        Map<HostAndPort, Set<Integer>> slots = cluster.getNodeSlots();
        System.out.println("cluster master size: " + slots.size());
        System.out.println(slots.keySet());
    }

}
