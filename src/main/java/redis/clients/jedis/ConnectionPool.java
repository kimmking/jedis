package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.util.Pool;

public class ConnectionPool extends Pool<Connection> {

  // 先放着，应该用不到，provider.cache里缓存了一份。 by kimmking 2023-02-14 16:15:52
  private HostAndPort hostAndPort;
  public HostAndPort getHostAndPort() {
    return this.hostAndPort;
  }

  public ConnectionPool(HostAndPort hostAndPort, JedisClientConfig clientConfig) {
    this(new ConnectionFactory(hostAndPort, clientConfig));
    //this.hostAndPort = hostAndPort;
  }

  public ConnectionPool(PooledObjectFactory<Connection> factory) {
    super(factory);
    this.hostAndPort = ((ConnectionFactory) factory).getHostAndPort();
  }

  public ConnectionPool(HostAndPort hostAndPort, JedisClientConfig clientConfig,
      GenericObjectPoolConfig<Connection> poolConfig) {
    this(new ConnectionFactory(hostAndPort, clientConfig), poolConfig);
    //this.hostAndPort = hostAndPort;
  }

  public ConnectionPool(PooledObjectFactory<Connection> factory,
      GenericObjectPoolConfig<Connection> poolConfig) {
    super(factory, poolConfig);
    this.hostAndPort = ((ConnectionFactory) factory).getHostAndPort();
  }

  @Override
  public Connection getResource() {
    Connection conn = super.getResource();
    conn.setHandlingPool(this);
    return conn;
  }
}
