package redis.clients.jedis.executors;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;
import redis.clients.jedis.providers.ClusterConnectionProvider;
import redis.clients.jedis.util.IOUtils;

public class ClusterCommandExecutor implements CommandExecutor {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public final ClusterConnectionProvider provider;
  protected final int maxAttempts;
  protected final Duration maxTotalRetriesDuration;

  protected final AtomicInteger threadId = new AtomicInteger();
  protected final ScheduledExecutorService checkService;

  public ClusterCommandExecutor(ClusterConnectionProvider provider, int maxAttempts,
      Duration maxTotalRetriesDuration) {
    this.provider = provider;
    this.maxAttempts = maxAttempts;
    this.maxTotalRetriesDuration = maxTotalRetriesDuration;
    this.checkService = Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "CheckService_" + threadId.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            });
    this.checkService.scheduleWithFixedDelay(this::schedule, 1000, 3000, TimeUnit.MILLISECONDS);
  }

  private void schedule() {
    System.out.println("schedule at " + System.currentTimeMillis() + "...");
    Set<HostAndPort> unavailableNodes = Collections.unmodifiableSet(provider.getUnavailableNodes());
    for(HostAndPort hap : unavailableNodes) {
      System.out.println("check for [" + hap + "]...");
      if(check(hap)) {
        System.out.println("[" + hap + "] is alive, and removed from UnavailableNodes.");
        provider.getUnavailableNodes().remove(hap);
      }
    }
  }

  private boolean check(HostAndPort hap) {
    Connection connection = null;
    try {
      connection = this.provider.getConnection(hap);
      return connection.ping();
    } catch (Throwable t) {
      System.out.println("check and ping throw error:" + t.getMessage());
      t.printStackTrace();
      return false;
    } finally {
      if(connection != null) IOUtils.closeQuietly(connection);
    }
  }

  @Override
  public void close() {
    this.provider.close();
  }

  @Override
  public final <T> T executeCommand(CommandObject<T> commandObject) {
    System.out.println("maxTotalRetriesDuration=>" + maxTotalRetriesDuration.toMillis());
    Instant deadline = Instant.now().plus(maxTotalRetriesDuration);
    long start = System.currentTimeMillis();

    JedisRedirectionException redirect = null;
    int consecutiveConnectionFailures = 0;
    Exception lastException = null;
    for (int attemptsLeft = this.maxAttempts; attemptsLeft > 0; attemptsLeft--) {
      Connection connection = null;
      try {
        if (redirect != null) {
          connection = provider.getConnection(redirect.getTargetNode());
          System.out.println("1.getConn cost: " + (System.currentTimeMillis() - start) + "ms ...");
          if (redirect instanceof JedisAskDataException) {
            // TODO: Pipeline asking with the original command to make it faster....
            connection.executeCommand(Protocol.Command.ASKING);
            System.out.println("1.1 ASKING cost: " + (System.currentTimeMillis() - start) + "ms ...");
          }
        } else {
          connection = provider.getConnection(commandObject.getArguments());
          System.out.println("2.getConn cost: " + (System.currentTimeMillis() - start) + "ms ...");
        }

        return execute(connection, commandObject);

      } catch (JedisClusterOperationException jnrcne) {
        System.out.println("3.jnrcne cost: " + (System.currentTimeMillis() - start) + "ms ...");
        throw jnrcne;
      } catch (JedisConnectionException jce) {
        System.out.println("4.jce cost: " + (System.currentTimeMillis() - start) + "ms ...");
        lastException = jce;
        ++consecutiveConnectionFailures;
        log.debug("Failed connecting to Redis: {}", connection, jce);
        // "- 1" because we just did one, but the attemptsLeft counter hasn't been decremented yet
        boolean reset = handleConnectionProblem(attemptsLeft - 1, consecutiveConnectionFailures, deadline, start);
        System.out.println("4.1 handle cost: " + (System.currentTimeMillis() - start) + "ms ...");
        if (reset) {
          consecutiveConnectionFailures = 0;
          redirect = null;
        }
      } catch (JedisRedirectionException jre) {
        System.out.println("5.jre cost: " + (System.currentTimeMillis() - start) + "ms ...");
        // avoid updating lastException if it is a connection exception
        if (lastException == null || lastException instanceof JedisRedirectionException) {
          lastException = jre;
        }
        log.debug("Redirected by server to {}", jre.getTargetNode());
        consecutiveConnectionFailures = 0;
        redirect = jre;
        // if MOVED redirection occurred,
        if (jre instanceof JedisMovedDataException) {
          // it rebuilds cluster's slot cache recommended by Redis cluster specification
          provider.renewSlotCache(connection);
          System.out.println("5.1 renew cost: " + (System.currentTimeMillis() - start) + "ms ...");
        }
      } finally {
        System.out.println("6.close1 cost: " + (System.currentTimeMillis() - start) + "ms ...");
        IOUtils.closeQuietly(connection);
        System.out.println("6.1 close2 cost: " + (System.currentTimeMillis() - start) + "ms ...");
      }
      if (Instant.now().isAfter(deadline)) {
        JedisClusterOperationException exceededException = new JedisClusterOperationException("Cluster retry deadline exceeded.");
        throw processNodeDown(lastException, exceededException, (ClusterCommandArguments) commandObject.getArguments());
      }
    }

    System.out.println("total cost: " + (System.currentTimeMillis() - start) + "ms ...");

    JedisClusterOperationException maxAttemptsException
        = new JedisClusterOperationException("No more cluster attempts left.");
    throw processNodeDown(lastException, maxAttemptsException, (ClusterCommandArguments) commandObject.getArguments());
  }

  private JedisClusterOperationException processNodeDown(Exception lastException, JedisClusterOperationException jcoe, ClusterCommandArguments args) {
    jcoe.addSuppressed(lastException);
    final int slot = args.getCommandHashSlot();
    if(slot >= 0) {
      HostAndPort hap = provider.getNode(slot);
      Set<HostAndPort> unavailableNodes = provider.getUnavailableNodes();
      if (!unavailableNodes.contains(hap)) {
        unavailableNodes.add(hap);
      }
      System.out.println("Isolate node:" + hap.toString());
    }
    return jcoe;
  }

  /**
   * WARNING: This method is accessible for the purpose of testing.
   * This should not be used or overriden.
   */
  protected <T> T execute(Connection connection, CommandObject<T> commandObject) {
    return connection.executeCommand(commandObject);
  }

  /**
   * Related values should be reset if <code>TRUE</code> is returned.
   *
   * @param attemptsLeft
   * @param consecutiveConnectionFailures
   * @param doneDeadline
   * @return true - if some actions are taken
   * <br /> false - if no actions are taken
   */
  private boolean handleConnectionProblem(int attemptsLeft, int consecutiveConnectionFailures, Instant doneDeadline, long start) {
    System.out.println("10.x handle cost: " + (System.currentTimeMillis() - start) + "ms ..." + attemptsLeft);
    if (this.maxAttempts < 3) {
      // Since we only renew the slots cache after two consecutive connection
      // failures (see consecutiveConnectionFailures above), we need to special
      // case the situation where we max out after two or fewer attempts.
      // Otherwise, on two or fewer max attempts, the slots cache would never be
      // renewed.
      if (attemptsLeft == 0) {
        System.out.println("10.0 renew1 cost: " + (System.currentTimeMillis() - start) + "ms ...");
        provider.renewSlotCache();
        System.out.println("10.0 renew1 cost: " + (System.currentTimeMillis() - start) + "ms ...");
        return true;
      }
      return false;
    }

    if (consecutiveConnectionFailures < 2) {
      return false;
    }

    System.out.println("10.1 sleep1 cost: " + (System.currentTimeMillis() - start) + "ms ...");
    sleep(getBackoffSleepMillis(attemptsLeft, doneDeadline));
    // sleep(100);
    System.out.println("10.1 sleep2 cost: " + (System.currentTimeMillis() - start) + "ms ...");
    //We need this because if node is not reachable anymore - we need to finally initiate slots
    //renewing, or we can stuck with cluster state without one node in opposite case.
    //TODO make tracking of successful/unsuccessful operations for node - do renewing only
    //if there were no successful responses from this node last few seconds
    provider.renewSlotCache();
    System.out.println("10.2 renew cost: " + (System.currentTimeMillis() - start) + "ms ...");
    return true;
  }

  private static long getBackoffSleepMillis(int attemptsLeft, Instant deadline) {
    if (attemptsLeft <= 0) {
      return 0;
    }

    long millisLeft = Duration.between(Instant.now(), deadline).toMillis();
    if (millisLeft < 0) {
      throw new JedisClusterOperationException("Cluster retry deadline exceeded.");
    }

    long maxBackOff = millisLeft / (attemptsLeft * attemptsLeft);
    System.out.println("10.3 sleep backoff: " +  maxBackOff + "ms ...");
    return ThreadLocalRandom.current().nextLong(maxBackOff + 1);
  }

  /**
   * WARNING: This method is accessible for the purpose of testing.
   * This should not be used or overriden.
   */
  protected void sleep(long sleepMillis) {
    try {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
    } catch (InterruptedException e) {
      throw new JedisClusterOperationException(e);
    }
  }
}
