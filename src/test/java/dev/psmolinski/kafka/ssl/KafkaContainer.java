package dev.psmolinski.kafka.ssl;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Wrapper container object for the official confluentinc/cp-server image.
 * @param <SELF>
 */
public class KafkaContainer<SELF extends KafkaContainer<SELF>> extends GenericContainer<SELF> {

  public KafkaContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
  }

  public static <T extends KafkaContainer<T>> KafkaContainer<T> cpServer(String version) {
    return new KafkaContainer<>(DockerImageName.parse("confluentinc/cp-server:"+version));
  }

  @Override
  protected void configure() {
    super.configure();

    withEnvDefault("KAFKA_OPTS", String.join(" ", jvmOpts));

    // KAFKA_ZOOKEEPER_CONNECT and KAFKA_ADVERTISED_LISTENERS are mandatory in configure script
    withEnvDefault("KAFKA_ZOOKEEPER_CONNECT", zookeeperConnect);

    if (!processRoles.isEmpty()) {
      withEnvDefault("KAFKA_PROCESS_ROLES", String.join(",", processRoles));
      withEnvDefault("KAFKA_CONTROLLER_QUORUM_VOTERS",
              controllerQuorumVoters.stream()
              .map(QuorumVoter::asString)
              .collect(Collectors.joining(",")));
      withEnvDefault("KAFKA_NODE_ID", String.valueOf(nodeId));
      withEnvDefault("KAFKA_CLUSTER_ID", clusterId);

      withCommand("sh", "-c", String.join("\n",
              "#!/bin/bash -e",

              // that's storage dir formatting in Apache Kafka 3.0.x
              "echo -n > $KAFKA_LOG_DIRS/meta.properties",
              "echo cluster.id=$KAFKA_CLUSTER_ID >> $KAFKA_LOG_DIRS/meta.properties",
              "echo node.id=$KAFKA_NODE_ID >> $KAFKA_LOG_DIRS/meta.properties",
              "echo version=1 >> $KAFKA_LOG_DIRS/meta.properties",

              // skip ensure which validates the ZooKeeper is available
              "/etc/confluent/docker/configure",
              // run the launch with exec to avoid empty parent shell and retain PID=1
              "exec /etc/confluent/docker/launch"
      ));

    }

    // set the defaults
    if (logDirs.isEmpty()) {
      logDirs.add("/var/lib/kafka/data");
    }
    withEnvDefault("KAFKA_LOG_DIRS", String.join(",", logDirs));

    withEnvDefault("KAFKA_BROKER_ID", String.valueOf(brokerId));
    withEnvDefault("KAFKA_LISTENERS", listeners.values().stream().map(Listener::getListener).collect(Collectors.joining(",")));
    withEnvDefault("KAFKA_ADVERTISED_LISTENERS", listeners.values().stream().map(Listener::getAdvertisedListener).collect(Collectors.joining(",")));
    withEnvDefault("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", listeners.values().stream().map(Listener::getSecurityProtocol).collect(Collectors.joining(",")));
    for (Listener listener : listeners.values()) {
      for (Map.Entry<String,String> e : listener.settings.entrySet()) {
        withEnvDefault("KAFKA_LISTENER_NAME_"+e.getKey()+"_"+asEnv(e.getKey()), e.getValue());
      }
    }

    for (Listener listener : listeners.values()) {
      if (listener.exposed) {
        // same port has to be used, because the advertised (like localhost:9092) and open
        // port must be the same
        addFixedExposedPort(listener.port, listener.port);
      }
    }

    if (interBrokerListener!=null) {
      withEnvDefault("KAFKA_INTER_BROKER_LISTENER_NAME", interBrokerListener);
    }

    if (controlPlaneListener!=null) {
      withEnvDefault("KAFKA_CONTROL_PLANE_LISTENER_NAME", controlPlaneListener);
    }
    if (!controllerListenerNames.isEmpty()) {
      withEnvDefault("KAFKA_CONTROLLER_LISTENER_NAMES", String.join(",", controllerListenerNames));
    }

    withEnvDefault("KAFKA_DEFAULT_REPLICATION_FACTOR", String.valueOf(replicationFactor));
    withEnvDefault("KAFKA_MIN_INSYNC_REPLICAS", String.valueOf(minIsr));
    withEnvDefault("KAFKA_NUM_PARTITIONS", String.valueOf(numPartitions));

    withEnvDefault("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", String.valueOf(replicationFactor));

    withEnvDefault("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", String.valueOf(replicationFactor));
    withEnvDefault("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", String.valueOf(minIsr));

    withEnvDefault("KAFKA_AUTO_CREATE_TOPICS_ENABLE", String.valueOf(autoCreateTopicsEnable));

    withEnvDefault("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", String.valueOf(replicationFactor));
    withEnvDefault("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", String.valueOf(minIsr));

    withEnvDefault("KAFKA_CONFLUENT_CLUSTER_LINK_ENABLE", String.valueOf(confluentClusterLinkEnable));
    withEnvDefault("KAFKA_CONFLUENT_BALANCER_ENABLE", String.valueOf(confluentBalancerEnable));

    // Kafka broker local license topic
    withEnvDefault("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", String.valueOf(replicationFactor));
    withEnvDefault("KAFKA_CONFLUENT_LICENSE_TOPIC_MIN_INSYNC_REPLICAS", String.valueOf(minIsr));

    if (schemaRegistryUrl != null) {
      withEnvDefault("KAFKA_SCHEMA_REGISTRY_URL", schemaRegistryUrl);
    }

    withEnvDefault("KAFKA_LOG4J_ROOT_LOGLEVEL", "INFO");
    withEnvDefault("KAFKA_LOG4J_LOGGERS", "kafka.authorizer.logger=INFO");

  }

  public SELF withEnvDefault(String key, String value) {
    if (!getEnvMap().containsKey(key)) {
      return withEnv(key, value);
    }
    return self();
  }

  private String zookeeperConnect = "zookeeper:2181";
  public SELF withZookeeperConnect(String zookeeperConnect) {
    this.zookeeperConnect = zookeeperConnect;
    return self();
  }

  private String interBrokerListener = null;
  public SELF withInterBrokerListener(String interBrokerListener) {
    this.interBrokerListener = interBrokerListener;
    return self();
  }

  private String controlPlaneListener = null;
  public SELF withControlPlaneListener(String controlPlaneListener) {
    this.controlPlaneListener = controlPlaneListener;
    return self();
  }

  private List<String> controllerListenerNames = new LinkedList<>();
  public SELF withControllerListenerNames(String...controllerListenerNames) {
    this.controllerListenerNames.addAll(Arrays.asList(controllerListenerNames));
    return self();
  }

  private String schemaRegistryUrl = null;
  public SELF withSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    return self();
  }

  private int brokerId = 0;
  public SELF withBrokerId(int brokerId) {
    this.brokerId = brokerId;
    return self();
  }

  private int replicationFactor = 1;

    /**
     * Set the default replication factor. Unlike in the real deployment, test container is usually
   * deployed as a single node.
   * @param replicationFactor
     * @return self
   */
  public SELF withReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
    return self();
  }

  private int numPartitions = 1;
  public SELF withNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return self();
  }

  private int minIsr = 1;
  public SELF withMinIsr(int minIsr) {
    this.minIsr = minIsr;
    return self();
  }

  private boolean autoCreateTopicsEnable = false;
  public SELF withAutoCreateTopicsEnable(boolean autoCreateTopicsEnable) {
    this.autoCreateTopicsEnable = autoCreateTopicsEnable;
    return self();
  }

  private boolean confluentClusterLinkEnable = true;
  public SELF withConfluentClusterLinkEnable(boolean confluentClusterLinkEnable) {
    this.confluentClusterLinkEnable = confluentClusterLinkEnable;
    return self();
  }
  private boolean confluentBalancerEnable = true;
  public SELF withConfluentBalancerEnable(boolean confluentBalancerEnable) {
    this.confluentBalancerEnable = confluentBalancerEnable;
    return self();
  }

  private Map<String, Listener> listeners = new LinkedHashMap<>();

  /**
   * Register a listener
   * @param name upper-case listener name
   * @param port port to be used (note: if port is exposed it must be the same)
   * @param exposed whether the port is exposed
   * @param hostname advertised host name
   * @param securityProtocol
   * @param settings
   * @return
   */
  public SELF withListener(String name, int port, boolean exposed, String hostname, String securityProtocol, Map<String,String> settings) {
    this.listeners.put(name, new Listener(name, port, exposed, hostname, securityProtocol, settings));
    return self();
  }

  private String clusterId = "-this-is-test-cluster-";
  /**
   * The cluster id is a unique and immutable identifier assigned to a Kafka cluster.
   * The cluster id can have a maximum of 22 characters and the allowed characters
   * are defined by the regular expression [a-zA-Z0-9_\-]+, which corresponds to the characters
   * used by the URL-safe Base64 variant with no padding. Conceptually, it is auto-generated
   * when a cluster is started for the first time.
   * @param clusterId
   * @return self
   */
  public SELF withClusterId(String clusterId) {
    this.clusterId = clusterId;
    return self();
  }

  private List<String> logDirs = new LinkedList<>();
  public SELF withLogDirs(String...logDir) {
    this.logDirs.addAll(Arrays.asList(logDir));
    return self();
  }

  private int nodeId = -1;
  public SELF withNodeId(int nodeId) {
    this.nodeId = nodeId;
    return self();
  }

  private List<QuorumVoter> controllerQuorumVoters = new LinkedList<>();
  public SELF withControllerQuorumVoter(int nodeId, String hostname, int port) {
    this.controllerQuorumVoters.add(new QuorumVoter(nodeId, hostname, port));
    return self();
  }

  private List<String> processRoles = new LinkedList<>();
  public SELF withProcessRoles(String...processRoles) {
    this.processRoles.addAll(Arrays.asList(processRoles));
    return self();
  }

  private List<String> jvmOpts = new LinkedList<>();
  public SELF withJvmOpts(String...kafkaOpts) {
    this.jvmOpts.addAll(Arrays.asList(kafkaOpts));
    return self();
  }

  public SELF withFixedExposedPort(int hostPort, int containerPort) {
    addFixedExposedPort(hostPort, containerPort);
    return self();
  }

  private static String asEnv(String key) {
    return key.replace('.', '_').toUpperCase();
  }

  private static class Listener {
    public Listener(String name, int port, boolean exposed, String hostname, String securityProtocol, Map<String, String> settings) {
      this.name = name;
      this.port = port;
      this.exposed = exposed;
      this.hostname = hostname;
      this.securityProtocol = securityProtocol;
      this.settings = settings;
    }
    String name;
    int port;
    boolean exposed;
    String hostname;
    String securityProtocol;
    Map<String, String> settings;
    public String getListener() {
      return name.toUpperCase()+"://:"+port;
    }
    public String getAdvertisedListener() {
      return name.toUpperCase()+"://"+hostname+":"+port;
    }
    public String getSecurityProtocol() {
      return name.toUpperCase()+":"+securityProtocol;
    }
  }

  private static class QuorumVoter {
    public QuorumVoter(int nodeId, String hostname, int port) {
      this.nodeId = nodeId;
      this.hostname = hostname;
      this.port = port;
    }
    int nodeId;
    String hostname;
    int port;
    public String asString() {
      return String.format("%d@%s:%d", nodeId, hostname, port);
    }
  }

  public CompletableFuture<Void> kafkaStarted() {
    String startedMessage;
    if (processRoles.isEmpty()) {
      startedMessage = "[KafkaServer id="+brokerId+"] started";
    } else {
      startedMessage = "Kafka Server started";
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    withLogConsumer(x->{
              if (future.isDone()) {
                return;
              }
              if (x.getUtf8String().contains(startedMessage)) {
                future.complete(null);
              }
            }
    );
    return future;
  }

}
