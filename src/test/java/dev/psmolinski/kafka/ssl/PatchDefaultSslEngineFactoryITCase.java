package dev.psmolinski.kafka.ssl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PatchDefaultSslEngineFactoryITCase {

    private static Network network;
    private static KafkaContainer<?> kafka;
    private static GenericContainer<?> java;

    @BeforeAll
    public static void beforeAll() throws Exception {
        network = Network.newNetwork();
        kafka = KafkaContainer.cpServer("7.0.1")

                .withBrokerId(0)

                // this turns on KRaft
                .withProcessRoles("controller", "broker")
                .withNodeId(0)

                // KRaft network settings
                .withControllerQuorumVoter(0, "kafka", 9089)
                .withListener("CONTROL", 9089, false, "kafka", "PLAINTEXT", Collections.emptyMap())
                .withControllerListenerNames("CONTROL")

                .withListener("BROKER", 9090, false, "kafka", "PLAINTEXT", Collections.emptyMap())
                .withInterBrokerListener("BROKER")

                .withListener("CLIENT", 9091, false, "kafka", "SSL",
                        MapBuilder.empty(String.class, String.class)
                                .with("ssl.keystore.location", "/etc/kafka/certificates/kafka.jks")
                                .with("ssl.keystore.type", "PKCS12")
                                .with("ssl.keystore.password", "changeit")
                                .with("ssl.key.password", "changeit")
                                .with("ssl.truststore.location", "/etc/kafka/certificates/ca.jks")
                                .with("ssl.truststore.type", "PKCS12")
                                .with("ssl.truststore.password", "changeit")
                                .build())

                // mumbo-jumbo unsupported by CP7 in KRaft mode
                .withConfluentClusterLinkEnable(false)
                .withConfluentBalancerEnable(false)

                // get rid of server-side schema validation warning
                .withSchemaRegistryUrl("https://registry:8081/")

                .withNetwork(network)
                .withNetworkAliases("kafka")

                .withFileSystemBind("src/test/certificates", "/etc/kafka/certificates");

        java = new GenericContainer<>(DockerImageName.parse("openjdk:8"))
                .withNetwork(network)
                .withFileSystemBind("src/test/certificates", "/app/certificates")
                .withFileSystemBind("target/lib", "/app/lib")
                .withFileSystemBind("target/test-classes", "/app/classes")
                .withFileSystemBind("target/kafka-ssl-fix.jar", "/app/lib/kafka-ssl-fix.jar")
                .withCommand("sleep", "inf");

        CompletableFuture<Void> kafkaStarted = kafka.kafkaStarted();

        kafka.start();

        kafkaStarted.get(10, TimeUnit.MINUTES);

        java.start();

    }

    @AfterAll
    public static void afterAll() {
        if (java!=null) {
            java.close();
            java = null;
        }
        if (kafka!=null) {
            kafka.close();
            kafka = null;
        }
        if (network!=null) {
            network.close();
        }
    }

    @Test
    public void testAgentPEM() throws Exception {

        Container.ExecResult result = java.execInContainer("java", "-javaagent:/app/lib/kafka-ssl-fix.jar", "-cp", "/app/lib/*:/app/classes", "dev.psmolinski.kafka.ssl.PatchDefaultSslEngineFactoryExample$Test1");

        Assertions.assertThat(result.getExitCode()).isEqualTo(0);

    }

    @Test
    public void testAgentPKCS12() throws Exception {

        Container.ExecResult result = java.execInContainer("java", "-javaagent:/app/lib/kafka-ssl-fix.jar", "-cp", "/app/lib/*:/app/classes", "dev.psmolinski.kafka.ssl.PatchDefaultSslEngineFactoryExample$Test2");

        Assertions.assertThat(result.getExitCode()).isEqualTo(0);

    }

}
