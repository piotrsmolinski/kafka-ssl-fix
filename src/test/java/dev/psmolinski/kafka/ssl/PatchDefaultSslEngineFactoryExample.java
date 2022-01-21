package dev.psmolinski.kafka.ssl;

import org.apache.kafka.clients.admin.AdminClient;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PatchDefaultSslEngineFactoryExample {

    public static void main(String...args) throws Exception {

        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "kafka:9091");
        config.put("security.protocol", "SSL");
        config.put("ssl.truststore.location", "");
        config.put("ssl.truststore.type", "PEM");
        config.put("ssl.truststore.certificates", readFile("/app/certificates/ca.crt"));

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.listTopics();
        }

    }

    private static String readFile(String file) throws Exception {
        Writer writer = new StringWriter();
        Reader reader = new InputStreamReader(new FileInputStream(file));
        char[] buffer = new char[8192];
        for (;;) {
            int r = reader.read(buffer);
            if (r<0) break;
            writer.write(buffer, 0, r);
        }
        return writer.toString();
    }

}
