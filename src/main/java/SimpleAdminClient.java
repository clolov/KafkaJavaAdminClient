import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleAdminClient {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        AdminClient admin = KafkaAdminClient.create(props);

        NewTopic topic = new NewTopic("batman", 1, (short) 1);

        System.err.println("Creating the topic");
        admin.createTopics(Collections.singletonList(topic)).all().get();

        System.err.println("Trying to list the topic");
        int counter = 0;
        while (true) {
            if (!admin.listTopics().names().get().contains(topic.name())) {
                System.err.println("Could not find topic after " + counter * 50 + " ms");
                counter++;
                Thread.sleep(50);
            } else {
                System.err.println("Found the topic");
                break;
            }
        }

        System.err.println("Completed");
    }

}
