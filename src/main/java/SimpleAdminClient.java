import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SimpleAdminClient {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "");
        AdminClient admin = KafkaAdminClient.create(props);

        int iterations = 100;

        Map<Integer, Integer> histogram = new HashMap<>();

        while (iterations > 0) {

            String uuid = UUID.randomUUID().toString();

            NewTopic topic = new NewTopic(uuid, 1, (short) 1);

            admin.createTopics(Collections.singletonList(topic)).all().get();

            int counter = 0;

            while (true) {
                if (!admin.listTopics().names().get().contains(topic.name())) {
                    counter++;
                    Thread.sleep(50);
                } else {
                    break;
                }
            }

            Integer prev = histogram.getOrDefault(counter * 50, 0);
            histogram.put(counter * 50, prev + 1);

            iterations--;
        }

        System.err.println(histogram);
    }

}
