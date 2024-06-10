import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SimpleUnfinishedTransaction {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "");
        AdminClient admin = KafkaAdminClient.create(props);

        Properties producerProps = new Properties();
        producerProps.putAll(props);
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", StringSerializer.class);
        producerProps.put("transactional.timeout.ms", 60000);

        String transactionalUuid = UUID.randomUUID().toString();
        System.err.println("Transactional UUID: " + transactionalUuid);
        producerProps.put("transactional.id", transactionalUuid);
        Producer<String, String> producer = new KafkaProducer<>(producerProps);

        String uuid = UUID.randomUUID().toString();
        System.err.println("Topic name: " + uuid);

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

        producer.initTransactions();

        try {
            producer.beginTransaction();
            double randomDouble = Math.random();
            int randomNum = (int) (randomDouble * 10);
            producer.send(new ProducerRecord<>(topic.name(), Integer.toString(1), Integer.toString(randomNum)));
            System.err.printf("Sent %s:%s\n", 1, randomNum);
        } catch (KafkaException e) {
            System.err.println(e);
        }
    }
}
