import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallbackLoop {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallbackLoop.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is a producer with callback");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++){
            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i*i);

            //sending the data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("record sent successfully. : metadata - ");
                        log.info("topic:" + metadata.topic());
                        log.info("partition:" + metadata.partition());
                        log.info("offset:" + metadata.offset());
                    } else {
                        log.info("record not sent.");
                    }
                }
            });
            try {
                Thread.sleep(500);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        //flush - synchronous
        producer.flush();

        //OR just flush and close
        producer.close();
    }
}
