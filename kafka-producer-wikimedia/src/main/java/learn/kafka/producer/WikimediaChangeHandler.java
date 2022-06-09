package learn.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String, String> kafkaProducer; String topic;

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());


    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.close(); //flushing and closing
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        String value = messageEvent.getData();
        //async send
        kafkaProducer.send(new ProducerRecord<>(topic, value));
    }

    @Override
    public void onComment(String comment) throws Exception {
        //nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading");
    }
}
