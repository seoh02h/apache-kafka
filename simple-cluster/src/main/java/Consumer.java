import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

  public static void main(String[] args) {
    Properties configs = new Properties();

    String topic = "testtopic";
    String groupId = "testgroup";

    // 환경 변수 설정

    // kafka server host 및 port
    configs.put("bootstrap.servers", "localhost:9092");

    // session
    configs.put("session.timeout.ms", "10000");

    // group
    configs.put("group.id", groupId);

    // deserializer
    configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    // consumer 생성
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

    // topic 설정 (미리 만들어둔 topic)
    consumer.subscribe(Arrays.asList(topic));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(500);
      for (ConsumerRecord<String, String> record : records) {
        String s = record.topic();
        if (topic.equals(s)) {
          System.out.println(record.value());
        } else {
          throw new IllegalStateException("get message on topic " + record.topic());
        }
      }
    }
  }

}