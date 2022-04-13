import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

  public static void main(String[] args) throws IOException {

    String topic = "testtopic";

    Properties configs = new Properties();

    // 환경 변수 설정

    // kafka host 및 server 설정
    configs.put("bootstrap.servers", "localhost:9092");

    // ack
    configs.put("acks", "all");

    // buffer
    configs.put("block.on.buffer.full", "true");

    // serialize
    configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    // producer 생성
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

    for (int i = 0; i < 5; i++) {
      String v = "msg" + i;
      producer.send(new ProducerRecord<String, String>(topic, v));
    }

    // 종료
    producer.flush();
    producer.close();
  }

}