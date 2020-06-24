

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Producer {
    
    public static void main(String[] argv)throws Exception {
       
        String topicName = "test2";
       
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");

        org.apache.kafka.clients.producer.Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(configProperties);

        ObjectMapper objectMapper = new ObjectMapper();
        int i=20;
		System.out.println("SynchronousProducer Completed with success.");

        while(i>0) {
            Contact contact = new Contact();
            contact.setContactId(i);
            contact.setFirstName("Aparna"+String.valueOf(i));
            contact.setLastName("Bhat"+String.valueOf(i));
            JsonNode  jsonNode = objectMapper.valueToTree(contact);
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
        	try {
            RecordMetadata metadata = (RecordMetadata)producer.send(rec).get();
      //    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");

			System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset "
					+ metadata.offset());
			System.out.println("SynchronousProducer Completed with success.");

        	}
        	catch(Exception e) {
        		
        	}
        	i--;
        }
        
        producer.close();
    }
}