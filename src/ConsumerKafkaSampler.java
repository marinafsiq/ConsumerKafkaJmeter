import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaSampler extends AbstractJavaSamplerClient {
    KafkaConsumer<String, String> consumer;

    @Override
    public void setupTest(JavaSamplerContext context) {
        super.setupTest(context);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-chalala");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        String topic = "first_topic";
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        boolean success = true;

        StringBuilder stringBuilder = new StringBuilder();

        result.sampleStart();
        ConsumerRecords<String, String> records = consumer.poll(100);


        for (ConsumerRecord<String, String> record : records) {
            //p.println("offset = " + record.offset() + " value = " + record.value());
            stringBuilder.append(record.value());
            System.out.println("OIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
            System.out.println("offset = " + record.offset() + " value = " + record.value());
        }
        consumer.commitSync();

        result.sampleEnd();
        result.setSuccessful(success);
        result.setResponseCodeOK();
        result.setResponseData(stringBuilder.toString(), "UTF-8");
        return result;



        /*FileOutputStream f;
        PrintStream p;
        try {
            f = new FileOutputStream("//home//marina//Downloads//apache-jmeter-5.0/data-marina.csv", true);
            p = new PrintStream(f);


            //while (System.currentTimeMillis() < end) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                p.println("offset = " + record.offset() + " value = " + record.value());
                System.out.println("OIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
                System.out.println("offset = " + record.offset() + " value = " + record.value());
            }
            consumer.commitSync();
            //}
            consumer.close();
            p.close();
            f.close();
        } catch (Exception e) {
            e.printStackTrace();

        }*/
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        super.teardownTest(context);
        consumer.close();
    }
}
