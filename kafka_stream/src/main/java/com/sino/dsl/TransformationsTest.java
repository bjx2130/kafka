package com.sino.dsl;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * 
 * 使用main方法启动
 * 演示kafka-stream的基本使用 【无状态 和 有状态转换】
 * 
 * @author Administrator
 *
 */
public class TransformationsTest {
    public static final String INPUT_TOPIC = "streams-Stateless-input";
    public static final String OUTPUT_TOPIC = "streams-Stateless-output";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-Stateless");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.0.193:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return props;
    }


    public static void main(final String[] args) throws InterruptedException {
        final Properties props = getStreamsConfig();
        
        final StreamsBuilder builder = new StreamsBuilder();
        
        //从topic读取数据
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
        			//[无状态]peek 可当做 logging or tracking 使用
        			.peek((key, value) -> System.out.println("1、原始消息 "+ value))
        			//[无状态]分配一个key
        			.selectKey((key, value) -> value.split(" ")[0])
        			.peek((key, value) -> System.out.println("2、分配一个 key=" + key + ", value=" + value))
        			//[无状态]过滤数据
        			.filter((k,v)->v.startsWith("a"))
        			//[无状态]转换数据：将value分割
        			.flatMapValues(value -> Arrays.asList(((String) value).toLowerCase(Locale.getDefault()).split(" ")))
        			.peek((key, value) -> System.out.println("3、value分割后  key=" + key + ", value=" + value))
        			//[有状态]通过value分组,转换成KGroupedStream<String, String> 
        			.groupBy((key, value) -> value)
        			//[有状态]统计
        			.count()
        			//KTable<String, Long>转换成KStream<String, Long>
        			.toStream()
        			//输出 OUTPUT_TOPIC
        			.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        
		//创建KafkaStreams 实例
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        startKafkaStreams(streams);
        
        
    }
    
    
    
    static void startKafkaStreams(KafkaStreams streams) throws InterruptedException {
    	// attach shutdown handler to catch control-c
    	final CountDownLatch latch = new CountDownLatch(1);
    	
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
        	//5、启动任务
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    
    
    
    
    
}
