package com.sino.dsl;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * 演示 Tumbling time windows【滚动窗口】
 * 翻滚时间窗口Tumbling time windows是跳跃时间窗口hopping time windows的一种特殊情况，
 * 与后者一样，翻滚时间窗也是基于时间间隔的。但它是固定大小、不重叠、无间隙的窗口。
 * 翻滚窗口只由一个属性定义：size。翻滚窗口实际上是一种跳跃窗口，其窗口大小与其前进间隔相等。
 * 由于翻滚窗口从不重叠，数据记录将只属于一个窗口。
 * 
 * @author Administrator
 *
 */
//@Component
public class TumblingWinStreamService {
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	
	private final static String TOPIC="topic02";
	
	/**
	 * 消息生产者
	 */
	@Scheduled(cron = "00/1 * * * * ?")
	public void send() {
		String[] message = new String[] { "this is a demo", "hello world", "hello boy" };
		ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC,
				message[new Random().nextInt(message.length)]);
		future.addCallback(o -> System.out.println("send-消息发送成功：" + o.getProducerRecord().value()),
				throwable -> System.out.println("消息发送失败：" + message.toString()));
	}

	/**
	 * 消息消费者
	 * 
	 * @param record
	 */
//	@KafkaListener(topics = "topic02", id = "g1")
	public void processMessage(ConsumerRecord<?, ?> record, Acknowledgment ack) {
		System.out.println("recive-消息接收:" + record.value());
		ack.acknowledge();
	}
	
    
	@PostConstruct
	private void start() {
		
		final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        
        builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
			.flatMapValues((String key, String value) -> {
				List<String> result = new ArrayList<>();
				String[] tokens = value.split("\\W+");
				for (String token : tokens) {
					result.add(token);
				}
				return result;
			})
			.selectKey((key, value) -> value)
			.mapValues((v) -> 1)
			.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
			.windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
			.reduce((v1, v2) -> v1 + v2,
					Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("tumbling-word-count")
							.withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer()))
			.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), Suppressed.BufferConfig.unbounded()))
			.toStream().peek((k, v) -> {
				SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
				Window window = k.window();
				String start = sdf.format(window.start());
				String end = sdf.format(window.end());
				System.out.println(start + " - " + end + "\t" + k.key() + ":" + v);
			});
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
	
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
}
