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
 * 演示 Hopping time windows【滑动窗口 或 跳跃时间窗口】
 * 跳跃时间窗口Hopping time windows是基于时间间隔的窗口。它们为固定大小(可能)重叠的窗口建模。
 * 跳跃窗口由两个属性定义:【窗口的size】及其【前进间隔advance interval】 (也称为hop)。
 * 前进间隔指定一个窗口相对于前一个窗口向前移动多少。
 * 例如，您可以配置一个size为5分钟、advance为1分钟的跳转窗口。
 * 由于跳跃窗口可以重叠(通常情况下确实如此)，数据记录可能属于多个这样的窗口。
 * 
 * @author Administrator
 *
 */
@Component
public class HoppingWinStreamService {
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	
	private final static String TOPIC="topic_hop";
	private static final long TIME_WINDOW_SECONDS = 5L; //窗口大小设为5秒
	private static final long ADVANCED_BY_SECONDS = 1L; //前进间隔1秒
	
	
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
		        .windowedBy(TimeWindows.of(Duration.ofSeconds(TIME_WINDOW_SECONDS))
		                    .advanceBy(Duration.ofSeconds(ADVANCED_BY_SECONDS))
		                    //设立一个数据晚到的期限，这个期限过了之后时间窗口才关闭
		                    .grace(Duration.ZERO))
		        .count(Materialized.with(Serdes.String(), Serdes.Long()))
		        //抑制住上游流的输出，直到当前时间窗口关闭后，才向下游发送数据。前面我们说过，每当统计值产生变化时，统计的结果会立即发送给下游
		        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
		        .toStream().peek((k, v) -> {
					SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
					Window window = k.window();
					String start = sdf.format(window.start());
					String end = sdf.format(window.end());
					System.out.println(start + " - " + end + "\t" + k.key() + ":" + v);
				});;
		
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
