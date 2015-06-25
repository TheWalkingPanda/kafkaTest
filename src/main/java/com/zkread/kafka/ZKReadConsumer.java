package com.zkread.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.zkread.util.IOUtil;
import com.zkread.util.PropertiesUtil;

public class ZKReadConsumer {
	private ConsumerConnector consumer;
	
	public ZKReadConsumer(){
		Properties props = new Properties();
		//指定zookeeper服务器地址
		props.put("zookeeper.connect", 		   PropertiesUtil.getProperty("zookeeper.connect"));
		//指定消费组（没有它会自动添加）
		props.put("group.id", 					   PropertiesUtil.getProperty("group.id"));
		//指定kafka等待多久zookeeper回复（ms）以便放弃并继续消费。
		props.put("zookeeper.session.timeout.ms", PropertiesUtil.getProperty("zookeeper.session.timeout.ms"));
		//指定zookeeper同步最长延迟多久再产生异常
		props.put("zookeeper.sync.time.ms", 	   PropertiesUtil.getProperty("zookeeper.sync.time.ms"));
		/*	指定多久消费者更新offset到zookeeper中。
			注意offset更新时基于time而不是每次获得的消息。
			一旦在更新zookeeper发生异常并重启，将可能拿到已拿到过的消息
		*/
		props.put("auto.commit.interval.ms", 	   PropertiesUtil.getProperty("auto.commit.interval.ms"));
		
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
	}
	
	/**
	 * @param Map<String, Integer> topicCountMap：String--[topic]，Integer--[用多少个线程来处理该topic]
	 * @return 根据map获取所有的主题对应的消息流
	 */
	public Map<String, List<KafkaStream<byte[], byte[]>>> getTopicsKafkaStream(Map<String, Integer> topicCountMap){
		return this.consumer.createMessageStreams(topicCountMap);
	}
	
	/**
	 * @return 该主题的消息流
	 */
	public List<KafkaStream<byte[], byte[]>> getKafkaStream(String topic, int numThreads){
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = getTopicsKafkaStream(topicCountMap);
		return consumerMap.get(topic);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		ZKReadConsumer client = new ZKReadConsumer();
		List<KafkaStream<byte[], byte[]>> streamList = client.getKafkaStream("test", 1);
		ConsumerIterator<byte[], byte[]> iterator = streamList.get(0).iterator();
		while(iterator.hasNext()){
			Object receiveObj = IOUtil.toObject(iterator.next().message());
			Map<String, String> receiveMap = (Map<String, String>) receiveObj;
			System.out.println("接收："+receiveMap.get("key"));
		}
		System.out.println("the end");
	}
}
