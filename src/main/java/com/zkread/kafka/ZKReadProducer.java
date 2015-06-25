package com.zkread.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.zkread.util.IOUtil;
import com.zkread.util.PropertiesUtil;

public class ZKReadProducer {
	//申明生产者：泛型1为分区key类型，泛型2为消息类型
	private Producer<Integer, Object> producer;
	
	public ZKReadProducer(){
		Properties props = new Properties();
		
		//指定采用哪种序列化方式将消息传输给Broker,也可以在发送消息的时候指定序列化类型，不指定则以此为默认序列化类型
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		//指定kafka节点：无需指定集群中所有Broker，只要指定其中部分即可，它会自动取meta信息并连接到对应的Broker节点
		props.put("metadata.broker.list", PropertiesUtil.getProperty("metadata.broker.list"));
		
		//指定消息发送对应分区方式，若不指定，则随机发送到一个分区，也可以在发送消息的时候指定分区类型。
//		config.put("partitioner.class", "example.producer.SimplePartitioner");
		
		//该属性表示你需要在消息被接收到的时候发送ack给发送者。以保证数据不丢失
//		config.put("request.required.acks", "1");
		
		ProducerConfig producerConfig = new ProducerConfig(props);
		this.producer = new Producer<Integer, Object>(producerConfig);
	}
	
	public boolean sendMsg(String topic, Object msg){
		try {
			/*创建KeyedMessage发送消息，参数1[String]为topic名，参数2[K]为分区名（若为null则随机发到一个分区），参数3[V]为消息
				三种构造方法：1.new KeyedMessage(String, K, Object, V)
							 2.new KeyedMessage(String, V)
							 3.new KeyedMessage(String, K, V)
				泛型1[K]为分区key类型，泛型2[V]为消息类型
			*/
			KeyedMessage<Integer, Object> message = new KeyedMessage<Integer, Object>(topic, msg);
			this.producer.send(message);
			this.producer.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		while(true){
			Scanner scanner = new Scanner(System.in);
			System.out.println("请输入消息:");
			String msg = scanner.nextLine();
			
			ZKReadProducer client = new ZKReadProducer();
			Map<String,String> message = new HashMap<String, String>();
			message.put("key", "val");
			client.sendMsg("test", IOUtil.toByteArray(message));
		}
	}
}
