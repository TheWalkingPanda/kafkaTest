package com.zkread.kafka;

import kafka.producer.Partitioner;

//自定义分区：
public class SimplePartitioner implements Partitioner {

	public int partition(Object arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}
}
