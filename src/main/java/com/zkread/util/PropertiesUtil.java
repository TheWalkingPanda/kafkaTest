package com.zkread.util;

import java.util.ResourceBundle;

public class PropertiesUtil {
	private static ResourceBundle rb = ResourceBundle.getBundle("properties/kafka");
	
	public static String getProperty(String key){
		if(!rb.containsKey(key)){
			return null;
		}
		String value = rb.getString(key);
		if(value!=null && value.trim().length()>0){
			return value.trim();
		}
		return null;
	}
}
