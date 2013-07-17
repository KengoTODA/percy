
package com.github.percy;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.db.marshal.*;

public class ValidationClassChooser {

	private static Map<Class, Class> vcMap = null;

	static {
		vcMap = new HashMap<Class, Class>();
		vcMap.put(int.class, Int32Type.class);
		vcMap.put(long.class, LongType.class);
		vcMap.put(double.class, DoubleType.class);
		vcMap.put(float.class, FloatType.class);
		vcMap.put(boolean.class, BooleanType.class);
		vcMap.put(char.class, BytesType.class);
		vcMap.put(byte[].class, BytesType.class);
		vcMap.put(String.class, UTF8Type.class);
		vcMap.put(Integer.class, Int32Type.class);
		vcMap.put(Long.class, LongType.class);
		vcMap.put(Boolean.class, BooleanType.class);
		vcMap.put(Date.class, DateType.class);
	}

	public static Map<Class, Class> getMap() {
		return vcMap;
	}

	public static Class getValidationClass(Class cls) {
		return vcMap.get(cls);
	}

	public static String getValidationClassString(Class cls) {
		return vcMap.get(cls).getSimpleName();
	}


}
