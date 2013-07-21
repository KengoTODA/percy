package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.Key;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Pair;

public class Utils {

	private static Map<Class, Class> vcMap = null;

	static {
		vcMap = new HashMap<Class, Class>();
		vcMap.put(int.class, Int32Type.class);
		vcMap.put(long.class, LongType.class);
		vcMap.put(float.class, FloatType.class);
		vcMap.put(double.class, DoubleType.class);
		vcMap.put(boolean.class, BooleanType.class);
		vcMap.put(char.class, BytesType.class);
		vcMap.put(byte.class, BytesType.class);

		vcMap.put(Integer.class, Int32Type.class);
		vcMap.put(Long.class, LongType.class);
		vcMap.put(Float.class, FloatType.class);
		vcMap.put(Double.class, DoubleType.class);
		vcMap.put(Boolean.class, BooleanType.class);
		vcMap.put(Character.class, BytesType.class);
		vcMap.put(Byte.class, BytesType.class);

		vcMap.put(Date.class, DateType.class);
		vcMap.put(String.class, UTF8Type.class);
	}

	public static Class getValidationClass(Class cls) {
		return vcMap.get(cls) == null ? BytesType.class : vcMap.get(cls);
	}

	public static String getValidationClassString(Class cls) {
		return getValidationClass(cls).getSimpleName();
	}

	public static List<Field> getAllFields(final Class cls) {
		if (cls.getSuperclass() == Object.class) {
			return Arrays.asList(cls.getDeclaredFields());
		}
		List<Field> ret = Lists.newArrayList();
		ret.addAll(Arrays.asList(cls.getDeclaredFields()));
		ret.addAll(getAllFields(cls.getSuperclass()));
		return ret;
	}

	public static Predicate<Field> getAnnotationPredicate(final Class cls) {
		if (!cls.isAnnotation()) {
			throw new IllegalArgumentException(cls.getName() + " is not an annotation");
		}
		return new Predicate<Field>() {
			@Override
			public boolean apply(Field input) {
				return input.isAnnotationPresent(cls);
			}
		};
	}

	public static ByteBuffer getValueByteBufffer(Object obj) {
		if (obj.getClass() == Integer.class) {
			System.out.println("Integer");
			return ByteBuffer.allocate(4).putInt(((Integer) obj).intValue());
		} else if (obj.getClass() == Long.class) {
			return ByteBuffer.allocate(8).putLong(((Long)obj).longValue());
		} else if (obj.getClass() == Float.class) {
			return ByteBuffer.allocate(4).putFloat(((Float) obj).floatValue());
		} else if (obj.getClass() == Double.class) {
			return ByteBuffer.allocate(8).putDouble(((Double) obj).doubleValue());
		} else if (obj.getClass() == Boolean.class) {
			if (((Boolean)obj).booleanValue() == false) {
				return ByteBuffer.allocate(1).put((byte)0);
			}
			return ByteBuffer.allocate(1).put((byte)1);
		} else if (obj.getClass() == Character.class) {
			return ByteBuffer.allocate(2).putChar(((Character)obj).charValue());
		} else if (obj.getClass() == Byte.class) {
			return ByteBuffer.allocate(1).put(((Byte)obj).byteValue());
		} else if (obj.getClass() == Date.class) {
			return ByteBuffer.allocate(8).putLong(((Date)obj).getTime());
		}
		throw new IllegalArgumentException("Unsupported object type.");
	}

	public static Pair<Field, List<Field>> getKeyValueFields(Class cls) {
		Field keyField = null;
		List<Field> valuesFields = Lists.newArrayList();
		List<Field> fields = getAllFields(cls);
		for (Field f : fields) {
			if (f.isAnnotationPresent(Key.class)) {
				keyField = f;
			} else if (f.isAnnotationPresent(Column.class)) {
				valuesFields.add(f);
			}
		}
		return Pair.create(keyField, valuesFields);
	}

	public static ByteBuffer getValueByteBufffer(int i) {
		System.out.println("int");
		return ByteBuffer.allocate(4).putInt(i);
	}

	public static ByteBuffer getValueByteBufffer(long l) {
		System.out.println("long");
		return ByteBuffer.allocate(8).putLong(l);
	}

	public static ByteBuffer getValueByteBufffer(float f) {
		return ByteBuffer.allocate(4).putFloat(f);
	}

	public static ByteBuffer getValueByteBufffer(double d) {
		return ByteBuffer.allocate(8).putDouble(d);
	}

	public static ByteBuffer getValueByteBufffer(boolean b) {
		if (b == false) {
			return ByteBuffer.allocate(1).put((byte)0);
		}
		return ByteBuffer.allocate(1).put((byte)1);
	}

	public static ByteBuffer getValueByteBufffer(char c) {
		return ByteBuffer.allocate(2).putChar(c);
	}

	public static ByteBuffer getValueByteBufffer(byte b) {
		return ByteBuffer.allocate(1).put(b);
	}

	public static ByteBuffer getValueByteBufffer(String s) {
		try {
			return ByteBuffer.wrap(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Bug in source code.\n" + e.getMessage());
		}
	}


}
