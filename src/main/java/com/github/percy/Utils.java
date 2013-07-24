package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

	public static Pair<Field, List<Field>> getKeyValueFields(Class cls) {
		return validateColumnFamilyClass(cls);
	}
	
	public static ByteBuffer getByteBufffer(Object obj) {
		if (obj.getClass() == Integer.class) {
			System.out.println("Integer");
			return ByteBuffer.allocate(4).putInt(((Integer)obj).intValue());
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
		} else if (obj.getClass() == String.class) {
			try {
				return ByteBuffer.wrap(((String)obj).getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Bug in source code.\n" + e.getMessage());
			}
		}
		throw new IllegalArgumentException("Unsupported object type.");
		// TODO : get byte buffer by triggering specific methods
	}

	public static ByteBuffer getByteBufffer(int i) {
		System.out.println("int");
		return ByteBuffer.allocate(4).putInt(i);
	}

	public static ByteBuffer getByteBufffer(long l) {
		return ByteBuffer.allocate(8).putLong(l);
	}

	public static ByteBuffer getByteBufffer(float f) {
		return ByteBuffer.allocate(4).putFloat(f);
	}

	public static ByteBuffer getByteBufffer(double d) {
		return ByteBuffer.allocate(8).putDouble(d);
	}

	public static ByteBuffer getByteBufffer(boolean b) {
		if (b == false) {
			return ByteBuffer.allocate(1).put((byte)0);
		}
		return ByteBuffer.allocate(1).put((byte)1);
	}

	public static ByteBuffer getByteBufffer(char c) {
		return ByteBuffer.allocate(2).putChar(c);
	}

	public static ByteBuffer getByteBufffer(byte b) {
		return ByteBuffer.allocate(1).put(b);
	}

	private static Pair<Field, List<Field>> validateColumnFamilyClass(Class cls) {
		if (!cls.isAnnotationPresent(ColumnFamily.class)) {
			throw new IllegalArgumentException(cls.getName() + " : should be annotated with com.github.percy.annotations.ColumnFamily");
		}

		Field keyField = null;
		List<Field> columnFields = new ArrayList<Field>();
		List<Field> fields = getAllFields(cls);
		int keyFieldCnt = 0;

		for (Field f : fields) {
			if (f.isAnnotationPresent(Key.class)) {
				keyFieldCnt++;
				keyField = f;
			} else if (f.isAnnotationPresent(Column.class)) {
				columnFields.add(f);
			}
		}
		if (keyFieldCnt != 1) {
			throw new IllegalArgumentException(cls.getName() + " : only 1 @Key can be defined in a column family. " + String.valueOf(keyFieldCnt) + " found.");
		}
		if (columnFields.isEmpty()) {
			throw new IllegalArgumentException(cls.getName() + " : at least 1 @Column should be defined in a column family");
		}
		return Pair.create(keyField, columnFields);
	}
}
