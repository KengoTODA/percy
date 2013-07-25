package com.github.percy;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBarn {

	Logger logger = LoggerFactory.getLogger(CassandraIO.class);

	private Map<ByteBuffer, Map<Class, List<Mutation>>> pushBuffer;
	private Map<ByteBuffer, Map<Class, List<Mutation>>> fetchBuffer;


	public DataBarn() {
		pushBuffer = new TreeMap<ByteBuffer, Map<Class, List<Mutation>>>();
	}

	public <T> DataBarn	store(List<T> rows) {
		if (rows.isEmpty()) {
			return this;
		}
		Class cls = rows.get(0).getClass();
		Pair<Field, List<Field>> kvField = Utils.getKeyValueFields(cls);
		Field keyField = kvField.left;
		List<Field> valueFields = kvField.right;
		List<Boolean> accessFlags = Lists.transform(valueFields, new Function<Field, Boolean>() {
			@Override
			public Boolean apply(Field f) {
				return f.isAccessible();
			}
		});
		for (Field f : valueFields) {
			f.setAccessible(true);
		}
		boolean keyAccessFlag = keyField.isAccessible();
		keyField.setAccessible(true);
		for (T row : rows) {
			List<Mutation> mutations = Lists.newArrayList();
			ByteBuffer keyBuffer = null;
			try {
				if (keyField.getType() == int.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getInt(row));
				} else if (keyField.getType() == long.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getLong(row));
				} else if (keyField.getType() == float.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getFloat(row));
				} else if (keyField.getType() == double.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getDouble(row));
				} else if (keyField.getType() == boolean.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getBoolean(row));
				} else if (keyField.getType() == char.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getChar(row));
				} else if (keyField.getType() == byte.class) {
					keyBuffer = Utils.getByteBufffer(keyField.getByte(row));
				} else {
					keyBuffer = Utils.getByteBufffer(keyField.get(row));
				}
			} catch (IllegalArgumentException e) {
				logger.error("Please check type of the @Key fields.");
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				logger.error("Bug in source code.");
				throw new RuntimeException(e);
			}
			Preconditions.checkNotNull(keyField);

			for (Field f : valueFields) {
				try {
					ByteBuffer valueBuffer = null;
					if (f.getType() == int.class) {
						valueBuffer = Utils.getByteBufffer(f.getInt(row));
					} else if (f.getType() == long.class) {
						valueBuffer = Utils.getByteBufffer(f.getLong(row));
					} else if (f.getType() == float.class) {
						valueBuffer = Utils.getByteBufffer(f.getFloat(row));
					} else if (f.getType() == double.class) {
						valueBuffer = Utils.getByteBufffer(f.getDouble(row));
					} else if (f.getType() == boolean.class) {
						valueBuffer = Utils.getByteBufffer(f.getBoolean(row));
					} else if (f.getType() == char.class) {
						valueBuffer = Utils.getByteBufffer(f.getChar(row));
					} else if (f.getType() == byte.class) {
						valueBuffer = Utils.getByteBufffer(f.getByte(row));
					} else {
						valueBuffer = Utils.getByteBufffer(f.get(row));
					}
					Preconditions.checkNotNull(valueBuffer);
					org.apache.cassandra.thrift.Column column;
					column = new org.apache.cassandra.thrift.Column()
							.setName(ByteBuffer.wrap(f.getName().getBytes("UTF-8")))
							.setValue(valueBuffer).setTimestamp(System.currentTimeMillis());
					mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column)));
				} catch (IllegalArgumentException e) {
					logger.error("Please check type of the @Column fields.");
					throw new RuntimeException(e);
				} catch (IllegalAccessException e) {
					logger.error("Bug in source code.");
					throw new RuntimeException(e);
				} catch (UnsupportedEncodingException e) {
					logger.error("Bug in source code.");
					throw new RuntimeException(e);
				}
			} // end for valueFields

			if (pushBuffer.containsKey(keyBuffer)) {
				if (pushBuffer.get(keyBuffer).containsKey(cls.getSimpleName())) {
					pushBuffer.get(keyBuffer).put(cls, mutations);
					Preconditions.checkNotNull(pushBuffer.get(keyBuffer).get(cls.getSimpleName()));
				} else {
					Map<Class, List<Mutation>> tmpMap = new HashMap<Class, List<Mutation>>();
					tmpMap.put(cls, mutations);
					pushBuffer.put(keyBuffer, tmpMap);
					Preconditions.checkNotNull(pushBuffer.get(keyBuffer).get(cls.getSimpleName()));
				}
			} else {
				Map<Class, List<Mutation>> tmp = new HashMap<Class, List<Mutation>>();
				tmp.put(cls, mutations);
				pushBuffer.put(keyBuffer, tmp);
				Preconditions.checkNotNull(pushBuffer.get(keyBuffer));
				Preconditions.checkNotNull(pushBuffer.get(keyBuffer).get(cls));
			}
		} // end for <T> rows
		// recover access privileges
		for (int i = 0; i < accessFlags.size(); i++) {
			valueFields.get(i).setAccessible(accessFlags.get(i));
		}
		keyField.setAccessible(keyAccessFlag);
		return this;
	}

	public <T> DataBarn store(T row) {
		return store(Arrays.asList(row));
	}

	public <T> DataBarn store(T... elems) {
		return store(Lists.newArrayList(elems));
	}


	public DataBarn clear() {
		pushBuffer.clear();
		return this;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		for (Map.Entry<ByteBuffer, Map<Class, List<Mutation>>> rowEnts : pushBuffer.entrySet()) {
			output.append(new String(rowEnts.getKey().array()) + "\n");
			Map<Class, List<Mutation>> ks = pushBuffer.get(rowEnts.getKey());
			for (Map.Entry<Class, List<Mutation>> ksEnts : ks.entrySet()) {
				output.append("\t" + ksEnts.getKey().getSimpleName() + "\n");
				for (Mutation m : ksEnts.getValue()) {
					output.append("\t\t" + new String(m.getColumn_or_supercolumn().getColumn().bufferForName().array()) + "  :  ");
					output.append(m.getColumn_or_supercolumn().getColumn().bufferForValue().getInt(0) + "\n");
				}
			}
		}
		return output.toString();
	}
}
