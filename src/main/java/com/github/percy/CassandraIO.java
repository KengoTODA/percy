
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
import java.util.logging.Level;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraIO {

	Logger logger = LoggerFactory.getLogger(CassandraIO.class);
	
	Connection conn = null;
	Map<ByteBuffer, Map<String, List<Mutation>>> rowDefs = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

	public CassandraIO() {
	}

	public CassandraIO(Connection conn) {
		this.conn = conn;
	}

	public CassandraIO setConnection(Connection conn) {
		this.conn = conn;
		return this;
	}

	public Connection getConnection() {
		return conn;
	}

	public <T> void store(List<T> rows) {
		if (rows.isEmpty()) {
			return;
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
		// key          cf           column-name-value
		// long timeStamp = System.currentTimeMillis();
		for (T row : rows) {
			List<Mutation> mutations = Lists.newArrayList();
			ByteBuffer keyBuffer = null;
			try {
				if (keyField.getType()== int.class) {
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
					keyBuffer =Utils.getByteBufffer(keyField.get(row));
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
					if (f.getType()== int.class) {
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
						valueBuffer =Utils.getByteBufffer(f.get(row));
					}
					Preconditions.checkNotNull(valueBuffer);
					org.apache.cassandra.thrift.Column column;
					column = new org.apache.cassandra.thrift.Column()
									.setName(ByteBuffer.wrap(f.getName().getBytes("UTF-8")))
									.setValue(valueBuffer).setTimestamp(0);
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
			if (rowDefs.containsKey(keyBuffer)) {
				if (rowDefs.get(keyBuffer).containsKey(cls.getSimpleName())) {
					rowDefs.get(keyBuffer).put(cls.getSimpleName(), mutations);
				} else {
					
				}
			} else {
				rowDefs.put(keyBuffer, (Map<String, List<Mutation>>)(new HashMap<String, List<Mutation>>().put(cls.getSimpleName(), mutations)));
			}
		} // end for <T> rows
		// recover access privileges
		for (int i = 0; i < accessFlags.size(); i++) {
			valueFields.get(i).setAccessible(accessFlags.get(i));
		}
		keyField.setAccessible(keyAccessFlag);
	}

	public <T> void store(T row) {
		store(Arrays.asList(row));
	}

	public <T> void store(T... elems) {
		store(Lists.newArrayList(elems));
	}

	public <T, K> T load(K key) {
		return null;
	}

	public <T, K> List<T> load(List<K> key, Class cls) {
		return null;
	}

	public void clear() {
		rowDefs.clear();
	}

	public void flush() {
		
	}

}
