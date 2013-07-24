
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
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraIO {

	Logger logger = LoggerFactory.getLogger(CassandraIO.class);
	
	Connection conn = null;
	Map<ByteBuffer, Map<String, List<Mutation>>> rowDefs = new TreeMap<ByteBuffer, Map<String, List<Mutation>>>();

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

	public <T> CassandraIO store(List<T> rows) {
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

			if (rowDefs.containsKey(keyBuffer)) {
				if (rowDefs.get(keyBuffer).containsKey(cls.getSimpleName())) {
					rowDefs.get(keyBuffer).put(cls.getSimpleName(), mutations);
					Preconditions.checkNotNull(rowDefs.get(keyBuffer).get(cls.getSimpleName()));
				} else {
					Map<String, List<Mutation>> tmpMap = new HashMap<String, List<Mutation>>();
					tmpMap.put(cls.getSimpleName(), mutations);
					rowDefs.put(keyBuffer, tmpMap);
					Preconditions.checkNotNull(rowDefs.get(keyBuffer).get(cls.getSimpleName()));
				}
			} else {
				Map<String, List<Mutation>> tmp = new HashMap<String, List<Mutation>>();
				tmp.put(cls.getSimpleName(), mutations);
				rowDefs.put(keyBuffer, tmp);
				Preconditions.checkNotNull(rowDefs.get(keyBuffer));
				Preconditions.checkNotNull(rowDefs.get(keyBuffer).get(cls.getSimpleName()));
			}
		} // end for <T> rows
		// recover access privileges
		for (int i = 0; i < accessFlags.size(); i++) {
			valueFields.get(i).setAccessible(accessFlags.get(i));
		}
		keyField.setAccessible(keyAccessFlag);
		return this;
	}

	public <T> CassandraIO store(T row) {
		return store(Arrays.asList(row));
	}

	public <T> CassandraIO store(T... elems) {
		return store(Lists.newArrayList(elems));
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

	public void flush() throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		conn.getClient().batch_mutate(rowDefs, ConsistencyLevel.QUORUM);
		rowDefs.clear();
	}

	public void print() {
		for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> rowEnts : rowDefs.entrySet()) {
			System.out.println(new String(rowEnts.getKey().array()));
			Map<String, List<Mutation>> ks = rowDefs.get(rowEnts.getKey());
//			System.out.println(ks);
			for (Map.Entry<String, List<Mutation>> ksEnts : ks.entrySet()) {
				System.out.println("\t" + ksEnts.getKey());
				for (Mutation m : ksEnts.getValue()) {
					System.out.println("\t\t" + 
						new String(m.getColumn_or_supercolumn().getColumn().bufferForName().array()));
					System.out.println("\t\t" +
						m.getColumn_or_supercolumn().getColumn().bufferForValue().getInt(0));
				}
			}
		}
	}

}
