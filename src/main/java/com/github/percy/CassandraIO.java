
package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.Key;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.lang.SerializationUtils;

public class CassandraIO {
	Connection conn = null;

	public CassandraIO() {
	}

	public CassandraIO setConnection(Connection conn) {
		this.conn = conn;
		return this;
	}

	public Connection getConnection() {
		return conn;
	}

	public <T> void store(List<T> rows) throws IllegalArgumentException, IllegalAccessException, UnsupportedEncodingException {
		if (rows.isEmpty()) {
			return;
		}
		Collection<Field> fields = Collections2.filter(
				Utils.getAllFields(rows.get(0).getClass()),
				Utils.getAnnotationPredicate(Column.class));

		//     key          cf           column-name-value
		Map<ByteBuffer, Map<String, List<Mutation>>> rowDefs = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		for (T row : rows) {
			List<Mutation> mutations = Lists.newArrayList();
			long timeStamp = System.currentTimeMillis();
			for (Field f : fields) {
				boolean accessible = f.isAccessible();
				f.setAccessible(true);
				System.out.println(f.getName() + " : " + f.get(row));
				Mutation mutation = new Mutation();
				ByteBuffer valueBuffer = null;
				if (f.getType()== int.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getInt(row));
				} else if (f.getType() == long.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getLong(row));
				} else if (f.getType() == float.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getFloat(row));
				} else if (f.getType() == double.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getDouble(row));
				} else if (f.getType() == boolean.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getBoolean(row));
				} else if (f.getType() == char.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getChar(row));
				} else if (f.getType() == byte.class) {
					valueBuffer = Utils.getValueByteBufffer(f.getByte(row));
				} else {
					valueBuffer =Utils.getValueByteBufffer(f.get(row));
				}
				org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column()
						.setName(ByteBuffer.wrap(f.getName().getBytes("UTF-8")))
						.setValue(valueBuffer).setTimestamp(timeStamp);
				mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column)));
				f.setAccessible(accessible);
			}
		}
//		Client client = conn.getClient();
	}

	public <T> void store(T row) {
//		store(Arrays.asList(row));
	}

	public <T, K> T load(K key) {
		return null;
	}

	public <T, K> List<T> load(List<K> key, Class cls) {
		return null;
	}

}
