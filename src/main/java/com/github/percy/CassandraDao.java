
package com.github.percy;

import com.github.percy.annotations.Column;
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
import java.util.List;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.lang.SerializationUtils;

public class CassandraDao<T> {
	Connection conn = null;

	public CassandraDao() {
	}

	public CassandraDao setConnection(Connection conn) {
		this.conn = conn;
		return this;
	}

	public Connection getConnection() {
		return conn;
	}

	public void store(List<T> rows) throws IllegalArgumentException, IllegalAccessException, UnsupportedEncodingException {
		List<Mutation> mutations = Lists.newArrayList();
		for (T row : rows) {
			Collection<Field> fields = Collections2.filter(
				Utils.getAllFields(row.getClass()),
				Utils.getAnnotationPredicate(Column.class));
			for (Field f : fields) {
				boolean accessible = f.isAccessible();
				f.setAccessible(true);
				System.out.println(f.getName() + " : " + f.get(row));
				Mutation mutation = new Mutation();
				org.apache.cassandra.thrift.Column column = new org.apache.cassandra.thrift.Column()
						.setName(ByteBuffer.wrap(f.getName().getBytes("UTF-8")))
						.setValue(f.get(row).toString().getBytes()).setTimestamp(System.currentTimeMillis());
				mutations.add(new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column)));

				f.setAccessible(accessible);
			}
		}
//		Client client = conn.getClient();
	}

	public void store(T row) {
//		store(Arrays.asList(row));
	}

	public <K> T load(K key) {
		return null;
	}

	public <K> List<T> load(List<K> key, Class cls) {
		return null;
	}

}
