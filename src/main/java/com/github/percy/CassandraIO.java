
package com.github.percy;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;

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

	public <T> void store(List<T> rows) {
		Mutation mutation = new Mutation();
	}

	public <T> void store(T row) {
		store(Arrays.asList(row));
	}

	public <T, K> T load(K key) {
		return null;
	}

	public <T, K> List<T> load(List<K> key, Class cls) {
		return null;
	}

}
