
package com.github.percy;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraIO {

	Logger logger = LoggerFactory.getLogger(CassandraIO.class);
	
	Connection conn = null;
	DataBarn dataBarn = null;

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
		dataBarn.store(rows);
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
		dataBarn.clear();
	}

	public void flush() throws InvalidRequestException, UnavailableException, TimedOutException, TException {
	}

}
