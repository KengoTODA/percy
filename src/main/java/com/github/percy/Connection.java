package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Connection {

	public static final String SIMPLE_STRATGY = "org.apache.cassandra.locator.SimpleStrategy";
	public static final int ONE_REPLICATION = 1;

	private final String host;
	private final int port;
	private TTransport tTransport;
	private TProtocol tProtocol;
	private Cassandra.Client client;
	private String keyspace = null;

	public Connection(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void open() throws TTransportException {
		tTransport = new TFramedTransport(new TSocket(host, port));
		tProtocol = new TBinaryProtocol(tTransport);
		client = new Cassandra.Client(tProtocol);
		tTransport.open();
	}

	public void close() {
		tTransport.close();
	}

	public String createKeyspace(String keyspace, String strategyClass, int replicationFactor) throws InvalidRequestException, SchemaDisagreementException, TException {
		KsDef ks = new KsDef();
		ks.setName(keyspace);
		ks.setCf_defs(new ArrayList<CfDef>());
		ks.setStrategy_class(strategyClass);
		Map<String, String> optionMap = new TreeMap<String, String>();
		optionMap.put("replication_factor", String.valueOf(replicationFactor));
		ks.setStrategy_options(optionMap);
		return client.system_add_keyspace(ks);
	}

	public void dropKeyspace(final String keyspace) throws InvalidRequestException, SchemaDisagreementException, TException {
		client.system_drop_keyspace(keyspace);
	}

	public Connection setKeyspace(String keyspace) throws InvalidRequestException, TException {
		this.keyspace = keyspace;
		client.set_keyspace(keyspace);
		return this;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public Cassandra.Client getClient() {
		return client;
	}

	public String createColumnFamily(Class cls) throws InvalidRequestException, SchemaDisagreementException, TException {
		Pair<Field, List<Field>> kvField = Utils.getKeyValueFields(cls);
		Field keyField = kvField.left;
		List<Field> valueFields = kvField.right;
		CfDef cfDef = new CfDef();
		if (keyspace == null) {
			throw new IllegalStateException("keyspace unset");
		}
		cfDef.setKeyspace(keyspace);
		cfDef.setName(cls.getSimpleName());
		cfDef.setComparator_type(Utils.getValidationClassString(keyField.getType()));
		cfDef.setKey_validation_class("UTF8Type");
		cfDef.setDefault_validation_class("UTF8Type");
		for (Field f : valueFields) {
			try {
				cfDef.addToColumn_metadata(new ColumnDef(
						ByteBuffer.wrap(f.getName().getBytes("UTF-8")),
						Utils.getValidationClassString(f.getType())));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Bug in source code. " + e.getMessage());
			}
		}
		return client.system_add_column_family(cfDef);
	}

	public void dropColumnFamily(Class cls) throws InvalidRequestException, SchemaDisagreementException, TException {
		if (keyspace == null) {
			throw new IllegalStateException("keyspace unset");
		}
		client.system_drop_column_family(cls.getSimpleName());
	}

	public void createCounterColumn(Class cls) {
	}

	public void dropCounterColumn(Class cls) {
	}

}