
package com.github.percy;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
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

	public String createKeyspace(final String keyspace, final String strategyClass, int replicationFactor) throws InvalidRequestException, SchemaDisagreementException, TException {
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

}