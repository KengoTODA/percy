package com.github.percy;

import java.io.UnsupportedEncodingException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class CreateDropKeyspaceTest {

	private static Connection conn = null;

	@BeforeClass
	public static void prepare() throws TTransportException {
		conn = new Connection(ConnectionArgs.HOST, ConnectionArgs.PORT);
		conn.open();
	}

	@Test
	public void testCreateDropKeyspace() throws TTransportException, InvalidRequestException, SchemaDisagreementException, TException, UnsupportedEncodingException, UnavailableException, TimedOutException {
//		conn.createKeyspace(ConnectionArgs.KEYSPACE, Connection.SIMPLE_STRATGY, Connection.ONE_REPLICATION);
//		conn.setKeyspace(ConnectionArgs.KEYSPACE);
//		conn.dropKeyspace(ConnectionArgs.KEYSPACE);
	}

	@AfterClass
	public static void cleanup() {
		conn.close();
	}
}
