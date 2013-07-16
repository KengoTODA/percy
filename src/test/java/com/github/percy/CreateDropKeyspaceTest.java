package com.github.percy;

import com.github.percy.sample.Person;
import com.github.percy.sample.Teacher;
import java.io.UnsupportedEncodingException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
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
	public void testCreateDropKeyspace() throws TTransportException, InvalidRequestException, SchemaDisagreementException, TException, UnsupportedEncodingException {
//		conn.createKeyspace(ConnectionArgs.KEYSPACE, Connection.SIMPLE_STRATGY, Connection.ONE_REPLICATION);
		conn.setKeyspace(ConnectionArgs.KEYSPACE);
//		conn.createColumnFamily(Person.class);
//		conn.createColumnFamily(Teacher.class);
		conn.dropColumnFamily(Person.class);
		conn.dropColumnFamily(Teacher.class);
		conn.dropKeyspace(ConnectionArgs.KEYSPACE);
	}

	@AfterClass
	public static void cleanup() {
		conn.close();
	}
}
