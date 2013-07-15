
package com.github.percy;

import com.github.percy.sample.Person;
import java.io.UnsupportedEncodingException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateDropColumnFamily {
	private static Connection conn = null;

	@BeforeClass
	public static void prepare() throws TTransportException {
		conn = new Connection(ConnectionArgs.HOST, ConnectionArgs.PORT);
		conn.open();
	}

	@Test
	public void testCreateDropKeyspace() throws UnsupportedEncodingException, InvalidRequestException, SchemaDisagreementException, TException {

	}

	@AfterClass
	public static void cleanup() {
		conn.close();
	}
}
