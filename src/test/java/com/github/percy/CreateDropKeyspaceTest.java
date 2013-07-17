package com.github.percy;

import com.github.percy.sample.Person;
import com.github.percy.sample.Teacher;
import com.github.percy.sample.Whatever;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
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
		conn.setKeyspace(ConnectionArgs.KEYSPACE);
		conn.createColumnFamily(Person.class);
//		conn.createColumnFamily(Teacher.class);
//		conn.createColumnFamily(Whatever.class);

//		Cassandra.Client client = conn.getClient();
//		mutation.setColumn_or_supercolumn(new Column().set)
//		List<Mutation> mutations = Lists.newArrayList();
//		Column column = new Column().setName(ByteBuffer.wrap("name".getBytes("UTF-8")))
//									.setValue(ByteBuffer.wrap("name value".getBytes("UTF-8")))
//									.setTimestamp(System.currentTimeMillis());
//		Mutation mutation = new Mutation();
//		mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
//		mutations.add(mutation);
//		Map<String,List<Mutation>> columnFamilyValues = new HashMap<String,List<Mutation>>();
//		columnFamilyValues.put("Person", mutations);
//		Map<ByteBuffer, Map<String, List<Mutation>>> rowDefinition = new HashMap<ByteBuffer, Map<String,List<Mutation>>>();
//		rowDefinition.put(ByteBuffer.wrap(("key1").getBytes("UTF8")), columnFamilyValues);
//		client.batch_mutate(rowDefinition, ConsistencyLevel.ONE);

//		conn.dropColumnFamily(Person.class);
//		conn.dropColumnFamily(Teacher.class);
//		conn.dropKeyspace(ConnectionArgs.KEYSPACE);
	}

	@AfterClass
	public static void cleanup() {
		conn.close();
	}
}
