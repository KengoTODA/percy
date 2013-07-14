package com.github.percy;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ConnectionMain {

	private static final String HOST = "127.0.0.1";
	private static final int PORT = 9160;

	public static void main(String[] args) throws TTransportException, InvalidRequestException, SchemaDisagreementException, TException {
		System.out.println("hello");

		Connection conn = new Connection(HOST, PORT);
		conn.open();

//		conn.dropKeyspace("keyspace2");
//		conn.createKeyspace("keyspace2", Connection.SIMPLE_STRATGY, Connection.ONE_REPLICATION);

		conn.close();

		System.out.println("finished");
	}
}
