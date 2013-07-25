
package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraIOTest {

	static Connection conn = null;

	@BeforeClass
	public static void setup() throws TTransportException, InvalidRequestException, SchemaDisagreementException, TException {
//		conn = new Connection(ConnectionArgs.HOST, ConnectionArgs.PORT).open().setKeyspace(ConnectionArgs.KEYSPACE);
	}

	@Test
	public void test1() throws InvalidRequestException, InvalidRequestException, SchemaDisagreementException, SchemaDisagreementException, TException, UnavailableException, TimedOutException {
//		conn.createColumnFamily(IntCF.class);
		DataBarn dataBarn = new DataBarn();
		dataBarn.store(new IntCF("k1", 1, 10), new IntCF("k2", 2, 20));
		System.out.println(dataBarn);
//		conn.dropColumnFamily(IntCF.class);
	}

	@AfterClass
	public static void cleanup() {
//		conn.close();
	}

	@ColumnFamily
	private class IntCF {
		@Key private String key;
		@Column private int i1;
		@Column private Integer i2;

		public IntCF(String key, int i1, Integer i2) {
			this.key = key;
			this.i1 = i1;
			this.i2 = i2;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public int getI1() {
			return i1;
		}

		public void setI1(int i1) {
			this.i1 = i1;
		}

		public Integer getI2() {
			return i2;
		}

		public void setI2(Integer i2) {
			this.i2 = i2;
		}
	}
}
