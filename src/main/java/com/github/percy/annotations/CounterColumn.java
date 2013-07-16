package com.github.percy.annotations;

import org.apache.cassandra.thrift.ConsistencyLevel;

public @interface CounterColumn {

	ConsistencyLevel consistencyLevel() default ConsistencyLevel.QUORUM;

}
