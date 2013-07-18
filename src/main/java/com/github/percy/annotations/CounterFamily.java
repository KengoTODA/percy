package com.github.percy.annotations;

import org.apache.cassandra.thrift.ConsistencyLevel;

public @interface CounterFamily {

	ConsistencyLevel consistencyLevel() default ConsistencyLevel.QUORUM;

}
