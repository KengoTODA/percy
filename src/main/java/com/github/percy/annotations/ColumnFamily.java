
package com.github.percy.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.cassandra.thrift.ConsistencyLevel;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ColumnFamily {

	ConsistencyLevel consistencyLevel() default ConsistencyLevel.QUORUM;

}
