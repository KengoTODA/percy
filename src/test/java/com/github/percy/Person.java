package com.github.percy;

import com.github.percy.annotations.ColumnParent;
import com.github.percy.annotations.Key;

@ColumnParent
public class Person {

	@Key
	private String id;

	private String name;
	
}


//CREATE KEYSPACE keyspace1 WITH
//placement_strategy = 'SimpleStrategy'
//AND strategy_options = {replication_factor:2};
//
//
//CREATE COLUMN FAMILY cf1
//WITH comparator = UTF8Type
//AND key_validation_class=UTF8Type
//AND column_metadata = [
//{column_name: name, validation_class: UTF8Type}
//{column_name: age, validation_class: UTF8Type}
//];
