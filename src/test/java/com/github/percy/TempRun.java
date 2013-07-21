
package com.github.percy;

import com.github.percy.sample.AB;
import com.github.percy.sample.Person;
import com.github.percy.sample.Teacher;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.Ignore;

public class TempRun {


	@Test
	public void tempRun() throws IllegalArgumentException, IllegalAccessException, UnsupportedEncodingException {
		AB a1 = new AB("key1", 1, "v1");
		AB a2 = new AB("key2", 2, "v2");

		new CassandraIO().store(Lists.newArrayList(a1, a2));

		System.out.println(new Integer(1).toString());
	}

	@Ignore
	public void tempRun2() {
		test(new Date());
		test(new Person(null, null, 1, null));
//		int[] l = {1, 2, 3};
		test2(Arrays.asList(1, 2, 3l));
		test2(Arrays.asList(1.0, 2.0, 3.0));
	}

	public static <T> void test(T t) {
		System.out.println(t.getClass());
	}

	public static <T> void test2(List<T> t) {
		System.out.println(t.size());
		for (T l : t) {
			System.out.println(l.getClass().getName());
		}
	}

}
