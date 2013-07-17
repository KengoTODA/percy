
package com.github.percy;

import com.github.percy.sample.Person;
import com.github.percy.sample.Teacher;
import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.Ignore;

public class TempRun {


	@Ignore
	public void tempRun() {
		for (Field f : Teacher.class.getDeclaredFields()) {
			System.out.println(f.getName());
		}
		System.out.println(Person.class.getSuperclass().getName());
		System.out.println(Teacher.class.getSuperclass().getName());
		for (Field f : Person.class.getDeclaredFields()) {
			System.out.println(f.getType());
			System.out.println(f.getType() == int.class);
		}
		for (Map.Entry<Class, Class> v : ValidationClassChooser.getMap().entrySet()) {
			System.out.println(v.getKey().getSimpleName() + " " + v.getValue().getSimpleName());
		}
	}

	@Test
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
