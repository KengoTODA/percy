
package com.github.percy;

import com.github.percy.sample.Person;
import com.github.percy.sample.Teacher;
import java.lang.reflect.Field;
import org.junit.Test;

public class TempRun {

	@Test
	public void tempRun() {
		for (Field f : Teacher.class.getDeclaredFields()) {
			System.out.println(f.getName());
		}
		System.out.println(Person.class.getSuperclass().getName());
		System.out.println(Teacher.class.getSuperclass().getName());
		for (Field f : Person.class.getDeclaredFields()) {
			System.out.println(f.getType().getName());
		}
	}
}
