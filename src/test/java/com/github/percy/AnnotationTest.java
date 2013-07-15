
package com.github.percy;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import com.github.percy.sample.Person;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import java.lang.reflect.Field;
import java.util.Collections;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class AnnotationTest {
	@Test
	public void testTypeAnnotation() {
		assertThat(Person.class.isAnnotationPresent(ColumnFamily.class), is(equalTo(true)));
	}

	@Test
	public void testFieldAnnotation() {
		for (Field f : Person.class.getDeclaredFields()) {
			System.out.println(f.getName());
			System.out.println(f.isAnnotationPresent(Key.class));
			System.out.println(f.isAnnotationPresent(Column.class));
		}
	}
}
