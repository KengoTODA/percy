
package com.github.percy;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class Utils {

	public static List<Field> getAllFields(final Class cls) {
		if (cls.getSuperclass() == Object.class)
			return Arrays.asList(cls.getDeclaredFields());
		List<Field> ret = Lists.newArrayList();
		ret.addAll(Arrays.asList(cls.getDeclaredFields()));
		ret.addAll(getAllFields(cls.getSuperclass()));
		return ret;
	}

	public static Predicate<Field> getAnnotationPredicate(final Class cls) {
		if (!cls.isAnnotation()) {
			throw new IllegalArgumentException(cls.getName() + " is not an annotation");
		}
		return new Predicate<Field>() {
			@Override
			public boolean apply(Field input) {
				return input.isAnnotationPresent(cls);
			}
		};
	}

}
