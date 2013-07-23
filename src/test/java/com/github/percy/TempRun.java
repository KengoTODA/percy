
package com.github.percy;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.Test;
import org.junit.Ignore;

public class TempRun {


	@Test
	public void tempRun() {

	}

	@Ignore
	public void tempRun2() {
		test(new Date());
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
