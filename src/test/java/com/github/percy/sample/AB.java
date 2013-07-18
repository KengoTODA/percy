
package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;

@ColumnFamily
public class AB {
	@Key	private String id;
	@Column private int a;
	@Column private String b;

	public AB(String id, int a, String b) {
		this.id = id;
		this.a = a;
		this.b = b;
	}

	public AB() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getA() {
		return a;
	}

	public void setA(int a) {
		this.a = a;
	}

	public String getB() {
		return b;
	}

	public void setB(String b) {
		this.b = b;
	}

}
