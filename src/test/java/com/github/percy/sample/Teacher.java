
package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import java.util.Date;

@ColumnFamily
public class Teacher extends Person {

	@Column
	private String subject;

	private String school;

	public Teacher(PersonId personId, String name, String sex, Date birth, String subject) {
		super(personId, name, sex, birth);
		this.subject = subject;
	}

	public Teacher setSubject(String subject) {
		this.subject = subject;
		return this;
	}

	public String getSubject() {
		return subject;
	}

	@Override
	public Person valudOf(PersonId personId, String name, String sex, Date birth) {
		return super.valudOf(personId, name, sex, birth);
	}

}
