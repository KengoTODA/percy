
package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import java.util.Date;

@ColumnFamily
public class Teacher extends Person {

	@Column
	private String subject;

	@Column
	private String school;

	public Teacher(String subject, String school, PersonId personId, String name, int sex, Date birth) {
		super(personId, name, sex, birth);
		this.subject = subject;
		this.school = school;
	}

}
