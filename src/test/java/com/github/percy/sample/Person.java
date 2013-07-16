package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import com.google.common.base.Function;
import java.util.Date;

@ColumnFamily
public class Person {

	private static final String SEX_MALE = "male";
	private static final String SEX_FEMALE = "female";

	@Key
	private PersonId personId;
	@Column
	private String name;
	@Column
	private String sex;
	@Column
	private Date birth;

	public static final Function<Person, PersonId> ID_FUNC = new Function<Person, PersonId>() {
		@Override
		public PersonId apply(Person input) {
			return input.getPersonId();
		}
	};

	Person(PersonId personId, String name, String sex, Date birth) {
		this.personId = personId;
		this.name = name;
		this.sex = sex;
		this.birth = birth;
	}

	public PersonId getPersonId() {
		return personId;
	}

	public Person setPersonId(PersonId personId) {
		this.personId = personId;
		return this;
	}

	public String getName() {
		return name;
	}

	public Person setName(String name) {
		this.name = name;
		return this;
	}

	public String getSex() {
		return sex;
	}

	public Person setSex(String sex) {
		this.sex = sex;
		return this;
	}

	public Date getBirth() {
		return birth;
	}

	public Person setBirth(Date birth) {
		this.birth = birth;
		return this;
	}

	public Person valudOf(PersonId personId, String name, String sex, Date birth) {
		return new Person(personId, name, sex, birth);
	}
}