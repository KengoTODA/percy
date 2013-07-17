package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import com.google.common.base.Function;
import java.util.Date;

@ColumnFamily
public class Person {

	public static final Function<Person, PersonId> ID_FUNC = new Function<Person, PersonId>() {
		@Override
		public PersonId apply(Person input) {
			return input.getPersonId();
		}
	};

	public static final int SEX_MALE = 1;
	public static final int SEX_FEMALE = 2;

	@Key
	private PersonId personId;
	@Column
	private String name;
	@Column
	private int sex;
	@Column
	private Date birth;


	public Person(PersonId personId, String name, int sex, Date birth) {
		this.personId = personId;
		this.name = name;
		this.sex = sex;
		this.birth = birth;
	}

	public PersonId getPersonId() {
		return personId;
	}

	public void setPersonId(PersonId personId) {
		this.personId = personId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getSex() {
		return sex;
	}

	public void setSex(int sex) {
		this.sex = sex;
	}

	public Date getBirth() {
		return birth;
	}

	public void setBirth(Date birth) {
		this.birth = birth;
	}

}