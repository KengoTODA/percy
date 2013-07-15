
package com.github.percy.sample;

import java.util.UUID;

public class PersonId {
	public final String id;

	PersonId(String id) {
		this.id = id;
	}

	public static PersonId valueOf() {
		return new PersonId(UUID.randomUUID().toString());
	}

}
