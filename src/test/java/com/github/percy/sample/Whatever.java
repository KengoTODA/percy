
package com.github.percy.sample;

import com.github.percy.annotations.Column;
import com.github.percy.annotations.ColumnFamily;
import com.github.percy.annotations.Key;
import java.util.Date;

@ColumnFamily
public class Whatever {

	@Key private String id;
	
	@Column	private int intColumn;
	@Column private long longColumn;
	@Column private boolean booleanColumn;
	@Column private double doubleColumn;
	@Column private byte[] bytesColumn;
	@Column	private Date dateColumn;
	@Column	private Integer integerColumn;
	@Column private Long longClassColumn;

}
