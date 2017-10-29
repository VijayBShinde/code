package org.dm.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Vijay.Shinde
 *
 */
public class UsersRecord implements Writable{

	public Text userName = new Text();
	
	public UsersRecord(){}
	
	public UsersRecord(String userName){
		this.userName.set(userName);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.userName.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.userName.write(out);
	}
}
