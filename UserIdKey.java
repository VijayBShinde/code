package org.dm.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Vijay.Shinde
 *
 */
public class UserIDKey implements WritableComparable<UserIDKey>{
	
	public static final IntWritable USER_RECORD = new IntWritable(0);
	public static final IntWritable COMMENT_RECORD = new IntWritable(1);
	
	public IntWritable userID = new IntWritable();
	public IntWritable recordType = new IntWritable();
	
	public UserIDKey(){}
	public UserIDKey(int userID, IntWritable recordType){
		this.userID.set(userID);
		this.recordType = recordType;

	}
	
	public void readFields(DataInput in) throws IOException {
		this.userID.readFields(in);
		this.recordType.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.userID.write(out);
		this.recordType.write(out);
	}

	public int compareTo(UserIDKey other) {
		
		if(this.userID.equals(other.userID)){
			return this.recordType.compareTo(other.recordType);
		}
		else{
			return this.userID.compareTo(other.userID);
		}
		
	}
	
	public boolean equals(UserIDKey other) {

		return this.userID.equals(other.userID) && this.recordType.equals(other.recordType);
	}
	
	@Override
	public int hashCode() {

		return this.userID.hashCode();
	}

	
}
