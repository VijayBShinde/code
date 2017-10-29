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
public class CommentsRecord implements Writable{

	public Text commentDescription = new Text();
	
	public CommentsRecord(){}
	
	public CommentsRecord(String commentDescription){
		this.commentDescription.set(commentDescription);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.commentDescription.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.commentDescription.write(out);
		
	}

}
