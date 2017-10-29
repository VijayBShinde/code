package org.dm.mr;

import java.io.IOException;
import java.util.Date;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Vijay.Shinde
 *
 */
public class MainDriverClass extends Configured implements Tool{

	public static Logger logger = LoggerFactory.getLogger(MainDriverClass.class);
	public static Configuration conf;
	static{
		conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/hadoop-2.7.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"));
	}
	/**
	 *  All the records having same value of userID in UserIDKey class to be sent to the same reduce call, 
	 *  so the UserRecord containing User Name , can be combined (or Join) with the instances of CommentsRecord
	 *  having same userID in key.
	 */	
	public static class MyJoinGroupingComparator extends WritableComparator{

		public MyJoinGroupingComparator(){
			super(UserIDKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UserIDKey first = (UserIDKey) a;
			UserIDKey second = (UserIDKey) b;
			return first.userID.compareTo(second.userID);
		}
	}

	/**
	 *  This sorts the records making sure that the first record is the User record, and then all the data comes.
	 */	
	public static class MyJoinSortingComparator extends WritableComparator {
		public MyJoinSortingComparator()
		{
			super (UserIDKey.class, true);
		}

		@Override
		public int compare (WritableComparable a, WritableComparable b){
			UserIDKey first = (UserIDKey) a;
			UserIDKey second = (UserIDKey) b;

			return first.compareTo(second);
		}
	}
	
	/**
	 * 
	 * There are two mappers required. 
	 * One will process the users.csv file and emit UserIDKey, and UsersRecord instances, and 
	 * the other will process the comments.csv file, and emit UserIDKey, and CommentsRecord instances. 
	 * 
	 */
	public static class UserMapper extends Mapper<LongWritable, Text, UserIDKey, MyGenericWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("In User Mapper");
			String[] recordFields = value.toString().split(",");
			logger.info("User ID: " + recordFields[0] + "User Name : " + recordFields[1]);
			int userID = Integer.parseInt(recordFields[0]);
			String userName = recordFields[1];

			UserIDKey recordKey = new UserIDKey(userID, UserIDKey.USER_RECORD);
			UsersRecord record = new UsersRecord(userName);
			MyGenericWritable genericRecord = new MyGenericWritable(record);
			context.write(recordKey, genericRecord);
		}
	}

	public static class CommentsMapper extends Mapper<LongWritable, Text, UserIDKey, MyGenericWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
			logger.info("In CommetsMapper");
			String[] recordFields = value.toString().split(",");
			int userID = Integer.parseInt(recordFields[2]);
			logger.info("Comment ID: " + recordFields[0] + "Comment Description : " + recordFields[1] + "User ID : " + recordFields[2]);
			String commentDescription = recordFields[1];

			UserIDKey recordKey = new UserIDKey(userID, UserIDKey.COMMENT_RECORD);
			CommentsRecord record = new CommentsRecord(commentDescription);
			MyGenericWritable genericRecord = new MyGenericWritable(record);
			
			logger.info("Record Key: " + recordKey.userID.get() + "Comment Description : " + record.commentDescription);
			context.write(recordKey, genericRecord);
		}
	}

	/**
	 * 
	 * In reducer, looks at the recordType field in the key to determine which value received from respective mapper.
	 * It then writes the UserID, UserName and comments which more than 2
	 *
	 */
	public static class JoinReducer extends Reducer<UserIDKey, MyGenericWritable, NullWritable, Text>{

		public void reduce(UserIDKey key, Iterable<MyGenericWritable> values, Context context) throws IOException, InterruptedException{
			logger.info("in reduce ---------------->>>>>>>");
			int noOfComments = 0;
 			StringBuilder output = new StringBuilder();
			for (MyGenericWritable v : values) {
				Writable record = v.get();
				if (key.recordType.equals(UserIDKey.USER_RECORD)){
										
					UsersRecord pRecord = (UsersRecord)record;
					logger.info("ifKey : " + key.userID + " Value : " + pRecord.userName);
					output.append(Integer.parseInt(key.userID.toString())).append(", ");
					output.append(pRecord.userName.toString()).append(", ");
				} else {
					noOfComments++;
					CommentsRecord record2 = (CommentsRecord)record;
					logger.info("Key : " + key.userID + " Value : " + record2.commentDescription);
					output.append(record2.commentDescription.toString() + " | ");
				}
				
				logger.info("cnt : " + noOfComments);
			}
			if(noOfComments > 2)
				context.write(NullWritable.get(), new Text(output.toString()));
			else
				logger.info("Comments are less that 2 : ");
		}
	}

	public int run(String[] allArgs) throws Exception {

		logger.info("*********************     Job Started At: " + new Date() + "      ********************");
		String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();
		Path inputPath1 = new Path(args[0]);
		Path inputPath2 = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(MainDriverClass.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(UserIDKey.class);
		job.setMapOutputValueClass(MyGenericWritable.class);

		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, CommentsMapper.class);

		job.setReducerClass(JoinReducer.class);

		job.setSortComparatorClass(MyJoinSortingComparator.class);
		job.setGroupingComparatorClass(MyJoinGroupingComparator.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		int returnValue = job.waitForCompletion(true) ? 0:1;
		logger.info("Job status  :::   " + job.isSuccessful());
		logger.info("********************     Job Ended At: " + new Date() + "     ************************");
		return returnValue;
	}

	public static void main(String[] args) throws Exception{                               
		int res = ToolRunner.run(conf,new MainDriverClass(), args);
		System.exit(res);
	}
}
