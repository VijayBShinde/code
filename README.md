# Problem Statement

Assume there are two text files, user and comments. Below are the fields in each of the files.


User File
•	userID
•	userName

Comments File
•	commentID
•	commentDescription
•	userID


Write a Java map reduce program to print out the users who have more than 2 comments.
Implement the same program using PIG scripts and HIVE SQL. The output text file should have following fields.


Output File
•	userID
•	username
•	commentDescription


# How to execute?
$ mvn package


$ hadoop jar target/mr-0.0.1-SNAPSHOT.jar org.dm.mr.MainDriverClass /data/input/users.csv /data/input/comments.csv /data/input/out2
