
-- User table
CREATE TABLE IF NOT EXISTS userdata(
	userrId INT,
	userName STRING
	)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',';
 
-- Load the user csv data into userdata table
LOAD DATA LOCAL INPATH '<path>' OVERWRITE INTO TABLE userdata; 
 
-- Comment table
CREATE TABLE IF NOT EXISTS commentdata(
	commentId INT,
	commentDescription STRING,
	userrId INT
	)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',';

-- Load the comment csv data into commentdata table
LOAD DATA LOCAL INPATH '<path>' OVERWRITE INTO TABLE commentdata; 
  
-- Query returns the users who have more than 2 comments.  
SELECT 
	u.userId,u.userName,c.commentDescription 
	FROM userdata u JOIN commentdata c 
	ON u.userId = c.userId
	WHERE u.userId 
	IN (SELECT userId from (SELECT userId,count(commentDescritption) as cnt FROM commentdata GROUP BY userId) r where cnt > 2);
