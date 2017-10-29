-- Problem Stmt : find the users who have more than 2 comments.
users = LOAD '<path>/users.csv' USING PigStorage(',')   as (id:int, userName:chararray);
comments = LOAD '/data/input/comments.csv' USING PigStorage(',')   as (commentId:int, commentDescription:chararray, userId:int);


joinData = JOIN  users by id, comments by userId;

jnd_grp = group joinData by (users::id);

filter_data = FILTER jnd_grp BY COUNT(joinData) > 2;

dump filter_data;

STORE filter_data INTO ‘/data/input/result’ using PigStorage(',');
