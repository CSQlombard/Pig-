/*
Using the 'cse344-test-file' file , write a Pig script that groups tuples by the subject column, and creates/stores
histogram data showing the distribution of counts per subject, then generate a scatter-plot of this histogram. The histogram consists of:
    The x-axis is the counts associated with the subjects, and
    The y-axis is the total number of subjects associated with each particular count.
So, for each point (x,y) that we generate, we mean to say that
y subjects each had x tuples associated with them after we group by subject.

A few comments to help you get started:
We expect that your script will (1) group the input data by subject and count the tuples associated with each subject then
(2) group the results by these intermediate counts (x-axis values) and compute the final counts (y-axis values).
To get more familiar with the Pig Latin commands, we suggest that you also take a look at the Pig Latin Reference.

What you need to turn in: How many (x, y) points are generated in the histogram?
*/
--------------------------------------------------------------
-- Very important! to be able to use "User Defined Functions"
--------------------------------------------------------------

-- Cuando PARALLEL es mas que 1 no me funciona en la compu!!
-- porque sera? Averigua.

register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
-- register /home/sebastian/Documents/Pig/Coursera_Ex/myudfs.jar
-- register hdfs://localhost:54310/user/hduser/Coursera_Ex/myudfs.jar

-------------------------------
-- load the test file into Pig
-------------------------------

-- This part if to load from my local driver to check and test the script.
-- raw = LOAD '/home/sebastian/Documents/Pig/Coursera_Ex/cse344-test-file' USING TextLoader as (line:chararray);

-- This part is if I want to run pig in mapreduce form
-- raw = LOAD 'hdfs://localhost:54310/user/hduser/Coursera_Ex/cse344-test-file' USING TextLoader as (line:chararray);

-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);
-- later you will load to other files, example:
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject column
subjects = GROUP ntriples BY (subject) PARALLEL 5000;

-- flatten the objects out (because group by produces a tuple of each object
-- in the first column, and we want each object ot be a string, not a tuple),
-- and count the number of tuples associated with each object

count_by_subject = FOREACH subjects GENERATE $0, COUNT($1) PARALLEL 5000;

-- Group by counter
counter_group = GROUP count_by_subject BY $1 PARALLEL 5000;

final_count = FOREACH counter_group GENERATE $0, COUNT($1) PARALLEL 5000;

-- Apply a reduce phase to generate a unique file with all data
-- I have to apply somehow a reduce task here, if not I get 50 files.
a = GROUP final_count BY RANDOM();
final_count_reduce = FOREACH a GENERATE FLATTEN(final_count);

-- Creo que tambien sirve
b = GROUP final_count ALL;
final_count_reduce_b = FOREACH b GENERATE FLATTEN(final_count);

-- Group everything together and then count.
-- all_objects = GROUP final_count All PARALLEL 50;
-- all_objects_count = FOREACH all_objects GENERATE COUNT(final_count);

-- Save Important Data
--store final_count into '/user/hadoop/problem_2a' using PigStorage();
--store all_objects_count into '/home/hadoop/problem_2b' using PigStorage();
store final_count_reduce into 's3://aws-logs-277243302517-us-west-2/elasticmapreduce/problem_4_HIST_a' using PigStorage('\t'); -- s3 bucket direction.
store final_count_reduce_b into 's3://aws-logs-277243302517-us-west-2/elasticmapreduce/problem_4_HIST_b' using PigStorage('\t'); -- s3 bucket direction.
