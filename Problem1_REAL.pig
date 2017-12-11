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
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by object column
objects = group ntriples by (object) PARALLEL 50;

-- flatten the objects out (because group by produces a tuple of each object
-- in the first column, and we want each object ot be a string, not a tuple),
-- and count the number of tuples associated with each object
count_by_object = foreach objects generate flatten($0), COUNT($1) as counter PARALLEL 50;

-------------Pro-----------------------------------------
--PROBLEM 1: How many records are in count_by_object
------------------------------------------------------
-- Group everything together and then count.
all_objects = GROUP count_by_object All PARALLEL 50;
all_objects_count = FOREACH all_objects GENERATE COUNT(count_by_object);
-- This output should be the same as "Total records written"

--order the resulting tuples by their count in descending order
count_by_object_ordered = order count_by_object by (counter) PARALLEL 50;

dump count_by_object_ordered;

-- Save Important Data
--store count_by_object_ordered into '/user/hadoop/problem_1a' using PigStorage();
store all_objects_count into '/home/hadoop/problem_1b' using PigStorage();

-- store count_by_object_ordered into '/home/sebastian/Documents/Pig/Coursera_Ex/Codes_For_Cluster/Results' using PigStorage();
-- Alternatively, you can store the results in S3, see instructions:
--store count_by_object_ordered into 's3n://superman/example-results';
