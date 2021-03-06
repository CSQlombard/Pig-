/* In this problem we will consider the subgraph consisting of triples whose subject matches rdfabout.com:
for that, filter on subject matches '.*rdfabout\.com.*'.
Find all chains of lengths 2 in this subgraph.
More precisely, return all sextuples (subject, predicate, object, subject2, predicate2, object2) where object=subject2.

Note: Newer versions of Pig will automatically drop the duplicate column in the join output.
In that case, you do NOT need to return the sixth column.

Suggestions on how to proceed:

First filter the data so you only have tuples whose subject matches 'rdfabout.com'.

Make another copy of the filtered collection
(it's best to re-label the subject, predicate, and objects, for example to subject2, predicate2, object2).

Now join the two copies:
the first copy of the 'rdfabout.com' collection should match on object.
the second copy of the 'rdfabout.com' collection should match on subject2.

Remove duplicate tuples from the result of the join
While debugging on the test file, make the following two changes:

Use the following filter
subject matches '.*business.*'

Change the join predicate to be subject=subject2
Otherwise, you will not get any results.

Note: this script took about 18 minutes with 10 small nodes.

What you need to turn in: How many records are generated by the join for the cse-344-test-file dataset? For the btc-2010-chunk-000 dataset? DON'T FORGET TO SHUTDOWN YOUR INSTANCES!
*/

-- For Test
-- register hdfs://localhost:54310/user/hduser/Coursera_Ex/myudfs.jar
-- raw = LOAD 'hdfs://localhost:54310/user/hduser/Coursera_Ex/cse344-test-file' USING TextLoader as (line:chararray);

-- For Real
register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);

ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- Filter subject by '.*business.*' (for test) and '.*rdfabout\\.com.*' for actual file.

-- For Test
-- filtered_data = FILTER ntriples BY subject MATCHES '.*business.*';

-- For Real
filtered_data = FILTER ntriples BY subject MATCHES '.*rdfabout\\.com.*';

-- Copy Data
filtered_data2 = FOREACH filtered_data GENERATE subject AS subject2, predicate AS predicate2, object AS object2;

-- Perform the Join, for subject=subject2 (for test) and object = subject2 (actual data)

-- For Test
-- joined_data = JOIN filtered_data BY subject, filtered_data2 BY subject2;

-- For Real
joined_data = JOIN filtered_data BY object, filtered_data2 BY subject2;

-- Remove Duplicates
Final_Join = DISTINCT joined_data;

-- Count shit
all_objects = GROUP Final_Join All; -- 2998 for joined_data and same for the other one.
all_objects_count = FOREACH all_objects GENERATE COUNT(Final_Join);

-- Save Important Data
--store Final_Join into '/user/hadoop/problem_3a' using PigStorage();
store all_objects_count into '/user/hadoop/problem_3b' using PigStorage();
