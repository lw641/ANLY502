-- wordcount_shakespeare2.pig
--
--
-- Find the top-20 words in Shakespeare, ignoring case
--
-- Clear the output directory location
--
rmf sorted_words2

--
-- Run the script

shakespeare = LOAD 's3://gu-anly502/ps04/Shakespeare.txt' as (line:chararray);

words = foreach shakespeare generate flatten(TOKENIZE(line)) as lower(word); 
grouped = GROUP words by word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
sorted_words = ORDER wordcount BY $1 DESC; 
sorted_words20 = limit sorted_words 20; 
dump sorted_words20;

STORE sorted_words20 INTO 'sorted_words2' USING PigStorage();
 
-- Get the results
--
fs -getmerge sorted_words2 wordcount_shakespeare.txt
