#!/bin/bash
cat  ~/bin/filename.txt | while read line
do
	first_dir=$(hdfs dfs -ls $line | grep '^d' | awk '{print $8}' | head -n 1)
	first_size=$(hdfs dfs -du -s $first_dir | awk '{print $1}')
	last_size=$(hdfs dfs -du -s $line/dt=2022-12-31 | awk '{print $1}')
	echo "$first_dir:$first_size;dt=2022-12-31:$last_size" >> ~/bin/filesize.txt
done

