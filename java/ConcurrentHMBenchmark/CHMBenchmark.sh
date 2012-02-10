#!/bin/bash

echo "gc_algorithm,chm_impl,threads,get_put_remove,ops" > CHMBenchmark.csv
for gc_algorithm in "-XX:+UseConcMarkSweepGC -XX:-UseParNewGC" "-XX:+UseG1GC"
do
	if [ "$gc_algorithm" == "-XX:+UseConcMarkSweepGC -XX:-UseParNewGC" ]; then
		gc_algo_short="CMS"
	else
		gc_algo_short="G1"
	fi
	for chm_impl in 3 6 8 11 12
	do
		#  3 => ConcurrentHashMap_16
		#  6 => NonBlockingHashMap
		#  8 => ConcurrentLinkedHashMap_16
		# 11 => ConcurrentSkipListMap
		# 12 => SnapTreeMap
		if [ "$chm_impl" == "3" ]; then
			chm_impl_str="CHM_16"
		elif [ "$chm_impl" == "6" ]; then
			chm_impl_str="NBHM"
		elif [ "$chm_impl" == "8" ]; then
			chm_impl_str="CLHM_16"
		elif [ "$chm_impl" == "11" ]; then
			chm_impl_str="CSLM"
		else
			chm_impl_str="SnapTree"
		fi
		for threads in 1 2 4 8 16 32 64
		do
			for read_ratio in 0 70 90
			do
				if [ "$read_ratio" == "0" ]; then
					get_put_remove="00-50-50"
				elif [ "$read_ratio" == "70" ]; then
					get_put_remove="70-15-15"
				else
					get_put_remove="90-05-05"
				fi
				java -cp .:lib/* -Xms1G -Xmx4G ${gc_algorithm} PerfHashBenchmark ${read_ratio} ${threads} ${threads} 1 100000 ${chm_impl} | sed '1,5d' | sed '2d' | awk 'BEGIN {OFS=","} {print "'$gc_algo_short'", "'$chm_impl_str'", "'$threads'", "'$get_put_remove'", $12}' | tee -a CHMBenchmark.csv
			done
		done
	done
done