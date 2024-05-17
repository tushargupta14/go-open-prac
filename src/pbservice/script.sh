#!/bin/bash

for t in TestConcurrentSame TestConcurrentSameUnreliable TestRepeatedCrash TestRepeatedCrashUnreliable TestPartition1 TestPartition2  # add all the test names here
do
	  echo $t
	    count=0
	      n=50
	        for i in $(seq 1 $n)
			  do
				      go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.txt
				          result=$(grep -E '^PASS$' log-${t}-${i}.txt| wc -l)
					      count=$((count + result))
					          if [ $result -eq 1 ]; then
							         rm ./log-${t}-${i}.txt
								     fi
								       done
								         echo "$count/$n"
								 done
