#!/bin/bash

echo "Enter file sizes(ex: LDBC1 LDBC3 LDBC10 LDBC100 LDBC300):"
read -a sizes

echo "Enter storage formats (ex: CSV ORC Parquet Hive Neo4j):"
read -a file_formats

echo "Micro or Macro? (ex: micro macro):"
read queryType

echo "Enter the number of runs(ex: 5):"
read n

for s in "${!sizes[@]}"; 
do 
      for f in "${!file_formats[@]}"; 
      do 

                  if [ ${file_formats[$f]} == "Hive" ]
                  then
                     echo "ChangeDatabase" ${sizes[$s]} ${file_formats[$f]}

                  elif [ ${file_formats[$f]} == "Neo4j" ]
                  then
                     echo "ChangeDatabase" ${sizes[$s]} ${file_formats[$f]}
                     echo  "neo4j stop"
                     echo  "neo4j start"
                  fi


                  for (( counter=1; counter<=n; counter++ ));
                  do
                     echo "$counter" "$queryType" ${file_formats[$f]} ${sizes[$s]}
                     ##spark-submit --class org.opencypher.morpheus.ragabexamples.micro.${file_formats[$f]} --master local[*]  --deploy-mode client  morpheus-examples/build/libs/morpheus-examples-0.4.3-SNAPSHOT.jar ${sizes[$s]} $queryType

                     ##spark-submit --class "ee.ut.cs.bigdata.sp2bench.${file_formats[$f]}.${classes[$k]}" --master yarn --executor-memory 16G --executor-cores 4 --num-executors 19 --deploy-mode client  /home/hadoop/RDFBenchMarking/ProjectSourceCode/target/scala-2.12/rdfbenchmarkingproject_2.12-0.1.jar ${sizes[$s]} ${partition[$p]} 

                     
                  done                 

      done
done
