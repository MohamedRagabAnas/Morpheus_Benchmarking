/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.morpheus.ragabexamples.micro

import java.io.{File, FileOutputStream}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.ragabexamples.micro.ORC.graph
import org.opencypher.okapi.api.graph.Namespace

object CSV2 {

  def main(args: Array[String]): Unit = {

  // 1) Create Morpheus session
  val conf = new SparkConf(true)
  //conf.set("spark.driver.memory","100g")
  //conf.set("spark.executor.memory","100g")
  //conf.set("spark.sql.codegen.wholeStage", "true")
  //conf.set("spark.sql.shuffle.partitions", "12")
  //conf.set("spark.default.parallelism", "8")


  val spark = SparkSession
    .builder()
    .config(conf)
   // .master("spark://172.17.77.48:7077")
    .appName(s"morpheus-local-${UUID.randomUUID()}")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("error")
  println("Spark Session created ")

  implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)
  println("Morpheus session created ya Allah")

  val ds = args(0)  // data size (ex: LDBC1)
  println(s"Data size: $ds")

  val csvPgds = GraphSources.fs(s"hdfs://172.17.77.48:9000/user/hadoop/morpheus/$ds").csv
  morpheus.registerSource(Namespace("myNewGraphNameSapce"), csvPgds)
  val graph= morpheus.catalog.graph("myNewGraphNameSapce.CSV")


  val fos = new FileOutputStream(new File(s"/home/hadoop/MorpheusBench/Expriments/Logs/micro/CSV/$ds.txt"),true)
  val queries = List(MicroBenchQueries.Q1)
                    /* MicroBenchQueries.Q2,
                     MicroBenchQueries.Q3,
                     MicroBenchQueries.Q4,
                     MicroBenchQueries.Q5,
                     MicroBenchQueries.Q6,
                     MicroBenchQueries.Q7,
                     MicroBenchQueries.Q8,
                     MicroBenchQueries.Q9,
                     MicroBenchQueries.Q10,
                     MicroBenchQueries.Q11,
                     MicroBenchQueries.Q12,
                     MicroBenchQueries.Q13,
                     MicroBenchQueries.Q14,
                     MicroBenchQueries.Q15,
                     MicroBenchQueries.Q16) */


  var count = 1
  for (query <- queries)
  {
    val start_time=System.nanoTime()
    println(s"Result of Query $count: ")
    graph.cypher(query).records.show
    val end_time=System.nanoTime()
    val result = (end_time-start_time).toDouble/1000000000

    if(count != queries.size) {
      Console.withOut(fos){print(result + ",")}
    } else {
      Console.withOut(fos){println(result)}
    }
    count+=1
  }

}

}
