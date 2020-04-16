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
package org.opencypher.morpheus.ragabexamples.`macro`

import java.io.{File, FileOutputStream}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.okapi.api.graph.Namespace

object ORC extends App {


  // 1) Create Morpheus session
  val conf = new SparkConf(true)
  //conf.set("spark.executor.memory","100g")
  //conf.set("spark.executor.memory","20g")
  //conf.set("spark.executor.cores","5")
  conf.setMaster("yarn-client")
  /*conf.set("spark.sql.codegen.wholeStage", "true")
  conf.set("spark.sql.shuffle.partitions", "12")
  conf.set("spark.default.parallelism", "8")*/
  println("Conf created ")

  val spark = SparkSession
    .builder()
    .config(conf)
    //.master("spark://172.17.77.48:7077")
    .appName(s"morpheus-local-${UUID.randomUUID()}")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("error")

  println("Spark Session created ")

  implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)

  println("Morpheus session created ")


  val orcPgds = GraphSources.fs("hdfs://172.17.77.48:9000/user/hadoop/morpheus/LDBC3").orc
  morpheus.registerSource(Namespace("myNewGraphNameSapce"), orcPgds)
  val graph= morpheus.catalog.graph("myNewGraphNameSapce.ORC")


  //val ds=args(0)
  val ds="ldbc3"
  val fos = new FileOutputStream(new File(s"/home/hadoop/MorpheusBench/Expriments/Logs/$ds/ORC/$ds.txt"),true)

   val queries = List( //LDBCBIQueries.Q1,
                      //LDBCBIQueries.Q2,
                      //LDBCBIQueries.Q3,
                      //LDBCBIQueries.Q4,
                      //LDBCBIQueries.Q5,
                      LDBCBIQueries.Q6,
                      LDBCBIQueries.Q7,
                      LDBCBIQueries.Q8,
                      LDBCBIQueries.Q9,
                      LDBCBIQueries.Q12,
                      LDBCBIQueries.Q14,
                      LDBCBIQueries.Q15,
                      LDBCBIQueries.Q17,
                      LDBCBIQueries.Q18,
                      LDBCBIQueries.Q19,
                      LDBCBIQueries.Q20,
                      LDBCBIQueries.Q21,
                      LDBCBIQueries.Q22,
                      LDBCBIQueries.Q23,
                      LDBCBIQueries.Q24)

 // val queries = List( LDBCBIQueries.Q5)
  var count = 1
  for (query <- queries)
  {
    val start_time=System.nanoTime()
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
