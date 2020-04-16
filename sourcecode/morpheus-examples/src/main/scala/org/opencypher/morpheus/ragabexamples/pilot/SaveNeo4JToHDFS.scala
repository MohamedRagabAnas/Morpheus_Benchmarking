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
package org.opencypher.morpheus.ragabexamples.pilot

import java.net.URI

import org.apache.log4j.{Level, Logger}
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.util.App
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.Neo4jConfig

object SaveNeo4JToHDFS extends App {

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

  // Create MorpheusSession
  implicit val morpheus: MorpheusSession = MorpheusSession.local()



  val neo4jConfig        =Neo4jConfig(URI.create("bolt://localhost:7687"),"neo4j",Some(""))
  val neo4jNamespace     =Namespace("neo4jMorpheus")
  val neo4jSource        =GraphSources.cypher.neo4j(config = neo4jConfig)

  morpheus.catalog.register(namespace = neo4jNamespace,dataSource = neo4jSource)

  val myLDBCGraph =morpheus.catalog.graph("neo4jMorpheus.graph")


  val ldbcgraph="LDBC3"


  // Write (store) the created graph to HDFS->CSV format.
  println(s"Storing $ldbcgraph to HDFS CSV...")
  val csvPgds = GraphSources.fs(s"hdfs://172.17.77.48:9000/user/hadoop/morpheus/$ldbcgraph").csv
  morpheus.registerSource(Namespace("myLDBCGraphNSHDFSCSV"), csvPgds)
  morpheus.catalog.store("myLDBCGraphNSHDFSCSV.CSV", myLDBCGraph)
  println(s"Successfulyy Stored $ldbcgraph to HDFS CSV.")


  // Write (store) the created graph to HDFS->Parquet format.
  println(s"Storing $ldbcgraph to HDFS Parquet...")
  val parquetPgds = GraphSources.fs(s"hdfs://172.17.77.48:9000/user/hadoop/morpheus/$ldbcgraph").parquet
  morpheus.registerSource(Namespace("myLDBCGraphNSHDFSParquet"), parquetPgds)
  morpheus.catalog.store("myLDBCGraphNSHDFSParquet.Parquet", myLDBCGraph)
  println(s"Successfulyy Stored $ldbcgraph to HDFS Parquet.")


  // Write (store) the created graph to HDFS->ORC format.
  println(s"Storing $ldbcgraph to HDFS ORC...")
  val orcPgds = GraphSources.fs(s"hdfs://172.17.77.48:9000/user/hadoop/morpheus/$ldbcgraph").orc
  morpheus.registerSource(Namespace("myLDBCGraphNSHDFSORC"), orcPgds)
  morpheus.catalog.store("myLDBCGraphNSHDFSORC.ORC", myLDBCGraph)
  println(s"Successfulyy Stored $ldbcgraph to HDFS ORC.")

}
