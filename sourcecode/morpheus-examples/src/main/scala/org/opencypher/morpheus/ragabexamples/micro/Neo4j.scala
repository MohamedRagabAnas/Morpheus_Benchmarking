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
import java.net.URI

import org.apache.log4j.{Level, Logger}
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.util.App
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.neo4j.io.Neo4jConfig

object Neo4j extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create Morpheus session
  implicit val morpheus: MorpheusSession = MorpheusSession.local()


  val neo4jConfig        =Neo4jConfig(URI.create("bolt://localhost:7687"),"neo4j",Some(""))
  val neo4jNamespace     =Namespace("neo4jMorpheus")
  val neo4jSource        =GraphSources.cypher.neo4j(config = neo4jConfig)

  morpheus.catalog.register(namespace = neo4jNamespace, dataSource = neo4jSource)

  //val ds=args(0)
  val ds="ldbc3"
  val fos = new FileOutputStream(new File(s"/home/hadoop/MorpheusBench/Expriments/Logs/$ds/Neo4j/$ds.txt"),true)

  val queries = List(QueriesAtomicLevel.Q1,
                     QueriesAtomicLevel.Q2,
                     QueriesAtomicLevel.Q3,
                     QueriesAtomicLevel.Q4,
                     QueriesAtomicLevel.Q5,
                     QueriesAtomicLevel.Q6,
                     QueriesAtomicLevel.Q7,
                     QueriesAtomicLevel.Q8,
                     QueriesAtomicLevel.Q9,
                     QueriesAtomicLevel.Q10,
                     QueriesAtomicLevel.Q11,
                     QueriesAtomicLevel.Q12,
                     QueriesAtomicLevel.Q13,
                     QueriesAtomicLevel.Q14,
                     QueriesAtomicLevel.Q15,
                     QueriesAtomicLevel.Q16,
                     QueriesAtomicLevel.Q17,
                     QueriesAtomicLevel.Q18)


  var count = 1
  for (query <- queries)
  {
    val start_time=System.nanoTime()
    morpheus.cypher("FROM Graph neo4jMorpheus.graph "+ query).records.show
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