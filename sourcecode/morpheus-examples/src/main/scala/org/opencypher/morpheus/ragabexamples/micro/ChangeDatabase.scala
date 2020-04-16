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

import java.io.File
import java.io.PrintWriter
import scala.io.Source

object ChangeDatabase {

  def main(args: Array[String]): Unit = {

       var dbName = args(0)
       val dbType = args(1)

       println("Started")

       if (dbType == "hive") {
         println(s"Changing $dbType db name into $dbName")

         val f1 = "/home/hadoop/morpheus-master/morpheus-examples/src/main/resources/ldbc/ddl/ldbc.ddl"  // Original File
         val f2 = new File("/home/hadoop/morpheus-master/morpheus-examples/src/main/resources/ldbc/ddl/ldbcTemp.ddl") // Temporary File
         val w = new PrintWriter(f2)

         Source.fromFile(f1).getLines
           .map { x => if(x.contains("warehouse")) s"SET SCHEMA warehouse.$dbName" else x }
           .foreach(x => w.println(x))

         w.close()
         val fileForRename = new File("/home/hadoop/morpheus-master/morpheus-examples/src/main/resources/ldbc/ddl/ldbc.ddl")
         f2.renameTo(fileForRename)
       }

       else if (dbType == "neo4j") {

         if (dbName == "ldbc1")
           dbName = "graphLDBCSF1.db"
         else if (dbName == "ldbc3")
           dbName = "graphLDBCSF3.db"
         else if (dbName == "ldbc10")
           dbName = "graphLDBCSF10.db"
         else if (dbName == "ldbc100")
           dbName = "graphLDBCSF100.db"
         else if (dbName == "ldbc300")
           dbName = "graphLDBCSF300.db"

         println(s"Changing $dbType db name into $dbName")

         val f1 = "/home/hadoop/neo4j/conf/neo4j.conf"  // Original File
         val f2 = new File("/home/hadoop/neo4j/conf/neo4jTemp.conf") // Temporary File
         val w = new PrintWriter(f2)

         Source.fromFile(f1).getLines
           .map { x => if(x.contains("active_database")) s"dbms.active_database=$dbName" else x }
           .foreach(x => w.println(x))

         w.close()
         val fileForRename = new File("/home/hadoop/neo4j/conf/neo4j.conf")
         f2.renameTo(fileForRename)
       }

       println("Finished")
  }

}


