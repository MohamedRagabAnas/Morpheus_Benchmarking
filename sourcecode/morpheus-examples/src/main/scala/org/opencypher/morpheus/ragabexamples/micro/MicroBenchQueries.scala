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


object MicroBenchQueries {


/////////////////////////// Graph Statistics/////////////////////////////

//Total number of Nodes
  val  Q1 =
    """
    |MATCH (n)
    |RETURN count(n) as count
    """.stripMargin

  // Total number of Relationships
  val  Q2 =
    """
    |MATCH ()-[r]->()
    |RETURN count(r) as count
    """.stripMargin

// Existing edge labels (no duplicates)

  val  Q3 =
    """
    |MATCH (n)-[r]-(m) RETURN distinct type(r)
    """.stripMargin




/////////////////////////// Search by Property /////////////////////////////

// Nodes with property Name=Value

  val  Q4 =
    """
    |MATCH (p:Person {gender: 'male'}) RETURN p
    """.stripMargin

//Edges with property Name=Value

  val  Q5 =
    """
    |MATCH ()-[r:STUDY_AT{classYear:2010}]->() RETURN r
    """.stripMargin

/////////////////////////// Search by Label /////////////////////////////

  //Edges with Label l

  val  Q6 =
    """
    |MATCH ()-[r:STUDY_AT]->() RETURN r
    """.stripMargin

/////////////////////////// Search by ID /////////////////////////////

  //The node with identifier id

  val  Q7 =
    """
    |MATCH (n {id:'1374389535138'}) RETURN n
    """.stripMargin


/////////////////////////// Traversals (Direct Neighbors) /////////////////////////////


  //Nodes adjacent to a specific node (identified wih ID) via incoming edges

  val  Q8 =
    """
    |MATCH (n {id:'1374389535138'})<-[r]-(m) RETURN m
    """.stripMargin

  //Nodes adjacent to a specific node (identified wih ID) via outgoing edges

  val  Q9 =
    """
    |MATCH (n {id:'1374389535138'})-[r]->(m) RETURN m
    """.stripMargin

// Nodes adjacent to a specific node (identified wih ID) via edges labeled "l"  (filtering on the label)
  val  Q10 =
    """
    |MATCH (n {id:'1374389535138'}) -[r:HAS_CREATOR]-(m) RETURN m
    """.stripMargin


/////////////////////////// Traversals (Node Edge-Labels) /////////////////////////////

// Labels of incoming edges of a specific node (identified wih ID)(no dupl.)
  val  Q11 =
    """
    |MATCH (n {id:'1374389535138'})<-[r]-(m)  RETURN DISTINCT type(r), count(r)
    """.stripMargin

// Labels of outgoing edges of a specific node (identified wih ID) (no dupl.)
  val  Q12 =
    """
    |MATCH (n {id:'1374389535138'})-[r]->(m)  RETURN DISTINCT type(r), count(r)
    """.stripMargin

// Labels of  edges of a specific node (identified wih ID) (no dupl.)
  val  Q13 =
    """
    |MATCH (n {id:'1374389535138'})-[r]-(m) RETURN DISTINCT type(r), count(r)
    """.stripMargin



/////////////////////////// Traversals (K-Degree Search) /////////////////////////////

// Nodes of at least k-incoming-degree
  val  Q14 =
    """
    //|MATCH (n) WHERE size( (n)<--() ) > 50 RETURN n
      |MATCH (n)<-[r]-()  WITH n, count(r) as cnt WHERE cnt> 50 RETURN n
    """.stripMargin

// Nodes of at least k-outgoing-degree
  val  Q15 =
    """
   // |MATCH (n) WHERE size( (n)-->() ) > 50 RETURN n   // Size (Boolean) is not supported in Morpheus yet, workarround -->
      |MATCH (n)-[r]->()  WITH n, count(r) as cnt WHERE cnt> 50 RETURN n
    """.stripMargin

// Nodes of at least k-degree
  val  Q16 =
    """
    |MATCH (n)-[r]-()  WITH n, count(r) as cnt WHERE cnt> 50 RETURN n
    """.stripMargin

//Nodes having at least one incoming edge
  val  Q17 =
    """
    |MATCH (n)<-[r]-()  WITH n, count(r) as cnt WHERE cnt>= 1 RETURN n
    """.stripMargin


}