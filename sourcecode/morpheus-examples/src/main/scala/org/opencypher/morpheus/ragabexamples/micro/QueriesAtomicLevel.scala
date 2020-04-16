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


object QueriesAtomicLevel {



  //All node Scan Query
  val Q1= "MATCH (m:Person)  RETURN m"



  //Predicates Adding
  val Q2= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m"

  // Adding more predicates
  val Q3="MATCH (m:Message {browserUsed:'Opera'}) WHERE m.length > 10  RETURN m"

  //Projection 1 column Adding, with predicates
  val Q4= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.length"


  //Projection 2 columns Adding, with predicates
  val Q5= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.creationDate,m.length"


  //Projection 3  columns Adding, with predicates
  val Q6= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.creationDate,m.length,m.locationIP"



  //Projection 1 column Adding, without predicates
  val Q7= "MATCH (m:Message) RETURN m.length"



  //Projection 2 columns Adding, without predicates
  val Q8= "MATCH (m:Message) RETURN m.creationDate,m.length"


  //Projection 3  columns Adding, without predicates
  val Q9= "MATCH (m:Message) RETURN m.creationDate,m.length,m.locationIP"



  //Expand with one join

  val Q10="MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag) RETURN message,tag"


  //Expand with two joins

  val Q11="MATCH (message:Message)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass) RETURN message,tag,tagclass"


  //Expand with three joins

  val Q12=
    """MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass), (message)-[:HAS_CREATOR]->(person:Person)
      |RETURN person,message,tag,tagclass
    """.stripMargin


  //Expand with four joins

  val Q13=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]->(person:Person)-[:IS_LOCATED_IN]->(city:City)
      |RETURN person,city, message,tag,tagclass
    """.stripMargin


  //Expand with five joins

  val Q14=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]->(person:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)
      |RETURN person,city, country, message,tag,tagclass
    """.stripMargin


  //Expand with six joins

  val Q15=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]-(person:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)-[:IS_PART_OF]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
    """.stripMargin


  // Expand with six joins + Sorting by one variable

  val Q16=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]-(person:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)-[:IS_PART_OF]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC
    """.stripMargin


  // Expand with six joins + Sorting by two variable

  val Q17=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]-(person:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)-[:IS_PART_OF]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC, city.name DESC
    """.stripMargin


  // Expand with six joins + Sorting by three variable

  val Q18=
    """
      |MATCH (message:Message) -[:HAS_TAG]-> (tag:Tag)-[:HAS_TYPE]->(tagclass:TagClass),
      |(message)-[:HAS_CREATOR]-(person:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)-[:IS_PART_OF]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC, city.name DESC, country.name ASC
    """.stripMargin


}