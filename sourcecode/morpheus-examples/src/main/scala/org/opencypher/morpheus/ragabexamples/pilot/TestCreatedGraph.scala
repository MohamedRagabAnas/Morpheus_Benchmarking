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

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.util.App
import org.opencypher.okapi.api.graph.Namespace

/**
  * Demonstrates basic usage of the Morpheus API by loading an example graph from [[DataFrame]]s.
  */
object TestCreatedGraph extends App {


  // 1) Create Morpheus session
  val conf = new SparkConf(true)
  //conf.set("spark.driver.memory","100g")
  //conf.set("spark.executor.memory","20g")
  //conf.set("spark.executor.cores","5")
  //conf.set("spark.executor.instances","21")

  conf.setMaster("yarn-client")
  //conf.set("spark.sql.codegen.wholeStage", "true")
  //conf.set("spark.sql.shuffle.partitions", "12")
  //conf.set("spark.default.parallelism", "8")
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


  val csvPgds = GraphSources.fs("hdfs://172.17.77.48:9000/user/hadoop/morpheus/LDBC3").orc
  morpheus.registerSource(Namespace("myNewGraphNameSapce"), csvPgds)
  val graph= morpheus.catalog.graph("myNewGraphNameSapce.ORC")






val  Q1 =
      """
        |MATCH (message:Message)
        |WHERE message.creationDate < 1313591219961
        |WITH count(message) AS totalMessageCountInt
        |WITH toFloat(totalMessageCountInt) AS totalMessageCount
        |MATCH (message:Message)
        |WHERE message.creationDate < 1313591219961
        |  AND message.content IS NOT NULL
        |WITH
        |  totalMessageCount,
        |  message,
        |  (message.creationDate/31556952000 + 1970) AS year
        |WITH
        |  totalMessageCount,
        |  year,
        |  message:Comment AS isComment,
        |  CASE
        |    WHEN message.length <  40 THEN 0
        |    WHEN message.length <  80 THEN 1
        |    WHEN message.length < 160 THEN 2
        |    ELSE                           3
        |  END AS lengthCategory,
        |  count(message) AS messageCount,
        |  floor(avg(message.length)) AS averageMessageLength,
        |  sum(message.length) AS sumMessageLength
        |RETURN
        |  year,
        |  isComment,
        |  lengthCategory,
        |  messageCount,
        |  averageMessageLength,
        |  sumMessageLength,
        |  messageCount / totalMessageCount AS percentageOfMessages
        |ORDER BY
        |  year DESC,
        |  isComment ASC,
        |lengthCategory ASC
      """.stripMargin




val  Q2 =
      """
          |MATCH
          |  (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)
          |  <-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(tag:Tag)
          |WHERE message.creationDate >= 1313591219961
          |  AND message.creationDate <= 2013591219961
          |  AND (country.name = 'Japan' OR country.name = 'Pakistan')
          |WITH
          |  country.name AS countryName,
          |  message.creationDate AS month,
          |  person.gender AS gender,
          |  tag.name AS tagName,
          |  message
          |WITH
          |  countryName, month, gender,tagName, count(message) AS messageCount
          |WHERE messageCount >= 1
          |RETURN
          |  countryName,
          |  month,
          |  gender,
          |  tagName,
          |  messageCount
          |ORDER BY
          |  messageCount DESC,
          |  tagName ASC,
          |  gender ASC,
          |  month ASC,
          |  countryName ASC
          |LIMIT 100
        """.stripMargin


val  Q3 =
    """
      |WITH
        2010 AS year1,
        10 AS month1,
        2010 + toInteger(10 / 12.0) AS year2,
        (10-(10/12)*12)  +1  AS month2
      // year-month 1
      MATCH (tag:Tag)
      OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
        WHERE (message1.creationDate/31556952000+1970)   = year1
          AND (message1.creationDate-(message1.creationDate/31556952000)*31556952000)/2592000000 +1 = month1
      WITH year2, month2, tag, count(message1) AS countMonth1
      // year-month 2
      OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
        WHERE (message2.creationDate/31556952000+1970)   = year2
          AND (message2.creationDate-(message2.creationDate/31556952000)*31556952000)/2592000000 +1 = month2
      WITH
        tag,
        countMonth1,
        count(message2) AS countMonth2
      RETURN
        tag.name,
        countMonth1,
        countMonth2,
        abs(countMonth1-countMonth2) AS diff
      ORDER BY
        diff DESC,
        tag.name ASC
      LIMIT 100
    """.stripMargin



  val  Q4 =
      """
        |MATCH
        |  (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
        |  (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
        |  (post:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass)
        |RETURN
        |  forum.title,
        |  forum.creationDate,
        |  person.firstName,
        |  count(DISTINCT post) AS postCount
        |ORDER BY
        |  postCount DESC,
        |  forum.title ASC LIMIT 20
      """.stripMargin


val  Q5 =
      """
        |MATCH
        |  (:Country {name: 'Japan'})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
        |  (person:Person)<-[:HAS_MEMBER]-(forum:Forum)
        |WITH forum, count(person) AS numberOfMembers
        |ORDER BY numberOfMembers DESC, forum.id ASC
        |LIMIT 100
        |WITH collect(forum) AS popularForums
        |UNWIND popularForums AS forum
        |MATCH
        |  (forum)-[:HAS_MEMBER]->(person:Person)
        |OPTIONAL MATCH
        |  (person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(popularForum:Forum)
        |WHERE popularForum IN popularForums
        |RETURN
        |  person.firstName,
        |  person.lastName,
        |  person.creationDate,
        |  count(DISTINCT post) AS postCount
        |ORDER BY
        |  postCount DESC,
        |  person.firstName ASC
        |LIMIT 10
      """.stripMargin


      val  Q6 =
      """
        |
        |MATCH
        | (tag:Tag {name: 'Adolf_Hitler'})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
        |OPTIONAL MATCH (:Person)-[like:LIKES]->(message)
        |OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Comment)
        |WITH person, count(DISTINCT like) AS likeCount, count(DISTINCT comment) AS replyCount, count(DISTINCT message) AS messageCount
        |RETURN
        |  person.firstName,
        |  replyCount,
        |  likeCount,
        |  messageCount,
        |  1*messageCount + 2*replyCount + 10*likeCount AS score
        |ORDER BY
        |  score DESC,
        |  person.firstName ASC
        |LIMIT 100
      """.stripMargin

      val  Q7 =
      """
        |MATCH
        | (tag:Tag {name: 'Adolf_Hitler'})
        |MATCH (tag)<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
        |MATCH (tag)<-[:HAS_TAG]-(message2:Message)-[:HAS_CREATOR]->(person1)
        |OPTIONAL MATCH (message2)<-[:LIKES]-(person2:Person)
        |OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
        |RETURN
        |  person1.firstName,
        |  count(DISTINCT like) AS authorityScore
        |ORDER BY
        |  authorityScore DESC,
        |  person1.firstName ASC
        |LIMIT 100
      """.stripMargin

      val  Q8 =
      """
        |MATCH
        |  (tag:Tag {name: 'Adolf_Hitler'})<-[:HAS_TAG]-(message:Message),
        |  (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
        |WHERE NOT (comment)-[:HAS_TAG]->(tag)
        |RETURN
        |  relatedTag.name,
        |  count(DISTINCT comment) AS count
        |ORDER BY
        |  count DESC,
        |  relatedTag.name ASC
        |LIMIT 100
      """.stripMargin


      val  Q9 =
      """
        |MATCH
        |  (forum:Forum)-[:HAS_MEMBER]->(person:Person)
        |WITH forum, count(person) AS members
        |WHERE members >= 10
        |MATCH
        |  (forum)-[:CONTAINER_OF]->(post1:Post)-[:HAS_TAG]->
        |  (:Tag)-[:HAS_TYPE]->(:TagClass {name:'Actor'})
        |WITH forum, count(DISTINCT post1) AS count1
        |MATCH
        |  (forum)-[:CONTAINER_OF]->(post2:Post)-[:HAS_TAG]->
        |  (:Tag)-[:HAS_TYPE]->(:TagClass {name:'TennisPlayer'})
        |WITH forum, count1, count(DISTINCT post2) AS count2
        |RETURN
        |  forum.title,
        |  count1,
        |  count2
        |ORDER BY
        |  abs(count2-count1) DESC,
        |  forum.title ASC
        |LIMIT 100
      """
      .stripMargin



      val  Q12 =
            """
            |MATCH
            |  (message:Message)-[:HAS_CREATOR]->(creator:Person),
            |  (message)<-[like:LIKES]-(:Person)
            |WHERE message.creationDate > 1313591219961
            |WITH message, creator, count(like) AS likeCount
            |WHERE likeCount > 20
            |RETURN
            |  message.content,
            |  message.creationDate,
            |  creator.firstName,
            |  creator.lastName,
            |  likeCount
            |ORDER BY
            |  likeCount DESC,
            |  message.content ASC
            |LIMIT 100
          """.stripMargin


      val  Q14 =
          """
          |MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF]-(reply:Message)
          |WHERE  post.creationDate >= 1313591219961
          |  AND  post.creationDate <= 2013591219961
          |  AND reply.creationDate >= 1313591219961
          |  AND reply.creationDate <= 2013591219961
          |RETURN
          |  person.firstName,
          |  person.lastName,
          |  count(DISTINCT post) AS threadCount,
          |  count(DISTINCT reply) AS messageCount
          |ORDER BY
          |  messageCount DESC,
          |  person.firstName ASC
          |LIMIT 100
          """.stripMargin


      val  Q15 =
        """
        |MATCH
        |  (country:Country {name:'India'})
        |MATCH
        |  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person1:Person)
        |OPTIONAL MATCH
        |  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(friend1:Person),
        |  (person1)-[:KNOWS]-(friend1)
        |WITH country, person1, count(friend1) AS friend1Count
        |WITH country, avg(friend1Count) AS socialNormalFloat
        |WITH country, floor(socialNormalFloat) AS socialNormal
        |MATCH
        |  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person2:Person)
        |OPTIONAL MATCH
        |  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(friend2:Person)-[:KNOWS]-(person2)
        |WITH country, person2, count(friend2) AS friend2Count, socialNormal
        |WHERE friend2Count = socialNormal
        |RETURN
        |  person2.firstName,
        |  friend2Count AS count
        |ORDER BY
        |  person2.firstName ASC
        |LIMIT 100
      """.stripMargin


      val  Q17 =
      """
      |MATCH (country:Country )
      |MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
      |MATCH (b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
      |MATCH (c:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
      |MATCH (a)-[:KNOWS]-(b), (b)-[:KNOWS]-(c), (c)-[:KNOWS]-(a)
      |WHERE a.id < b.id
      |  AND b.id < c.id
      |RETURN count(*) AS count
      """.stripMargin


      val  Q18 =  """
      |MATCH (person:Person)
      |OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF]->(post:Post)
      |WHERE message.content IS NOT NULL
      |  AND message.length < 10
      |  AND message.creationDate > 1290407375701
      |  AND post.language IN ['tk']
      |WITH
      |  person,
      |  count(message) AS messageCount
      |RETURN
      |  messageCount,
      |  count(person) AS personCount
      |ORDER BY
      |  personCount DESC,
      |messageCount DESC
    """.stripMargin


    val Q19 =
    """
    |MATCH
    |  (:TagClass {name:'Senator'})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-
    |  (forum1:Forum)-[:HAS_MEMBER]->(stranger:Person)
    |WITH DISTINCT stranger
    |MATCH
    |  (:TagClass {name:'TennisPlayer'})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-
    |  (forum2:Forum)-[:HAS_MEMBER]->(stranger)
    |WITH DISTINCT stranger
    |MATCH
    |  (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(message:Message)-[:HAS_CREATOR]->(stranger)
    |WHERE person.birthday > 1013591219961
    |  AND person <> stranger
    |  AND NOT (person)-[:KNOWS]-(stranger)
    |  AND NOT (message)-[:REPLY_OF]->(:Message)-[:HAS_CREATOR]->(stranger)
    |RETURN
    |  person.firstName,
    |  count(DISTINCT stranger) AS strangersCount,
    |  count(comment) AS interactionCount
    |ORDER BY
    |  interactionCount DESC,
    |  person.firstName ASC
    |LIMIT 100
  """.stripMargin





    // BI Q20
    // Not Working because of the unbound-variable [:issubclassof*0..] not already supported by CAPS
    // But working with removing the unbound-variable [:issubclassof*0..] to be just direct relationship [:issubclassof]


  val  Q20 =
    """
      |UNWIND ['MusicalWork','PopulatedPlace', 'Cleric' , 'Agent' ] AS tagClassName
      |MATCH
      |  (tagClass:TagClass {name: tagClassName})<-[:IS_SUBCLASS_OF]-
      |  (:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
      |RETURN
      |  tagClass.name,
      |  count(DISTINCT message) AS messageCount
      |ORDER BY
      |  messageCount DESC,
      |  tagClass.name ASC
      |LIMIT 100
    """.stripMargin




    //BI Q 21
    // Working but need to check it again because zombie in the return is null ?!!!!

    val  Q21 =
    """
      |MATCH (country:Country {name:'China'})
      |WITH
      |     country,
      |     2015   AS endDateYear,
      |      6     AS endDateMonth
      |MATCH
      |  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
      |OPTIONAL MATCH
      |  (zombie)<-[:HAS_CREATOR]-(message:Message)
      |WHERE zombie.creationDate  < 2013591219961
      |  AND message.creationDate < 1513591219961
      |WITH
      |  country,
      |  zombie,
      |  endDateYear,
      |  endDateMonth,
      |  (zombie.creationDate/31556952000+1970)  AS zombieCreationYear,
      |  (zombie.creationDate-(zombie.creationDate/31556952000) * 31556952000)/2592000000+1  AS zombieCreationMonth,
      |  count(message) AS messageCount
      |WITH
      |  country,
      |  zombie,
      |  12 * (endDateYear  - zombieCreationYear)
      |     + (endDateMonth - zombieCreationMonth)
      |     + 1 AS months,
      |  messageCount
      |WHERE messageCount / months < 1
      |WITH
      |  country,
      |  collect(zombie) AS zombies
      |UNWIND zombies AS zombie
      |OPTIONAL MATCH
      |  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
      |WHERE likerZombie IN zombies
      |WITH
      |  zombie,
      |  count(likerZombie) AS zombieLikeCount
      |OPTIONAL MATCH
      |  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
      |WHERE likerPerson.creationDate < 2013591219961
      |WITH
      |  zombie,
      |  zombieLikeCount,
      |  count(likerPerson) AS totalLikeCount
      |RETURN
      |  zombie.id,
      |  zombieLikeCount,
      |  totalLikeCount,
      |  CASE totalLikeCount
      |    WHEN 0 THEN 0.0
      |    ELSE zombieLikeCount / toFloat(totalLikeCount)
      |  END AS zombieScore
      |ORDER BY
      |  zombieScore DESC,
      |  zombie.firstName ASC
      |LIMIT 100
    """.stripMargin



  // BI  Q22
  // It's very very expensive query :( But still Running

  val  Q22 =
    """
 |MATCH
 |  (country1:Country{name:'China'})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person),
 |  (country2:Country{name:'Germany'})<-[:IS_PART_OF]-(city2:City)<-[:IS_LOCATED_IN]-(person2:Person)
 |WITH person1, person2, city1, 0 AS score
 |// subscore 1
 |OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Message)-[:HAS_CREATOR]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE WHEN c is null THEN 0 ELSE  4 END) AS score
 |// subscore 2
 |OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE  WHEN m is null THEN 0 ELSE  1 END) AS score
 |// subscore 3
 |OPTIONAL MATCH (person1)-[k:KNOWS]-(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE  WHEN k is null THEN 0 ELSE 15 END) AS score
 |// subscore 4
 |OPTIONAL MATCH (person1)-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE  WHEN m is null THEN 0 ELSE 10 END) AS score
 |// subscore 5
 |OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:LIKES]-(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE  WHEN m is null THEN 0 ELSE  1 END) AS score
 |// preorder
 |ORDER BY
 |  city1.name ASC,
 |  score DESC,
 |  person1.firstName ASC,
 |  person2.firstName ASC
 |WITH
 |  city1,
 |  // using a list might be faster, but the browser query editor does not like it
 |  collect({score: score, person1: person1, person2: person2})[0] AS top
 |RETURN
 |
 |  top.person1.firstName,
 |  city1.name,
 |  top.score
 |ORDER BY
 |  top.score DESC,
 |  top.person1.id ASC,
 |top.person2.id ASC
      """.stripMargin





  //Q23 Running :)

  val  Q23=
    """
      |MATCH
      |  (home:Country{name:'Germany'})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
      |  (:Person)<-[:HAS_CREATOR]-(message:Message)-[:IS_LOCATED_IN]->(destination:Country)
      |WHERE home <> destination
      |WITH
      |  message,
      |  destination,
        (message.creationDate-(message.creationDate/31556952000)*31556952000)/2592000000 +1 AS month
      |
      |RETURN
      |  count(message) AS messageCount,
      |  destination.name,
      |  month
      |ORDER BY
      |  messageCount DESC,
      |  destination.name ASC,
      |  month ASC
      |LIMIT 100
    """.stripMargin



  val  Q24 =
    """
      |MATCH (:TagClass {name:'Single'})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(message:Message)
      |WITH DISTINCT message
      |MATCH (message)-[:IS_LOCATED_IN]->(:Country)-[:IS_PART_OF]->(continent:Continent)
      |OPTIONAL MATCH (message)<-[like:LIKES]-(:Person)
      |WITH
      |  message,
      |  (message.creationDate/31556952000+1970) AS year,
      |  (message.creationDate-(message.creationDate/31556952000)*31556952000)/2592000000 +1 AS month,
      |  like,
      |  continent
      |RETURN
      |  count(DISTINCT message) AS messageCount,
      |  count(like) AS likeCount,
      |  year,
      |  month,
      |  continent.name
      |ORDER BY
      |  year ASC,
      |  month ASC,
      |  continent.name DESC
      |LIMIT 100
    """.stripMargin


  val result = graph.cypher("MATCH (p:Person)-[:IS_LOCATED_IN]->(c:City)   RETURN p,c limit 1")
  //val result = graph.cypher(Q4)
  result.records.show
}
