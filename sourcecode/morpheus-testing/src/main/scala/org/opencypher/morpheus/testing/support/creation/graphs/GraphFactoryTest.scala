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
package org.opencypher.morpheus.testing.support.creation.graphs

import java.sql.Date

import org.apache.spark.sql.Row
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.morpheus.api.value.MorpheusElement._
import org.opencypher.morpheus.impl.MorpheusConverters._
import org.opencypher.morpheus.schema.MorpheusSchema._
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.morpheus.testing.fixture.RecordsVerificationFixture
import org.opencypher.morpheus.testing.support.GraphMatchingTestSupport
import org.opencypher.okapi.api.graph.{NodeRelPattern, TripletPattern}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory

abstract class GraphFactoryTest extends MorpheusTestSuite with GraphMatchingTestSupport with RecordsVerificationFixture  {
  def factory: TestGraphFactory

  val createQuery: String =
    """
      |CREATE (max:Person:Astronaut {name: "Max", birthday: date("1991-07-10")})
      |CREATE (martin:Person:Martian {name: "Martin"})
      |CREATE (swedish:Language {title: "Swedish"})
      |CREATE (german:Language {title: "German"})
      |CREATE (orbital:Language {title: "Orbital"})
      |CREATE (max)-[:SPEAKS]->(swedish)
      |CREATE (max)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(orbital)
    """.stripMargin

  val personAstronautTable: MorpheusElementTable = MorpheusElementTable.create(NodeMappingBuilder
    .on("ID")
    .withImpliedLabels("Person", "Astronaut")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("birthday" -> "BIRTHDAY")
    .build, morpheus.sparkSession.createDataFrame(
    Seq((0L, "Max", Date.valueOf("1991-07-10")))).toDF("ID", "NAME", "BIRTHDAY"))

  val personMartianTable: MorpheusElementTable = MorpheusElementTable.create(NodeMappingBuilder
    .on("ID")
    .withImpliedLabels("Person", "Martian")
    .withPropertyKey("name" -> "NAME")
    .build, morpheus.sparkSession.createDataFrame(
    Seq((1L, "Martin"))).toDF("ID", "NAME"))

  val languageTable: MorpheusElementTable = MorpheusElementTable.create(NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Language")
    .withPropertyKey("title" -> "TITLE")
    .build, morpheus.sparkSession.createDataFrame(
    Seq(
      (2L, "Swedish"),
      (3L, "German"),
      (4L, "Orbital"))
  ).toDF("ID", "TITLE"))

  val knowsScan: MorpheusElementTable = MorpheusElementTable.create(RelationshipMappingBuilder
    .on("ID")
    .from("SRC").to("DST").relType("KNOWS").build, morpheus.sparkSession.createDataFrame(
    Seq(
      (0L, 5L, 2L),
      (0L, 6L, 3L),
      (1L, 7L, 3L),
      (1L, 8L, 4L))
  ).toDF("SRC", "ID", "DST"))

  test("testSchema") {
    val propertyGraph = CreateGraphFactory(createQuery)
    factory(propertyGraph).schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("Person", "Astronaut")("name" -> CTString, "birthday" -> CTDate)
      .withNodePropertyKeys("Person", "Martian")("name" -> CTString)
      .withNodePropertyKeys("Language")("title" -> CTString)
      .withRelationshipType("SPEAKS")
      .asMorpheus)
  }

  test("testAsScanGraph") {
    val propertyGraph = CreateGraphFactory(createQuery)
    val g = factory(propertyGraph).asMorpheus
    g shouldMatch morpheus.graphs.create(personAstronautTable, personMartianTable, languageTable, knowsScan)
  }

  it("can create graphs containing list properties") {
    val propertyGraph = CreateGraphFactory(
      """
        |CREATE ( {l: [1,2,3]} )
      """.stripMargin)

    val g = factory(propertyGraph).asMorpheus

    g.cypher("MATCH (n) RETURN n.l as list").records.toMaps should equal(Bag(
      CypherMap("list" -> List(1,2,3))
    ))
  }

  it("can handle nodes with the same label but different properties") {
    val propertyGraph = CreateGraphFactory(
      """
        |CREATE ( { } )
        |CREATE ( {val1: 1} )
        |CREATE ( {val1: 1, val2: "foo"} )
      """.stripMargin)

    val g = factory(propertyGraph).asMorpheus

    g.cypher("MATCH (n) RETURN n.val1, n.val2").records.toMaps should equal(Bag(
      CypherMap("n.val1" -> 1,    "n.val2" -> "foo"),
      CypherMap("n.val1" -> 1,    "n.val2" -> null),
      CypherMap("n.val1" -> null, "n.val2" -> null)
    ))
  }

  it("extracts additional patterns"){
    val nodeRelPattern = NodeRelPattern(CTNode("Person", "Martian"), CTRelationship("SPEAKS"))
    val tripletPattern = TripletPattern(CTNode("Person", "Martian"), CTRelationship("SPEAKS"), CTNode("Language"))

    val propertyGraph = CreateGraphFactory(createQuery)
    val g = factory(propertyGraph, Seq(nodeRelPattern, tripletPattern)).asMorpheus

    g.patterns should contain(nodeRelPattern)
    g.patterns should contain(tripletPattern)

    {
      val nodeVar = nodeRelPattern.nodeElement.toVar
      val relVar = nodeRelPattern.relElement.toVar

      val cols = Seq(
        nodeVar,
        HasLabel(nodeVar, Label("Person")),
        HasLabel(nodeVar, Label("Martian")),
        ElementProperty(nodeVar, PropertyKey("name"))(CTString),
        relVar,
        HasType(relVar, RelType("SPEAKS")),
        StartNode(relVar)(CTAny),
        EndNode(relVar)(CTAny)
      )

      val data = Bag(
        Row(1L.encodeAsMorpheusId.toList, true, true, "Martin", 7L.encodeAsMorpheusId.toList, true, 1L.encodeAsMorpheusId.toList, 3L.encodeAsMorpheusId.toList),
        Row(1L.encodeAsMorpheusId.toList, true, true, "Martin", 8L.encodeAsMorpheusId.toList, true, 1L.encodeAsMorpheusId.toList, 4L.encodeAsMorpheusId.toList)
      )

      val scan = g.scanOperator(nodeRelPattern)
      val result = morpheus.records.from(scan.header, scan.table)
      verify(result, cols, data)
    }

    {
      val sourceVar = tripletPattern.sourceElement.toVar
      val targetVar = tripletPattern.targetElement.toVar
      val relVar = tripletPattern.relElement.toVar

      val cols = Seq(
        sourceVar,
        HasLabel(sourceVar, Label("Person")),
        HasLabel(sourceVar, Label("Martian")),
        ElementProperty(sourceVar, PropertyKey("name"))(CTString),
        relVar,
        HasType(relVar, RelType("SPEAKS")),
        StartNode(relVar)(CTAny),
        EndNode(relVar)(CTAny),
        targetVar,
        HasLabel(targetVar, Label("Language")),
        ElementProperty(targetVar, PropertyKey("title"))(CTString)
      )

      val data = Bag(

        Row(1L.encodeAsMorpheusId.toList, true, true, "Martin", 8L.encodeAsMorpheusId.toList, true, 1L.encodeAsMorpheusId.toList, 4L.encodeAsMorpheusId.toList, 4L.encodeAsMorpheusId.toList, true, "Orbital"),
        Row(1L.encodeAsMorpheusId.toList, true, true, "Martin", 7L.encodeAsMorpheusId.toList, true, 1L.encodeAsMorpheusId.toList, 3L.encodeAsMorpheusId.toList, 3L.encodeAsMorpheusId.toList, true, "German")
      )

      val scan = g.scanOperator(tripletPattern)
      val result = morpheus.records.from(scan.header, scan.table)
      verify(result, cols, data)
    }
  }
}

class ScanGraphFactoryTest extends GraphFactoryTest {
  override def factory: TestGraphFactory = ScanGraphFactory
}
