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
package org.opencypher.morpheus.impl.acceptance

import org.opencypher.morpheus.api.value.MorpheusElement._
import org.opencypher.morpheus.api.value.{MorpheusNode, MorpheusRelationship}
import org.opencypher.morpheus.impl.MorpheusConverters._
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.morpheus.schema.MorpheusSchema._
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.schema.{PropertyGraphSchema, PropertyKeys}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.graph.UnionGraph
import org.opencypher.okapi.relational.impl.operators.SwitchContext
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._

import scala.language.existentials

class MultipleGraphTests extends MorpheusTestSuite with ScanGraphInit {

  def testGraph1: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph("CREATE (:Person {name: 'Phil'})")

  def testGraph3: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph("CREATE (:Car {type: 'Toyota'})")

  def testGraphRels: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(
    """|CREATE (mats:Person {name: 'Mats'})
       |CREATE (max:Person {name: 'Max'})
       |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
    """.stripMargin)

  it("can read graph via parameter") {
    val graph = initGraph(
      """|CREATE (:A {v: 1})
         |CREATE (:B {v: 100})""".stripMargin)
    morpheus.catalog.store("g1", graph)

    val query =
      """|FROM GRAPH $graphParam
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query, CypherMap("graphParam" -> "g1"))

    result.graph.nodes("n").size shouldBe 2
  }

  it("creates multiple copies of the same node") {
    val g = morpheus.cypher(
      """
        |CONSTRUCT
        |  CREATE ()
        |RETURN GRAPH
      """.stripMargin).graph
    val results = g.cypher(
      """
        |MATCH (a)
        |CONSTRUCT
        |  CREATE (f COPY OF a)-[:FOO]->(g COPY OF a)
        |MATCH (n)
        |RETURN n
      """.stripMargin).records

    results.size shouldBe 2
  }

  it("can match on constructed graph") {
    val results = morpheus.cypher(
      """
        |CONSTRUCT
        |  CREATE ()
        |MATCH (a)
        |CONSTRUCT
        |  CREATE (f COPY OF a)-[:FOO]->(g COPY OF a)
        |MATCH (n)
        |RETURN n
      """.stripMargin).records

    results.size shouldBe 2
  }

  //TODO: This test has no useful expectation
  it("CLONEs with an alias") {
    val query =
      """
        |MATCH (n)
        |CONSTRUCT
        |  CLONE n AS m
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords should be(None)
  }

  it("should return a graph") {
    val query =
      """RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords should be(None)

    result.graph.asMorpheus shouldMatch testGraph1
  }

  it("should switch to another graph and then return it") {
    morpheus.catalog.store("graph2", testGraph2)
    val query =
      """FROM GRAPH graph2
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords shouldBe None
    result.graph.asMorpheus shouldMatch testGraph2
  }

  it("can select a source graph to match data from") {
    morpheus.catalog.store("graph1", testGraph1)
    morpheus.catalog.store("graph2", testGraph2)

    val query =
      """FROM GRAPH graph2
        |MATCH (n:Person)
        |RETURN n.name AS name""".stripMargin

    val result = testGraph1.cypher(query)

    result.records.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil")
      ))
  }

  it("matches from different graphs") {
    morpheus.catalog.store("graph1", testGraph1)
    morpheus.catalog.store("graph2", testGraph2)
    morpheus.catalog.store("graph3", testGraph3)

    val query =
      """FROM GRAPH graph2
        |MATCH (n:Person)
        |WITH n.name AS name
        |FROM GRAPH graph3
        |MATCH (c:Car)
        |RETURN name, c.type AS car""".stripMargin

    val result = testGraph1.cypher(query)

    result.records.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil", "car" -> "Toyota")
      ))
  }

  it("constructs from different graphs with multiple distinct nodes") {
    val g1 = initGraph(
      """|CREATE (:A {v: 1})
         |CREATE (:B {v: 100})""".stripMargin)
    val g2 = initGraph(
      """|CREATE (:A {v: 2})
         |CREATE (:B {v: 200})""".stripMargin)
    morpheus.catalog.store("g1", g1)
    morpheus.catalog.store("g2", g2)

    val query =
      """|FROM GRAPH g1
         |MATCH (n)
         |FROM GRAPH g2
         |MATCH (m)
         |CONSTRUCT
         |  CREATE (n)
         |  CREATE (m)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.graph.nodes("n").size shouldBe 4
  }

  it("should construct a graph") {
    val query =
      """|CONSTRUCT
         |  CREATE (:A)-[:KNOWS]->(:B)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords should be(None)
    result.graph.schema.labels should equal(Set("A", "B"))
    result.graph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.graph.nodes("n").size should equal(2)
    result.graph.relationships("r").size should equal(1)
  }

  it("should CONSTRUCT a graph with multiple connected CREATE clauses") {
    val query =
      """|CONSTRUCT
         |  CREATE (a:A)-[:KNOWS]->(b:B)
         |  CREATE (b)-[:KNOWS]->(c:C)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords should be(None)
    result.graph.schema.labels should equal(Set("A", "B", "C"))
    result.graph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.graph.nodes("n").size should equal(3)
    result.graph.relationships("r").size should equal(2)
  }

  it("should CONSTRUCT a graph with multiple unconnected CREATE clauses") {
    val query =
      """|CONSTRUCT
         |  CREATE (a:A)-[:KNOWS]->(b:B)
         |  CREATE (c:C)-[:KNOWS]->(d:D)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A", "B", "C", "D"))
    result.graph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.graph.nodes("n").size should equal(4)
    result.graph.relationships("r").size should equal(2)
  }

  it("should CONSTRUCT a graph with multiple unconnected anonymous CREATE clauses") {
    val query =
      """|CONSTRUCT
         |  CREATE (:A)
         |  CREATE (:B)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None

    result.graph.schema.labels should equal(Set("A", "B"))
    result.graph.schema.relationshipTypes should equal(Set.empty)
    result.graph.nodes("n").size should equal(2)
    result.graph.relationships("r").size should equal(0)
  }

  it("should construct a node property from a matched node") {
    val query =
      """|MATCH (m)
         |CONSTRUCT
         |  CREATE (a :A { name: m.name})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A"))
    result.graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
    result.graph.cypher("MATCH (a:A) RETURN a.name").records.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }

  it("should construct a node property from a literal") {
    val query =
      """|CONSTRUCT
         |  CREATE ({name: 'Donald'})
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys()("name" -> CTString))
    result.graph.cypher("MATCH (a) RETURN a.name").records.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should construct a node label and a node property from a literal") {
    val query =
      """|CONSTRUCT
         |  CREATE (a :A {name: 'Donald'})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A"))
    result.graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
    result.graph.cypher("MATCH (a:A) RETURN a.name").records.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should construct multiple properties") {
    val query =
      """|CONSTRUCT
         |  CREATE (a:A:B {name:'Donald', age:100})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A", "B"))
    result.graph.schema should equal(
      PropertyGraphSchema.empty
        .withNodePropertyKeys(Set("A", "B"), PropertyKeys("name" -> CTString, "age" -> CTInteger))
    )
    result.graph.cypher("MATCH (a:A:B) RETURN a.name").records.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should pick up labels of the outer match") {
    val query =
      """|MATCH (m:Person)
         |CONSTRUCT
         |  CLONE m AS m
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("Person"))
    result.graph.schema should equal(
      PropertyGraphSchema.empty
        .withNodePropertyKeys(Set("Person"), PropertyKeys("name" -> CTString))
    )
    result.graph.cypher("MATCH (a:Person) RETURN a.name").records.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }

  it("should construct a relationship") {
    val query =
      """|CONSTRUCT
         |  CREATE ()-[r:FOO {val : 42}]->()
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.relationshipTypes should equal(Set("FOO"))
    result.graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys()()
      .withRelationshipPropertyKeys("FOO", PropertyKeys("val" -> CTInteger)))
    result.graph.cypher("MATCH ()-[r]->() RETURN r.val").records.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 42)
    ))
  }

  it("should copy a relationship") {
    val query =
      """|CONSTRUCT
         |  CREATE ()-[r:FOO {val : 42}]->()
         |MATCH ()-[s]->()
         |CONSTRUCT
         |  CREATE ()-[t COPY OF s {name : 'Donald'}]->()
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.cypher("MATCH ()-[r]->() RETURN r.val, r.name, type(r) as type").records.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 42, "r.name" -> "Donald", "type" -> "FOO")
    ))
  }

  it("should copy a mean relationship") {
    val graph = initGraph(
      """
        |CREATE ()-[:FOO {val: 1, val2: 2}]->()
        |CREATE ()-[:BAR {val: 1, val2: 3}]->()
      """.stripMargin)

    val query =
      """MATCH ()-[s]->()
        |CONSTRUCT
        |  CREATE ()-[t COPY OF s :BAZ {val2 : 'Donald'}]->()
        |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords shouldBe None

    result.graph.cypher("MATCH ()-[r]->() RETURN r.val, r.val2, type(r) as type").records.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 1, "r.val2" -> "Donald", "type" -> "BAZ"),
      CypherMap("r.val" -> 1, "r.val2" -> "Donald", "type" -> "BAZ")
    ))
  }

  it("should copy a node") {
    val query =
      """|CONSTRUCT
         |  CREATE (:Foo {foo: 'bar'})
         |MATCH (a)
         |CONSTRUCT
         |  CREATE (COPY OF a)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("Foo"))
    result.graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("Foo")("foo" -> CTString)
    )

    result.graph.cypher("MATCH (a) RETURN a.foo, labels(a) as labels").records.iterator.toBag should equal(Bag(
      CypherMap("a.foo" -> "bar", "labels" -> Seq("Foo"))
    ))
  }

  it("should copy a node with labels") {
    val graph = initGraph(
      """
        |CREATE (:A {val: 1})
        |CREATE (:B {val: 2})
        |CREATE (:A:C {val: 3})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  CREATE (COPY OF a)
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords shouldBe None
    result.graph.cypher("MATCH (a) RETURN a.val, labels(a) as labels").records.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> 1, "labels" -> Seq("A")),
      CypherMap("a.val" -> 2, "labels" -> Seq("B")),
      CypherMap("a.val" -> 3, "labels" -> Seq("A", "C"))
    ))
  }

  it("can override in SET") {
    val graph = initGraph(
      """
        |CREATE ({val: 1})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  CREATE (COPY OF a {val: 2})
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords shouldBe None
    result.graph.cypher("MATCH (a) RETURN a.val").records.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> 2)
    ))
  }

  it("can override heterogeneous types in SET") {
    val graph = initGraph(
      """
        |CREATE ({val: 1})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  CREATE (COPY OF a {val: 'foo'})
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords shouldBe None
    result.graph.cypher("MATCH (a) RETURN a.val").records.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> "foo")
    ))
  }

  it("supports CLONE in CONSTRUCT") {
    val res = testGraph1.unionAll(testGraph2).cypher(
      """
        |MATCH (n),(m)
        |WHERE n.name = 'Mats' AND m.name = 'Phil'
        |CONSTRUCT
        | CLONE n AS n, m AS m
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 1
  }

  it("implicitly CLONEs in CONSTRUCT") {
    val res = testGraph1.unionAll(testGraph2).cypher(
      """
        |MATCH (n),(m)
        |WHERE n.name = 'Mats' AND m.name = 'Phil'
        |CONSTRUCT
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 1
  }

  it("constructs multiple relationships") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n),(m)
        |WHERE n <> m
        |CONSTRUCT
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 2
  }

  it("implicitly clones when constructing multiple relationships") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |WITH DISTINCT n, m
        |CONSTRUCT
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 2
  }

  it("constructs multiple relationships 2") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |CONSTRUCT
        | CLONE n AS n, m AS m
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 3
  }

  it("implicitly clones when constructing multiple relationships 2") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |CONSTRUCT
        | CREATE (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.graph.nodes("n").collect.length shouldBe 2
    res.graph.relationships("r").collect.length shouldBe 3
  }

  it("CONSTRUCTS ON a single graph") {
    morpheus.catalog.store("one", testGraph1)
    val query =
      """
        |CONSTRUCT ON one
        |RETURN GRAPH""".stripMargin

    val result = testGraph2.cypher(query).graph

    result.schema.asMorpheus should equal(testGraph1.schema)
    result.nodes("n").toMaps should equal(testGraph1.nodes("n").toMaps)
    result.relationships("r").toMaps should equal(testGraph1.relationships("r").toMaps)
  }

  it("CONSTRUCT ON a single graph without GraphUnionAll") {
    morpheus.catalog.store("one", testGraph1)
    val query =
      """
        |CONSTRUCT ON one
        |MATCH (n) RETURN n""".stripMargin

    val result = testGraph2.cypher(query)

    result.asMorpheus.maybeRelational match {
      case Some(relPlan) =>
        val switchOp = relPlan.collectFirst { case op: SwitchContext[_] => op }.get
        val containsUnionGraph = switchOp.context.queryLocalCatalog.head._2 match {
          case g: UnionGraph[_] => g.graphs.collectFirst { case op: UnionGraph[_] => op }.isDefined
          case _ => false
        }
        withClue("CONSTRUCT plans union on a single input graph") {
          containsUnionGraph shouldBe false
        }

      case None =>
    }
  }

  it("CONSTRUCTS ON two graphs") {
    morpheus.catalog.store("one", testGraph1)
    morpheus.catalog.store("two", testGraph2)
    val query =
      """
        |CONSTRUCT ON one, two
        |RETURN GRAPH""".stripMargin

    val result = testGraph2.cypher(query).graph

    result.schema should equal(testGraph1.schema ++ testGraph2.schema)
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    result.relationships("r").toMaps should equal(testGraph1.unionAll(testGraph2).relationships("r").toMaps)
  }

  it("CONSTRUCTS ON two graphs and adds a relationship") {
    morpheus.catalog.store("one", testGraph1)
    morpheus.catalog.store("two", testGraph2)
    val query =
      """|FROM GRAPH one
         |MATCH (m: Person)
         |FROM GRAPH two
         |MATCH (p: Person)
         |CONSTRUCT ON one, two
         |  CREATE (m)-[:KNOWS]->(p)
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query).graph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("KNOWS")())
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    val resultRelationship = result.relationships("r").toMaps.head._1("r").asInstanceOf[MorpheusRelationship]
    resultRelationship.startId should equal(0L.withPrefix(0).toList)
    resultRelationship.endId should equal(0L.withPrefix(1).toList)
    resultRelationship.id should equal(0L.withPrefix(-1).toList)
    resultRelationship.relType should equal("KNOWS")
  }

  it("implictly clones when CONSTRUCTing ON two graphs and adding a relationship") {
    morpheus.catalog.store("one", testGraph1)
    morpheus.catalog.store("two", testGraph2)
    val query =
      """|FROM GRAPH one
         |MATCH (m: Person)
         |FROM GRAPH two
         |MATCH (p: Person)
         |CONSTRUCT ON one, two
         |  CREATE (m)-[:KNOWS]->(p)
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query).graph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("KNOWS")())
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    val resultRelationship = result.relationships("r").toMaps.head._1("r").asInstanceOf[MorpheusRelationship]
    resultRelationship.startId should equal(0L.withPrefix(0).toList)
    resultRelationship.endId should equal(0L.withPrefix(1).toList)
    resultRelationship.id should equal(0L.withPrefix(-1).toList)
    resultRelationship.relType should equal("KNOWS")
  }

  it("constructs a created node") {
    val query =
      """
        |CONSTRUCT
        |  CREATE (a)
        |RETURN GRAPH
      """.stripMargin

    val graph = morpheus.cypher(query).graph

    graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys(Set.empty[String]))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0, Set.empty[String]))
    ))
  }

  it("construct match construct") {
    morpheus.catalog.store("g1", testGraphRels)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a)
        |CONSTRUCT // generated qgn
        |  CLONE a AS a
        |MATCH (b)
        |CONSTRUCT
        |  ON g1
        |  CLONE b AS b
        |RETURN GRAPH
      """.stripMargin

    val graph = morpheus.cypher(query).graph

    graph.schema.asMorpheus should equal(testGraphRels.schema)
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0.withPrefix(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1.withPrefix(1), Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> MorpheusNode(0L.withPrefix(0), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1L.withPrefix(0), Set("Person"), CypherMap("name" -> "Max")))
    ))
  }

  it("does not clone twice when a variable is both constructed on and matched") {
    morpheus.catalog.store("g1", testGraph1)
    morpheus.catalog.store("g2", testGraph2)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a:Person)
        |FROM GRAPH g2
        |MATCH (b:Person)
        |CONSTRUCT
        |  ON g2
        |  CLONE a AS a, b AS b
        |RETURN GRAPH
      """.stripMargin

    val graph = morpheus.cypher(query).graph

    graph.schema.asMorpheus should equal(testGraph1.schema)
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L.withPrefix(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(0L.withPrefix(0), Set("Person"), CypherMap("name" -> "Phil")))
    ))
  }

  it("allows CONSTRUCT ON with relationships") {
    morpheus.catalog.store("testGraphRels1", testGraphRels)
    morpheus.catalog.store("testGraphRels2", testGraphRels)
    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |CONSTRUCT ON testGraphRels2
         |  CLONE p1 AS p1, r1 AS r1, p2 AS p2
         |  CREATE (p1)-[r1]->( p2)
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query).graph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("HAS_SIMILAR_NAME")())

    result.nodes("n").toMaps should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L.withPrefix(0), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1L.withPrefix(0), Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> MorpheusNode(0L.withPrefix(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1L.withPrefix(1), Set("Person"), CypherMap("name" -> "Max")))
    ))

    result.relationships("r").toMaps should equal(Bag(
      CypherMap("r" -> MorpheusRelationship(2L.withPrefix(0), 1L.withPrefix(0), 0L.withPrefix(0), "HAS_SIMILAR_NAME")),
      CypherMap("r" -> MorpheusRelationship(2L.withPrefix(1), 1L.withPrefix(1), 0L.withPrefix(1), "HAS_SIMILAR_NAME"))
    ))

  }

  it("allows cloning from different graphs with nodes and relationships") {
    def testGraphRels = initGraph(
      """|CREATE (mats:Person {name: 'Mats'})
         |CREATE (max:Person {name: 'Max'})
         |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
      """.stripMargin)

    morpheus.catalog.store("testGraphRels1", testGraphRels)
    morpheus.catalog.store("testGraphRels2", testGraphRels)

    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |FROM GRAPH testGraphRels2
         |MATCH (p3 :Person)-[r2]->(p4 :Person)
         |CONSTRUCT
         |  CLONE p1 AS p1, p2 AS p2, p3 AS p3, p4 AS p4, r1 AS r1, r2 AS r2
         |  CREATE (p1)-[r1]->(p2)
         |  CREATE (p3)-[r2]->(p4)
         |RETURN GRAPH""".stripMargin

    val result = morpheus.cypher(query).graph
    result.schema.asMorpheus shouldEqual testGraphRels.schema

    result.nodes("n").toMaps should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L.withPrefix(0), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1L.withPrefix(0), Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> MorpheusNode(0L.withPrefix(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> MorpheusNode(1L.withPrefix(1), Set("Person"), CypherMap("name" -> "Max")))
    ))

    result.relationships("r").toMaps should equal(Bag(
      CypherMap("r" -> MorpheusRelationship(2L.withPrefix(0), 1L.withPrefix(0), 0L.withPrefix(0), "HAS_SIMILAR_NAME")),
      CypherMap("r" -> MorpheusRelationship(2L.withPrefix(1), 1L.withPrefix(1), 0L.withPrefix(1), "HAS_SIMILAR_NAME"))
    ))
  }

  it("allows consecutive construction") {
    val query =
      """|CONSTRUCT
         |  CREATE (a:A)-[r:FOO]->(b:B)
         |MATCH (a)-->(b)
         |CONSTRUCT
         |  CLONE a AS a, b AS b
         |  CREATE (a)-[:KNOWS]->(b)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.graph.schema.labels should equal(Set("A", "B"))
    result.graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withRelationshipPropertyKeys("KNOWS")()
    )
    result.graph.cypher("MATCH ()-[r]->() RETURN type(r)").records.iterator.toBag should equal(Bag(
      CypherMap("type(r)" -> "KNOWS")
    ))
  }

  it("implictly clones when doing consecutive construction") {
    val query =
      """|CONSTRUCT
         |  CREATE (a:A)-[r:FOO]->(b:B)
         |MATCH (a)-->(b)
         |CONSTRUCT
         |  CREATE (a)-[:KNOWS]->(b)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.graph.schema.labels should equal(Set("A", "B"))
    result.graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withRelationshipPropertyKeys("KNOWS")())
    result.graph.cypher("MATCH ()-[r]->() RETURN type(r)").records.iterator.toBag should equal(Bag(
      CypherMap("type(r)" -> "KNOWS")
    ))
  }

  it("can construct a copy of a node with matched label") {
    morpheus.cypher("CATALOG CREATE GRAPH foo { CONSTRUCT CREATE (:A) RETURN GRAPH }")

    val graph = morpheus.cypher("FROM GRAPH foo RETURN GRAPH").graph

    graph.cypher(
      """MATCH (a:A)
        |CONSTRUCT
        |  CREATE (COPY OF a)
        |MATCH (n)
        |RETURN labels(n)
      """.stripMargin).records.iterator.toBag should equal(Bag(
      CypherMap("labels(n)" -> Seq("A"))
    ))
  }

  it("can construct with an input table expanded by unwind") {
    morpheus.cypher("CATALOG CREATE GRAPH foo { CONSTRUCT CREATE (:A) RETURN GRAPH }")

    val data = morpheus.cypher("FROM GRAPH foo RETURN GRAPH").graph.cypher(
      """MATCH (a:A)
        |UNWIND [1, 2, 3] AS i
        |CONSTRUCT
        |  CREATE (f COPY OF a)-[:FOO]->(g COPY OF a)
        |  CREATE (:B {name: 'foo'})
        |MATCH (n)
        |RETURN n.name
      """.stripMargin).records

    val nullRow = CypherMap("n.name" -> null)
    val fooRow = CypherMap("n.name" -> "foo")
    data.iterator.toBag should equal(Bag(
      nullRow,
      nullRow,
      nullRow,
      nullRow,
      nullRow,
      nullRow,
      fooRow,
      fooRow,
      fooRow
    ))
  }

  it("should set a node property from a matched node") {
    val query =
      """|MATCH (m)
         |CONSTRUCT
         |  CREATE (a :A)
         |  SET a.name = m.name
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A"))
    result.graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
    result.graph.cypher("MATCH (a:A) RETURN a.name").records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }

  it("should set a node property from a literal") {
    val query =
      """|CONSTRUCT
         |  CREATE (a :A)
         |  SET a.name = 'Donald'
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords shouldBe None
    result.graph.schema.labels should equal(Set("A"))
    result.graph.schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
    result.graph.cypher("MATCH (a:A) RETURN a.name").records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should set a node label") {
    val query =
      """|CONSTRUCT
         |  CREATE (a)
         |  SET a: FOO
         |MATCH (n)
         |RETURN n""".stripMargin

    val result = testGraph1.cypher(query)

    result.records.toMaps shouldBe Bag(
      CypherMap("n" -> MorpheusNode(0, Set("FOO")))
    )
  }

  it("fails early on incompatible graphs in CONSTRUCT") {
    val g1 = morpheus.cypher("CONSTRUCT CREATE (:A {p: 1}) RETURN GRAPH").graph
    val g2 = morpheus.cypher("CONSTRUCT CREATE (:A {p: 'foo'}) RETURN GRAPH").graph
    morpheus.catalog.store("g1", g1)
    morpheus.catalog.store("g2", g2)

    an[SchemaException] should be thrownBy morpheus.cypher("CONSTRUCT ON g1, g2 RETURN GRAPH")
  }

  it("should implicit clone a relationship #1") {
    val query =
      """|MATCH (a)-[r:HAS_SIMILAR_NAME]->(b)
         |CONSTRUCT
         |  CREATE (a)-[r]->(b)
         |RETURN GRAPH""".stripMargin
    val result = testGraphRels.cypher(query)
    result.graph.cypher("MATCH ()-[r]->() RETURN type(r) as type").records.iterator.toBag should equal(Bag(
      CypherMap("type" -> "HAS_SIMILAR_NAME")
    ))
  }

  it("should implicit clone a relationship #2") {
    val query =
      """|MATCH (a)-[r:HAS_SIMILAR_NAME]->(b)
         |WITH a,r,b
         |CONSTRUCT
         |  CREATE (a)-[r]->(b)
         |RETURN GRAPH""".stripMargin
    val result = testGraphRels.cypher(query)
    result.graph.cypher("MATCH ()-[r]->() RETURN type(r) as type").records.iterator.toBag should equal(Bag(
      CypherMap("type" -> "HAS_SIMILAR_NAME")
    ))
  }

  it("should implicit clone a relationship #3") {
    val query =
      """|MATCH (a)-[r:HAS_SIMILAR_NAME]->(b)
         |WITH a,r,b
         |CONSTRUCT
         |  CLONE a AS x, b AS y
         |  CREATE (x)-[r]->(y)
         |RETURN GRAPH""".stripMargin
    val result = testGraphRels.cypher(query)
    result.graph.cypher("MATCH ()-[r]->() RETURN type(r) as type").records.iterator.toBag should equal(Bag(
      CypherMap("type" -> "HAS_SIMILAR_NAME")
    ))
  }
}
