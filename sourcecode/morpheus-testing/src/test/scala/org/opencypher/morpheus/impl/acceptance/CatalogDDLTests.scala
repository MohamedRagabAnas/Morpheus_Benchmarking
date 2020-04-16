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

import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.graph.{GraphName, QualifiedGraphName}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, ViewAlreadyExistsException}
import org.opencypher.okapi.testing.Bag
import org.opencypher.v9_0.util.SyntaxException
import org.scalatest.BeforeAndAfterAll

class CatalogDDLTests extends MorpheusTestSuite with ScanGraphInit with BeforeAndAfterAll {

  override def afterEach(): Unit = {
    super.afterEach()
    morpheus.catalog.graphNames.filterNot(_ == morpheus.emptyGraphQgn).foreach(morpheus.catalog.dropGraph)
    morpheus.catalog.viewNames.foreach(morpheus.catalog.dropView)
  }

  describe("CATALOG CREATE GRAPH") {
    it("supports CATALOG CREATE GRAPH on the session") {
      val inputGraph = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)

      morpheus.catalog.store("foo", inputGraph)

      val result = morpheus.cypher(
        """
          |CATALOG CREATE GRAPH bar {
          | FROM GRAPH foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val sessionSource = morpheus.catalog.source(morpheus.catalog.sessionNamespace)
      sessionSource.hasGraph(GraphName("bar")) shouldBe true
      sessionSource.graph(GraphName("bar")) shouldEqual inputGraph
      result.getGraph shouldBe None
      result.getRecords shouldBe None
    }
  }

  describe("CATALOG CREATE VIEW") {

    it("supports storing a VIEW") {
      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar {
          | FROM GRAPH foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      morpheus.catalog.catalogNames should contain(bar)
      morpheus.catalog.viewNames should contain(bar)
    }

    it("throws an error when a view QGN collides with an existing view QGN") {
      morpheus.cypher(
        """
          |CATALOG CREATE VIEW foo {
          | FROM GRAPH whatever
          | RETURN GRAPH
          |}
        """.stripMargin)

      a[ViewAlreadyExistsException] should be thrownBy {
        morpheus.cypher(
          """
            |CATALOG CREATE VIEW foo {
            | FROM GRAPH whatever
            | RETURN GRAPH
            |}
          """.stripMargin)
      }

    }

    it("can still resolve a graph when a view with the same name exists") {

      morpheus.cypher(
        """
          |CATALOG CREATE GRAPH foo {
          | CONSTRUCT
          |   CREATE ()
          | RETURN GRAPH
          |}
        """.stripMargin)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW foo {
          | FROM GRAPH whatever
          | RETURN GRAPH
          |}
        """.stripMargin)

      morpheus.cypher("FROM GRAPH foo MATCH (n) RETURN n").records.size shouldBe 1

    }

    it("can still resolve a view when a graph with the same name exists") {

      morpheus.cypher(
        """
          |CATALOG CREATE GRAPH bar {
          | CONSTRUCT
          |   CREATE ()
          |   CREATE ()
          | RETURN GRAPH
          |}
        """.stripMargin)

      morpheus.cypher(
        """
          |CATALOG CREATE GRAPH foo {
          | CONSTRUCT
          |   CREATE ()
          | RETURN GRAPH
          |}
        """.stripMargin)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW foo {
          | FROM bar
          | RETURN GRAPH
          |}
        """.stripMargin)

      morpheus.cypher("FROM foo() MATCH (n) RETURN n").records.size shouldBe 2

    }

    it("throws an illegal argument exception, when no view with the given name is stored") {
      an[IllegalArgumentException] should be thrownBy {
        morpheus.cypher(
          """
            |FROM GRAPH someView()
            |MATCH (n)
            |RETURN n
          """.stripMargin)
      }
    }

    it("supports simple nested CATALOG CREATE VIEW in a query") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A {val: 0})
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW inc($g1) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | CONSTRUCT
          |   CREATE (:A { val: a.val + 1 })
          | RETURN GRAPH
          |}
        """.stripMargin)

      val inc = QualifiedGraphName("inc")
      morpheus.catalog.catalogNames should contain(inc)
      morpheus.catalog.viewNames should contain(inc)

      val result = morpheus.cypher(
        """
          |FROM GRAPH inc(inc(inc(inc(a))))
          |MATCH (n)
          |RETURN n.val as val
        """.stripMargin)

      result.records.toMaps should equal(Bag(CypherMap(
        "val" -> 4
      )))
    }

    it("disallows graph parameters as view invocation parameters") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A {val: 0})
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW inc($g1) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | CONSTRUCT
          |   CREATE (:A { val: a.val + 1 })
          | RETURN GRAPH
          |}
        """.stripMargin)

      val inc = QualifiedGraphName("inc")
      morpheus.catalog.catalogNames should contain(inc)
      morpheus.catalog.viewNames should contain(inc)

      a[SyntaxException] should be thrownBy {
        morpheus.cypher(
          """
            |FROM GRAPH inc($param)
            |MATCH (n)
            |RETURN n.val as val
          """.stripMargin, CypherMap("param" -> "a"))
      }
    }

    it("supports CATALOG CREATE VIEW with two parameters") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE (:B)
          |CREATE (:B)
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)
      morpheus.catalog.store("b", inputGraphB)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | FROM GRAPH $g2
          | MATCH (b: B)
          | CONSTRUCT
          |   CREATE (a)
          |   CREATE (b)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      morpheus.catalog.catalogNames should contain(bar)
      morpheus.catalog.viewNames should contain(bar)

      val resultGraph = morpheus.cypher(
        """
          |FROM GRAPH bar(a, b)
          |RETURN GRAPH
        """.stripMargin).graph

      resultGraph.nodes("n").size shouldBe 3
      resultGraph.nodes("a", CTNode("A")).size shouldBe 1
      resultGraph.nodes("b", CTNode("B")).size shouldBe 2
    }

    it("supports nested CREATE VIEW with two parameters") {
      val inputGraphA1 = initGraph(
        """
          |CREATE ({val: 1})
        """.stripMargin)
      val inputGraphA2 = initGraph(
        """
          |CREATE ({val: 1000})
        """.stripMargin)

      morpheus.catalog.store("a1", inputGraphA1)
      morpheus.catalog.store("a2", inputGraphA2)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (n)
          | FROM GRAPH $g2
          | MATCH (m)
          | CONSTRUCT
          |   CREATE ({val: n.val + m.val})
          | RETURN GRAPH
          |}
        """.stripMargin)

      val resultGraph = morpheus.cypher(
        """
          |FROM GRAPH bar(bar(a2, a1), bar(a1, a2))
          |RETURN GRAPH
        """.stripMargin).graph

      resultGraph.nodes("n").size shouldBe 1
      resultGraph.cypher("MATCH (n) RETURN n.val").records.toMaps should equal(Bag(
        CypherMap("n.val" -> 2002)
      ))
    }

    it("supports nested CREATE VIEW with two parameters and multiple constructed nodes") {
      val inputGraphA = initGraph(
        """
          |CREATE ({name: 'A1'})
          |CREATE ({name: 'A2'})
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE ({name: 'B1'})
          |CREATE ({name: 'B2'})
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)
      morpheus.catalog.store("b", inputGraphB)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (n)
          | FROM GRAPH $g2
          | MATCH (m)
          | CONSTRUCT
          |   CREATE (n)
          |   CREATE (m)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val resultGraph = morpheus.cypher(
        """
          |FROM GRAPH bar(bar(b, a), bar(a, b))
          |RETURN GRAPH
        """.stripMargin).graph

      resultGraph.nodes("n").size shouldBe 8
    }

    it("supports nested CREATE VIEW with two parameters with cloning") {
      val inputGraphA = initGraph(
        """
          |CREATE ({name: 'A1'})
          |CREATE ({name: 'A2'})
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE ({name: 'B1'})
          |CREATE ({name: 'B2'})
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)
      morpheus.catalog.store("b", inputGraphB)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (n)
          | FROM GRAPH $g2
          | MATCH (m)
          | CONSTRUCT
          |   CLONE n AS n
          |   CLONE m AS m
          | RETURN GRAPH
          |}
        """.stripMargin)

      val resultGraph = morpheus.cypher(
        """
          |FROM GRAPH bar(bar(b, a), bar(a, b))
          |RETURN GRAPH
        """.stripMargin).graph

      resultGraph.nodes("n").size shouldBe 8
    }

    it("supports nested CREATE VIEW with two parameters and empty constructed nodes") {
      val inputGraphA = initGraph(
        """
          |CREATE ({name: 'A1'})
          |CREATE ({name: 'A2'})
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE ({name: 'B1'})
          |CREATE ({name: 'B2'})
        """.stripMargin)

      morpheus.catalog.store("a", inputGraphA)
      morpheus.catalog.store("b", inputGraphB)

      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (n)
          | FROM GRAPH $g2
          | MATCH (m)
          | CONSTRUCT
          |   CLONE n AS n
          |   CREATE (COPY OF m)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val resultGraph = morpheus.cypher(
        """
          |FROM GRAPH bar(bar(b, a), bar(a, b))
          |RETURN GRAPH
        """.stripMargin).graph

      resultGraph.nodes("n").size shouldBe 42
    }

  }

  describe("DROP GRAPH/VIEW") {

    it("can drop a view") {
      morpheus.cypher(
        """
          |CATALOG CREATE VIEW bar {
          | FROM GRAPH foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      morpheus.catalog.catalogNames should contain(bar)
      morpheus.catalog.viewNames should contain(bar)

      morpheus.cypher(
        """
          |CATALOG DROP VIEW bar
        """.stripMargin
      )

      morpheus.catalog.catalogNames should not contain bar
      morpheus.catalog.viewNames should not contain bar
    }

    it("dropping a view is idempotent") {
      morpheus.catalog.dropView("foo")
      morpheus.cypher("CATALOG DROP VIEW foo")
    }

    it("can drop a session graph") {

      morpheus.catalog.store("foo", initGraph("CREATE (:A)"))

      val result = morpheus.cypher(
        """
          |CATALOG DROP GRAPH session.foo
        """.stripMargin
      )

      morpheus.catalog.source(morpheus.catalog.sessionNamespace).hasGraph(GraphName("foo")) shouldBe false
      result.getGraph shouldBe None
      result.getRecords shouldBe None
    }
  }
}
