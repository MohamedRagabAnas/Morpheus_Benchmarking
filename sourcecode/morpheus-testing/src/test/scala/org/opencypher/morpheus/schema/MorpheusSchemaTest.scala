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
package org.opencypher.morpheus.schema

import org.opencypher.morpheus.schema.MorpheusSchema._
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.morpheus.testing.fixture.GraphConstructionFixture
import org.opencypher.okapi.api.schema.{PropertyGraphSchema, PropertyKeys}
import org.opencypher.okapi.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.okapi.impl.exception.SchemaException

class MorpheusSchemaTest extends MorpheusTestSuite with GraphConstructionFixture {

  it("constructs schema correctly for unlabeled nodes") {
    val graph = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

    graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys(Set.empty[String], Map("id" -> CTInteger.nullable, "other" -> CTString.nullable))
      .asMorpheus
    )
  }

  it("constructs schema correctly for labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
      .asMorpheus
    )
  }

  it("constructs schema correctly for multi-labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A:B {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
      .withNodePropertyKeys("A", "B")("id" -> CTInteger)
      .asMorpheus
    )
  }

  it("constructs schema correctly for relationships") {
    val graph = initGraph(
      """
        |CREATE ()-[:FOO {p: 1}]->()
        |CREATE ()-[:BAR {p: 2, q: 'baz'}]->()
        |CREATE ()-[:BAR {p: 3}]->()
      """.stripMargin
    )

    graph.schema should equal(PropertyGraphSchema.empty
      .withNodePropertyKeys(Set.empty[String], PropertyKeys.empty)
      .withRelationshipPropertyKeys("FOO")("p" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p" -> CTInteger, "q" -> CTString.nullable)
      .asMorpheus
    )
  }

  it("fails when combining type conflicting schemas resulting in type ANY") {
    val schema1 = PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTInteger)

    the[SchemaException] thrownBy (schema1 ++ schema2).asMorpheus should have message
      "The property type 'UNION(INTEGER, STRING)' for property 'bar' can not be stored in a Spark column. The unsupported type is specified on label combination [A]."
  }

  it("fails when combining type conflicting schemas resulting in type NUMBER") {
    val schema1 = PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTInteger)
    val schema2 = PropertyGraphSchema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTFloat)

    the[SchemaException] thrownBy (schema1 ++ schema2).asMorpheus should have message
      "The property type 'NUMBER' for property 'baz' can not be stored in a Spark column. The unsupported type is specified on label combination [A]."
  }

  it("successfully verifies the empty schema") {
    noException shouldBe thrownBy(PropertyGraphSchema.empty.asMorpheus)
  }

  it("successfully verifies a valid schema") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    noException shouldBe thrownBy(schema.asMorpheus)
  }

  it("fails when verifying schema with conflict on implied labels") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("name" -> CTBoolean)

    the[SchemaException] thrownBy schema.asMorpheus should have message
      "The property type 'UNION(FLOAT, TRUE, FALSE)' for property 'name' can not be stored in a Spark column. The conflict appears between label combinations [Dog, Pet] and [Pet]."
  }

  it("fails when verifying schema with conflict on combined labels") {
    val schema = PropertyGraphSchema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Employee")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    the[SchemaException] thrownBy schema.asMorpheus should have message
      "The property type 'UNION(STRING, INTEGER)' for property 'name' can not be stored in a Spark column. The conflict appears between label combinations [Person] and [Employee, Person]."
  }
}
