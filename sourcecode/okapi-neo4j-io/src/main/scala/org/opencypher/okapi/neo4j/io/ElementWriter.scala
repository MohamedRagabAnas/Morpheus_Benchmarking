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
package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.exceptions.ClientException
import org.opencypher.okapi.impl.exception.IllegalStateException
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

import scala.util.{Failure, Success, Try}

object ElementWriter extends Logging {

  private val ROW_IDENTIFIER = "row"

  // TODO: Share more code with `createNodes`
  def mergeNodes[T](
    nodes: Iterator[T],
    rowMapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String],
    nodeKeys: Set[String]
  )(rowToListValue: T => Value): Unit = {
    val labelString = labels.cypherLabelPredicate

    val nodeKeyProperties = nodeKeys.map { nodeKey =>
      val keyIndex = rowMapping.indexOf(nodeKey)
      val parameterMapLookup = s"$ROW_IDENTIFIER[$keyIndex]"
      s"`$nodeKey`: $parameterMapLookup"
    }.mkString(", ")

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot { case (propertyKey, _) => propertyKey == null || nodeKeys.contains(propertyKey) }
      .map { case (key, i) => s"SET n.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |MERGE (n$labelString { $nodeKeyProperties })
         |$setStatements
         """.stripMargin

    writeElements(nodes, rowMapping, createQ, config, config.mergeNodeBatchSize)(rowToListValue)
  }

  // TODO: Share more code with `createRelationships`
  def mergeRelationships[T](
    relationships: Iterator[T],
    maybeMetaLabel: Option[String],
    startNodeIndex: Int,
    endNodeIndex: Int,
    rowMapping: Array[String],
    config: Neo4jConfig,
    relType: String,
    relKeys: Set[String]
  )(rowToListValue: T => Value): Unit = {

    val relKeyProperties = relKeys.map { relKey =>
      val keyIndex = rowMapping.indexOf(relKey)
      val parameterMapLookup = s"$ROW_IDENTIFIER[$keyIndex]"
      s"`$relKey`: $parameterMapLookup"
    }.mkString(", ")

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot { case (propertyKey, _) => propertyKey == null || relKeys.contains(propertyKey) }
      .map { case (key, i) => s"SET rel.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val labelString = maybeMetaLabel.toSet[String].cypherLabelPredicate

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |MATCH (from$labelString {$metaPropertyKey : $ROW_IDENTIFIER[$startNodeIndex]})
         |MATCH (to$labelString {$metaPropertyKey : $ROW_IDENTIFIER[$endNodeIndex]})
         |MERGE (from)-[rel:$relType { $relKeyProperties }]->(to)
         |$setStatements
         """.stripMargin

    writeElements(relationships, rowMapping, createQ, config, config.mergeRelationshipBatchSize)(rowToListValue)
  }

  def createNodes[T](
    nodes: Iterator[T],
    rowMapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String]
  )(rowToListValue: T => Value): Unit = {
    val labelString = labels.cypherLabelPredicate

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map { case (key, i) => s"SET n.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |CREATE (n$labelString)
         |$setStatements
         """.stripMargin

    writeElements(nodes, rowMapping, createQ, config, config.createNodeBatchSize)(rowToListValue)
  }

  def createRelationships[T](
    relationships: Iterator[T],
    startNodeIndex: Int,
    endNodeIndex: Int,
    rowMapping: Array[String],
    config: Neo4jConfig,
    relType: String,
    nodeLabel: Option[String]
  )(rowToListValue: T => Value): Unit = {
    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map { case (key, i) => s"SET rel.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val nodeLabelString = nodeLabel.toSet[String].cypherLabelPredicate

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |MATCH (from$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$startNodeIndex]})
         |MATCH (to$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$endNodeIndex]})
         |CREATE (from)-[rel:$relType]->(to)
         |$setStatements
         """.stripMargin

    writeElements(relationships, rowMapping, createQ, config, config.createRelationshipBatchSize)(rowToListValue)
  }

  private def writeElements[T](
    elements: Iterator[T],
    rowMapping: Array[String],
    query: String,
    config: Neo4jConfig,
    batchSize: Int
  )(rowToListValue: T => Value): Unit = {
    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)
    val reuseStatement = new Statement(query, reuseParameters)

    config.withSession { session =>
      val batches = elements.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val rowParameters = new Array[Value](batch.size)

        batch.zipWithIndex.foreach { case (row, i) => rowParameters(i) = rowToListValue(row) }

        reuseMap.put("batch", Values.value(rowParameters: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)
        Try {
          session.writeTransaction {
            new TransactionWork[Unit] {
              override def execute(transaction: Transaction): Unit = {
                logger.debug(s"Executing query: $reuseStatement")
                transaction.run(reuseStatement).consume()
              }
            }
          }
        } match {
          case Success(_) => ()

          case Failure(exception: ClientException) if exception.getMessage.contains("already exists") =>
            val originalMessage = exception.getMessage

            val elementType = if (originalMessage.contains("Node(")) "nodes" else "relationships"

            val duplicateIdRegex = """.+('[0-9a-fA-F]+')$""".r
            val duplicateId = originalMessage match {
              case duplicateIdRegex(idString) => idString
              case _ => "UNKNOWN"
            }

            val message = s"Could not write the graph to Neo4j. The graph you are attempting to write contains at least two $elementType with Morpheus id $duplicateId"
            throw IllegalStateException(message, Some(exception))

          case Failure(e) => throw e
        }
      }
    }
  }
}
