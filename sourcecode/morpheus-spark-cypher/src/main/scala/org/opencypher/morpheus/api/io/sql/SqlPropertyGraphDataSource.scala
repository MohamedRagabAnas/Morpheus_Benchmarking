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
package org.opencypher.morpheus.api.io.sql

import java.net.URI

import org.apache.spark.sql.functions.{monotonically_increasing_id, lit}
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, functions}
import org.opencypher.graphddl._
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.GraphElement.sourceIdKey
import org.opencypher.morpheus.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.morpheus.api.io.sql.GraphDdlConversions._
import org.opencypher.morpheus.api.io.sql.IdGenerationStrategy.{IdGenerationStrategy, _}
import org.opencypher.morpheus.api.io.sql.SqlDataSourceConfig.{File, Hive, Jdbc}
import org.opencypher.morpheus.api.io.{FileFormat, HiveFormat, JdbcFormat, MorpheusElementTable}
import org.opencypher.morpheus.impl.MorpheusFunctions
import org.opencypher.morpheus.impl.convert.SparkConversions._
import org.opencypher.morpheus.impl.expressions.EncodeLong._
import org.opencypher.morpheus.impl.io.MorpheusPropertyGraphDataSource
import org.opencypher.morpheus.impl.table.SparkTable._
import org.opencypher.morpheus.schema.MorpheusSchema
import org.opencypher.morpheus.schema.MorpheusSchema._
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CTVoid}
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

import scala.reflect.io.Path

object SqlPropertyGraphDataSource {

  def apply(
    graphDdl: GraphDdl,
    sqlDataSourceConfigs: Map[String, SqlDataSourceConfig],
    idGenerationStrategy: IdGenerationStrategy = SerializedId
  )(implicit morpheus: MorpheusSession): SqlPropertyGraphDataSource = {

    val unsupportedDataSources = sqlDataSourceConfigs.filter { case (_, config) => config.format == FileFormat.csv }
    if (unsupportedDataSources.nonEmpty) throw IllegalArgumentException(
      expected = "Supported FileFormat for SQL Property Graph Data Source",
      actual = s"${FileFormat.csv} used in the following data source configs: ${unsupportedDataSources.keys.mkString("[", ", ", "]")}")

    new SqlPropertyGraphDataSource(graphDdl, sqlDataSourceConfigs, idGenerationStrategy)
  }
}

case class SqlPropertyGraphDataSource(
  graphDdl: GraphDdl,
  sqlDataSourceConfigs: Map[String, SqlDataSourceConfig],
  idGenerationStrategy: IdGenerationStrategy
)(implicit val morpheus: MorpheusSession) extends MorpheusPropertyGraphDataSource {

  val relSourceIdKey: String = "rel_" + sourceIdKey
  private val className = getClass.getSimpleName
  override def hasGraph(graphName: GraphName): Boolean = graphDdl.graphs.contains(graphName)

  override def graph(graphName: GraphName): PropertyGraph = {
    val ddlGraph = graphDdl.graphs.getOrElse(graphName, throw GraphNotFoundException(s"Graph $graphName not found"))
    val schema = ddlGraph.graphType.asOkapiSchema

    val nodeTables = extractNodeTables(ddlGraph, schema)

    val relationshipTables = extractRelationshipTables(ddlGraph, schema)

    val patternTables = extractNodeRelTables(ddlGraph, schema, idGenerationStrategy)

    morpheus.graphs.create(Some(schema), nodeTables.head, nodeTables.tail ++ relationshipTables ++ patternTables: _*)
  }

  override def schema(name: GraphName): Option[MorpheusSchema] = graphDdl.graphs.get(name).map(_.graphType.asOkapiSchema.asMorpheus)

  override def store(name: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  override def delete(name: GraphName): Unit = unsupported("deleting a graph")

  override def graphNames: Set[GraphName] = graphDdl.graphs.keySet

  def malformed(desc: String, identifier: String): Nothing =
    throw MalformedIdentifier(s"$desc: $identifier")

  private def extractNodeTables(
    ddlGraph: Graph,
    schema: PropertyGraphSchema
  ): Seq[MorpheusElementTable] = {
    ddlGraph.nodeToViewMappings.mapValues(nvm => readTable(nvm.view)).map {
      case (nodeViewKey, df) =>
        val nodeViewMapping = ddlGraph.nodeToViewMappings(nodeViewKey)

        val (propertyMapping, nodeColumns) = extractNode(ddlGraph, schema, nodeViewMapping, df)
        val nodeDf = df.select(nodeColumns: _*)

        val mapping = NodeMappingBuilder.on(sourceIdKey)
          .withImpliedLabels(nodeViewMapping.nodeType.labels.toSeq: _*)
          .withPropertyKeyMappings(propertyMapping.toSeq: _*)
          .build

        MorpheusElementTable.create(mapping, nodeDf)
    }.toSeq
  }

  private def extractRelationshipTables(
    ddlGraph: Graph,
    schema: PropertyGraphSchema
  ): Seq[MorpheusElementTable] = {
    ddlGraph.edgeToViewMappings.map(evm => evm -> readTable(evm.view)).map {
      case (evm, df) =>
        val (propertyMapping, relColumns) = extractRelationship(ddlGraph, schema, evm, df)
        val relDf = df.select(relColumns: _*)

        val relElementType = evm.key.relType.labels.toList match {
          case relType :: Nil => relType
          case other => throw IllegalArgumentException(expected = "Single relationship type", actual = s"${other.mkString(",")}")
        }

        val mapping = RelationshipMappingBuilder
          .on(relSourceIdKey).from(sourceStartNodeKey).to(sourceEndNodeKey)
          .relType(relElementType)
          .withPropertyKeyMappings(propertyMapping.toSeq: _*)
          .build

        MorpheusElementTable.create(mapping, relDf)
    }
  }

  private def extractNodeRelTables(
    ddlGraph: Graph,
    schema: PropertyGraphSchema,
    strategy: IdGenerationStrategy
  ): Seq[MorpheusElementTable] = {
    ddlGraph.edgeToViewMappings
      .filter(evm => evm.view == evm.startNode.nodeViewKey.viewId)
      .map(evm => evm -> readTable(evm.view))
      .map {
        case (evm, df) =>
          val nodeViewKey = evm.startNode.nodeViewKey
          val nodeViewMapping = ddlGraph.nodeToViewMappings(nodeViewKey)

          val (nodePropertyMapping, nodeColumns) = extractNode(ddlGraph, schema, nodeViewMapping, df)
          val (relPropertyMapping, relColumns) = extractRelationship(ddlGraph, schema, evm, df)

          val patternColumns = nodeColumns ++ relColumns
          val patternDf = df.select(patternColumns: _*)

          val pattern = NodeRelPattern(CTNode(nodeViewMapping.nodeType.labels), CTRelationship(evm.relType.labels))
          val patternMapping = ElementMapping(
            pattern,
            Map(
              pattern.nodeElement -> nodePropertyMapping,
              pattern.relElement -> relPropertyMapping
            ),
            Map(
              pattern.nodeElement -> Map(SourceIdKey -> sourceIdKey),
              pattern.relElement -> Map(SourceIdKey -> relSourceIdKey, SourceStartNodeKey -> sourceStartNodeKey, SourceEndNodeKey -> sourceEndNodeKey)
            )
          )

          MorpheusElementTable.create(patternMapping, patternDf)
      }
  }

  private def extractNode(
    ddlGraph: Graph,
    schema: PropertyGraphSchema,
    nodeViewMapping: NodeToViewMapping,
    df: DataFrame
  ): (Map[String, String], Seq[Column]) = {

    val nodeIdColumn = {
      val inputNodeIdColumns = ddlGraph.nodeIdColumnsFor(nodeViewMapping.key) match {
        case Some(columnNames) => columnNames
        case None => df.columns.map(_.decodeSpecialCharacters).toList
      }

      generateIdColumn(df, nodeViewMapping.key, inputNodeIdColumns, sourceIdKey, schema)
    }

    val nodeProperties = generatePropertyColumns(nodeViewMapping, df, ddlGraph, schema)
    val nodePropertyColumns = nodeProperties.map { case (_, _, col) => col }.toSeq

    val nodeColumns = nodeIdColumn +: nodePropertyColumns
    val nodePropertyMapping = nodeProperties.map { case (property, columnName, _) => property -> columnName }

    nodePropertyMapping.toMap -> nodeColumns
  }

  private def extractRelationship(
    ddlGraph: Graph,
    schema: PropertyGraphSchema,
    evm: EdgeToViewMapping,
    df: DataFrame
  ): (Map[String, String], Seq[Column]) = {

    val relIdColumn = generateIdColumn(df, evm.key, df.columns.find(_ == SourceIdKey.name).toList, relSourceIdKey, schema)
    val relSourceIdColumn = generateIdColumn(df, evm.startNode.nodeViewKey, evm.startNode.joinPredicates.map(_.edgeColumn), sourceStartNodeKey, schema)
    val relTargetIdColumn = generateIdColumn(df, evm.endNode.nodeViewKey, evm.endNode.joinPredicates.map(_.edgeColumn), sourceEndNodeKey, schema)
    val relProperties = generatePropertyColumns(evm, df, ddlGraph, schema, Some("relationship"))
    val relPropertyColumns = relProperties.map { case (_, _, col) => col }

    val relColumns = Seq(relIdColumn, relSourceIdColumn, relTargetIdColumn) ++ relPropertyColumns
    val relPropertyMapping = relProperties.map { case (property, columnName, _) => property -> columnName }

    relPropertyMapping.toMap -> relColumns
  }

  private def readTable(viewId: ViewId): DataFrame = {
    val sqlDataSourceConfig = sqlDataSourceConfigs.get(viewId.dataSource) match {
      case None =>
        val knownDataSources = sqlDataSourceConfigs.keys.mkString("'", "';'", "'")
        throw SqlDataSourceConfigException(s"Data source '${viewId.dataSource}' not configured; see data sources configuration. Known data sources: $knownDataSources")
      case Some(config) =>
        config
    }

    val inputTable = sqlDataSourceConfig match {
      case hive@Hive => readSqlTable(viewId, hive)
      case jdbc: Jdbc => readSqlTable(viewId, jdbc)
      case file: File => readFile(viewId, file)
    }

    inputTable.toDF(inputTable.columns.map(_.toLowerCase.encodeSpecialCharacters).toSeq: _*)
  }

  private def readSqlTable(viewId: ViewId, sqlDataSourceConfig: SqlDataSourceConfig) = {
    val spark = morpheus.sparkSession

    implicit class DataFrameReaderOps(read: DataFrameReader) {
      def maybeOption(key: String, value: Option[String]): DataFrameReader =
        value.fold(read)(read.option(key, _))
    }

    sqlDataSourceConfig match {
      case Jdbc(url, driver, options) =>
        spark.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("fetchSize", "100") // default value
          .options(options)
          .option("dbtable", viewId.tableName)
          .load()

      case SqlDataSourceConfig.Hive =>
        spark.table(viewId.tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }
  }
  private def readFile(viewId: ViewId, dataSourceConfig: File): DataFrame = {
    val spark = morpheus.sparkSession

    val viewPath = viewId.parts.lastOption.getOrElse(
      malformed("File names must be defined with the data source", viewId.parts.mkString(".")))

    val filePath = if (new URI(viewPath).isAbsolute) {
      viewPath
    } else {
      dataSourceConfig.basePath match {
        case Some(rootPath) => (Path(rootPath) / Path(viewPath)).toString()
        case None => unsupported("Relative view file names require basePath to be set")
      }
    }

    spark.read
      .format(dataSourceConfig.format.name)
      .options(dataSourceConfig.options)
      .load(filePath.toString)
  }

  private def generatePropertyColumns(
    mapping: ElementToViewMapping,
    df: DataFrame,
    ddlGraph: Graph,
    schema: PropertyGraphSchema,
    maybePrefix: Option[String] = None
  ): Iterable[(String, String, Column)] = {
    val viewKey = mapping.key

    val elementTypes = viewKey match {
      case n: NodeViewKey => n.nodeType.labels
      case r: EdgeViewKey => r.relType.labels
    }

    def getTargetType(elementTypes: Set[String], property: String): DataType = {
      val maybeCT = viewKey match {
        case _: NodeViewKey => schema.nodePropertyKeyType(elementTypes, property)
        case _: EdgeViewKey => schema.relationshipPropertyKeyType(elementTypes, property)
      }

      maybeCT.getOrElse(CTVoid).getSparkType
    }

    val propertyMappings = mapping.propertyMappings

    propertyMappings.map {
      case (property, colName) =>
        val normalizedColName = colName.toLowerCase().encodeSpecialCharacters
        val sourceColumn = df.col(normalizedColName)
        val sourceType = df.schema.apply(normalizedColName).dataType
        val targetType = getTargetType(elementTypes, property)

        val withCorrectType = (sourceType, targetType) match {
          case _ if sourceType == targetType => sourceColumn
          case (IntegerType, LongType) => sourceColumn.cast(targetType)
          case (d1: DecimalType, d2: DecimalType) if d2.getCypherType().superTypeOf(d1.getCypherType()) => sourceColumn.cast(targetType)
          case _ => throw IllegalArgumentException(
            s"Property `$property` to be a subtype of $targetType",
            s"Property $sourceColumn with type $sourceType"
          )
        }

        val targetColumnName = maybePrefix.getOrElse("") + property.toPropertyColumnName

        (property, targetColumnName, withCorrectType.as(targetColumnName))
    }
  }

  private def generateIdColumn(
    dataFrame: DataFrame,
    elementViewKey: ElementViewKey,
    idColumnNames: List[String],
    newIdColumn: String,
    schema: PropertyGraphSchema
  ): Column = {
    val idColumns = if(idColumnNames.nonEmpty) {
      idColumnNames.map(_.toLowerCase.encodeSpecialCharacters).map(dataFrame.col)
    }
    else {
      List(monotonically_increasing_id(), lit(dataFrame.hashCode()))
    }
    idGenerationStrategy match {
      case HashedId =>
        val viewLiteral = functions.lit(elementViewKey.viewId.parts.mkString("."))
        val elementTypeLiterals = elementViewKey.elementType.toSeq.sorted.map(functions.lit)
        val columnsToHash = Seq(viewLiteral) ++ elementTypeLiterals ++ idColumns
        MorpheusFunctions.hash64(columnsToHash: _*).encodeLongAsMorpheusId(newIdColumn)
      case SerializedId =>
        val typeToId: Map[List[String], Int] =
          (schema.labelCombinations.combos.map(_.toList.sorted) ++ schema.relationshipTypes.map(List(_)))
            .toList
            .sortBy(s => s.mkString)
            .zipWithIndex.toMap
        val elementTypeToIntegerId = typeToId(elementViewKey.elementType.toList.sorted)
        val columnsToSerialize = functions.lit(elementTypeToIntegerId) :: idColumns
        MorpheusFunctions.serialize(columnsToSerialize: _*).as(newIdColumn)
    }
  }

  private def notFound(needle: Any, haystack: Traversable[Any] = Traversable.empty): Nothing =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${stringList(haystack)}" else "",
      actual = needle
    )

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")
}
