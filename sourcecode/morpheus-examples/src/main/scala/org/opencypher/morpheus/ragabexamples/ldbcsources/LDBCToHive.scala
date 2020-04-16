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

package org.opencypher.morpheus.ragabexamples.ldbcsources

import java.io.{File, FileOutputStream}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.io.sql.SqlDataSourceConfig.Hive
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.util.App
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.morpheus.util.LdbcUtil._
import org.opencypher.morpheus.testing.utils.FileSystemUtils._


object LDBCToHive extends App {



  /**
    * This demo reads data generated by the LDBC SNB data generator and performs the following steps:
    *
    * 1) Loads the raw CSV files into Hive tables
    * 2) Normalizes tables according to the LDBC schema (i.e. place -> [City, Country, Continent]
    * 3) Generates a Graph DDL script based on LDBC naming conventions (if not already existing)
    * 4) Initializes a SQL PGDS based on the generated Graph DDL file
    * 5) Runs a Cypher query over the LDBC graph in Spark
    *
    * More detail about the LDBC SNB data generator are available under https://github.com/ldbc/ldbc_snb_datagen
    */




  val database = "ldbc3"
  val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"
  //val warehouseLocation = "/user/hive/warehouse"
  implicit val resourceFolder: String = "/ldbc"
  val datasourceName = "warehouse"


  //SparkConf
  val conf = new SparkConf(true)
  //conf.set("spark.driver.memory","100g")
  conf.set("spark.executor.memory","100g")
  conf.set("spark.sql.codegen.wholeStage", "true")
  conf.set("spark.sql.shuffle.partitions", "12")
  conf.set("spark.default.parallelism", "8")

  implicit  val spark = SparkSession
    .builder()
    .config(conf)
    .master("spark://172.17.77.48:7077")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("hive.metastore.uris","thrift://172.17.77.48:9083")
    .enableHiveSupport()
    .appName(s"RagabMorpheus-local-${UUID.randomUUID()}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  println("Spark Session created ")

  implicit val session: MorpheusSession = MorpheusSession.create(spark)





  //Function that only get the CSV files form the generated files of LDBC Dir
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith("0_0.csv"))
      .map(_.getName).toList
  }

    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
    spark.sql(s"CREATE DATABASE $database")

    println("Database $database Created !!!")


    val csvFiles = getListOfFiles("/data/LDBCDatasets/GeneratedRaw/LDBC_SF_3/social_network/")
    // Load LDBC data from CSV files into Hive tables
    csvFiles.foreach { csvFile =>
      println(csvFile)

      spark.read
        .format("csv")
        .option("header", value = true)
        .option("inferSchema", value = true)
        .option("delimiter", "|")
        .load(s"hdfs://172.17.77.48:9000/user/hadoop/ldbc3/$csvFile")
        // cast e.g. Timestamp to String
        .withCompatibleTypes
        .write
        .saveAsTable(s"$database.${csvFile.dropRight("_0_0.csv".length)}")
    }

 // Create views that normalize LDBC data where necessary
    val views = readFile(resource("sql/ldbc_views.sql").getFile).split(";")
    views.foreach(spark.sql)



}
