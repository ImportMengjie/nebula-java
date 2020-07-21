/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.importer.reader

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.{
  ClusterCountMapReduce,
  PeerPressureVertexProgram
}
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory
import org.graphframes.GraphFrame
import org.neo4j.spark.dsl.{LoadDsl, PartitionsDsl, QueriesDsl, SaveDsl}
import org.neo4j.spark.rdd.Neo4jRDD
import org.neo4j.spark.{Neo4j, Partitions}

import scala.reflect.ClassTag

/**
  * @{link ServerBaseReader} is the abstract class of
  *        It include a spark session and a sentence which will sent to service.
  * @param session
  * @param sentence
  */
abstract class ServerBaseReader(override val session: SparkSession, sentence: String)
    extends Reader {

  override def close(): Unit = {
    session.close()
  }
}

/**
  * @{link HiveReader} extends the @{link ServerBaseReader}.
  *        The HiveReader reading data from Apache Hive via sentence.
  * @param session
  * @param sentence
  */
class HiveReader(override val session: SparkSession, sentence: String)
    extends ServerBaseReader(session, sentence) {
  override def read(): DataFrame = {
    session.sql(sentence)
  }
}

/**
  * The @{link MySQLReader} extends the @{link ServerBaseReader}.
  * The MySQLReader reading data from MySQL via sentence.
  *
  * @param session
  * @param host
  * @param port
  * @param database
  * @param table
  * @param user
  * @param password
  * @param sentence
  */
class MySQLReader(override val session: SparkSession,
                  host: String = "127.0.0.1",
                  port: Int = 3699,
                  database: String,
                  table: String,
                  user: String,
                  password: String,
                  sentence: String)
    extends ServerBaseReader(session, sentence) {
  override def read(): DataFrame = {
    val url = s"jdbc:mysql://${host}:${port}/${database}?useUnicode=true&characterEncoding=utf-8"
    session.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
  }
}

/**
  * @{link Neo4JReader} extends the @{link ServerBaseReader}
  * @param session
  * @param sentence
  */
class Neo4JReader(override val session: SparkSession, partition: Int, sentence: String)
    extends ServerBaseReader(session, sentence) {

  class PartitionsWithOffset(override val rows: Long,
                             override val batchSize: Long,
                             eachPartitionOffset: List[Long])
      extends Partitions {

    require(eachPartitionOffset.forall(_ < 2 * batchSize))
    override val partitions: Long = eachPartitionOffset.length.toLong

    override def effective(): Partitions = {
      new PartitionsWithOffset(rows, batchSize, eachPartitionOffset)
    }

    override def limit(index: Int): Long = {
      require(index < eachPartitionOffset.length)
      val remainder = (rows % batchSize) - eachPartitionOffset(index)
      if (index < partitions - 1)
        batchSize - eachPartitionOffset(index)
      else remainder + batchSize
    }

    override def skip(index: Int): Long = {
      require(index < eachPartitionOffset.length)
      index * batchSize + eachPartitionOffset(index)
    }

    override def toString: String = s"Partitions with offset ${eachPartitionOffset.mkString(",")}"
  }

  class Neo4jWithOffset(sc: SparkContext, eachPartitionOffset: List[Long], sentence: String) {

    val totalCount: Long = {
      val returnIndex   = sentence.toUpperCase.lastIndexOf("RETURN") + "RETURN".length
      val countSentence = sentence.substring(0, returnIndex) + " count(*)"
      Neo4j(sc).cypher(countSentence).loadNodeRdds.collect().head.getLong(0)
    }
    println(totalCount)

    val query: String        = sentence + " SKIP $_skip LIMIT $_limit"
    val partitionCount: Long = eachPartitionOffset.length.toLong
    val batchSize: Long      = totalCount / partitionCount
    val partitions           = new PartitionsWithOffset(totalCount, batchSize, eachPartitionOffset)
    val neo4jRddWithOffset   = new Neo4jRDD(session.sparkContext, query, partitions = partitions)

    def loadDataFrame: DataFrame = {
      if (neo4jRddWithOffset.isEmpty())
        throw new RuntimeException(
          "Cannot infer schema-types from empty result, please use loadDataFrame(schema: (String,String)*)")
      val schema = neo4jRddWithOffset.repartition(1).first().schema
      val df     = session.createDataFrame(neo4jRddWithOffset, schema)
      df
    }
  }

  override def read(): DataFrame = {
    val eachPartitionOffset = List(50L, 10L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
    val neo4jWithOffset     = new Neo4jWithOffset(session.sparkContext, eachPartitionOffset, sentence)
    val df = neo4jWithOffset.loadDataFrame
    df.foreachPartition(rows=>{
      println(s"==${TaskContext.getPartitionId()}====")
      rows.foreach(println)
      println(s"==${TaskContext.getPartitionId()}====")
    })
    df
  }
}

/**
  * @{link JanusGraphReader} extends the @{link ServerBaseReader}
  * @param session
  * @param sentence
  * @param isEdge
  */
class JanusGraphReader(override val session: SparkSession,
                       sentence: String,
                       isEdge: Boolean = false)
    extends ServerBaseReader(session, sentence) {

  override def read(): DataFrame = {
    val graph = GraphFactory.open("conf/hadoop/hadoop-gryo.properties")
    graph.configuration().setProperty("gremlin.hadoop.graphWriter", classOf[PersistedOutputRDD])
    graph.configuration().setProperty("gremlin.spark.persistContext", true)

    val result = graph
      .compute(classOf[SparkGraphComputer])
      .program(PeerPressureVertexProgram.build().create(graph))
      .mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create())
      .submit()
      .get()

    if (isEdge) {
      result.graph().edges()
    } else {
      result.graph().variables().asMap()
    }
    null
  }
}

/**
  *
  * @param session
  * @param sentence
  */
class NebulaReader(override val session: SparkSession, sentence: String)
    extends ServerBaseReader(session, sentence) {
  override def read(): DataFrame = ???
}
