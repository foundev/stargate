/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate.service

import java.time.Instant

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.term.Term
import stargate.cassandra.{CassandraColumn, CassandraColumns, CassandraKey, CassandraTable, DefaultCassandraColumn}
import stargate.model.{Mode, ScalarComparison, ScalarCondition}
import stargate.query.{read, write}
import stargate.{cassandra, model}
import stargate.util
import stargate.service.config.StargateConfig

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.concurrent.ExecutionContextExecutor

object datamodelRepository {

  val TABLE_NAME = "datamodel_repo"
  val NAME_COLUMN = "name"
  val TIMESTAMP_COLUMN = "timestamp"
  val DATAMODEL_COLUMN = "datamodel"
  val MODE_COLUMN = "mode"
  /**
    * factory method for CassandraTable
    *
    * @param sgConfig provides the keyspace to use and number of replicas
    * @param cqlSession
    * @return
    */
  def createDatamodelRepoTable(sgConfig: StargateConfig, cqlSession: CqlSession, executor: ExecutionContextExecutor): CassandraTable = {
    util.await(datamodelRepository.ensureRepoTableExists(sgConfig.stargateKeyspace, sgConfig.cassandra.cassandraReplication, cqlSession, executor)).get
  }

  def repoTable(keyspace: String): CassandraTable = {
    CassandraTable(keyspace, TABLE_NAME, CassandraColumns(
      CassandraKey(List(DefaultCassandraColumn(NAME_COLUMN, DataTypes.TEXT)), List(DefaultCassandraColumn(TIMESTAMP_COLUMN, DataTypes.TIMESTAMP))),
      List(DefaultCassandraColumn(DATAMODEL_COLUMN, DataTypes.TEXT), DefaultCassandraColumn(MODE_COLUMN, DataTypes.TEXT))
    ))
  }
  def ensureRepoTableExists(keyspace: String, replication: Map[String, Integer], session: CqlSession, executor: ExecutionContext): Future[CassandraTable] = {
    val table = repoTable(keyspace)
    val ensureKeyspace = cassandra.createKeyspaceAsync(session, keyspace, replication, executor)
    val ensureTable = ensureKeyspace.flatMap(_ => cassandra.createTableAsync(session, table))(executor)
    ensureTable.map(_ => table)(executor)
  }

  def updateDatamodel(appName: String, datamodel: String, mode: Mode.Value, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    require(Try(model.parser.parseModel(datamodel)).isSuccess)
    val row: Map[String,Object] = Map((NAME_COLUMN, appName), (TIMESTAMP_COLUMN, java.lang.Long.valueOf(System.currentTimeMillis())), (DATAMODEL_COLUMN, datamodel), (MODE_COLUMN, mode.toString))
    cassandra.executeAsync(session, write.insertStatement(repoTable.keyspace, repoTable.name, row).build, executor)
  }
  def updateDatamodelBlocking(appName: String, datamodel: String, mode: Mode.Value, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Unit = {
    Await.result(updateDatamodel(appName, datamodel, mode, repoTable, session, executor), Duration.Inf)
  }

  def fetchDatamodels(condition: List[ScalarCondition[Object]], repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[(String,Instant),(String,Mode.Value)]] = {
    val rows = cassandra.queryAsync(session, read.selectStatement(repoTable.keyspace, repoTable.name, condition).build, executor)
    rows.toList(executor).map(_.map(cassandra.rowToMap).map(row =>
      ((row(NAME_COLUMN).asInstanceOf[String], row(TIMESTAMP_COLUMN).asInstanceOf[Instant]), (row(DATAMODEL_COLUMN).asInstanceOf[String], Mode.fromString(row(MODE_COLUMN).asInstanceOf[String]))))
      .toMap)(executor)
  }
  def fetchLatestDatamodels(condition: List[ScalarCondition[Object]], repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[String,(String, Mode.Value)]] = {
    fetchDatamodels(condition, repoTable, session, executor).map(byNameAndTime => {
      val latestTimes = byNameAndTime.toList.groupMap(_._1._1)(_._1._2).view.mapValues(_.max).toMap
      latestTimes.map((nameTime:(String,Instant)) => (nameTime._1, byNameAndTime(nameTime._1, nameTime._2)))
    })(executor)
  }

  def fetchDatamodel(appName: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[List[(Instant,String,Mode.Value)]] = {
    val condition = List(ScalarCondition[Object](NAME_COLUMN, ScalarComparison.EQ, appName))
    fetchDatamodels(condition, repoTable, session, executor).map(_.toList.map(ntc => (ntc._1._2, ntc._2._1, ntc._2._2)))(executor)
  }
  def fetchLatestDatamodel(appName: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Option[(String,Mode.Value)]] = {
    val condition = List(ScalarCondition[Object](NAME_COLUMN, ScalarComparison.EQ, appName))
    fetchLatestDatamodels(condition, repoTable, session, executor).map(_.headOption.map(_._2))(executor)
  }

  def fetchAllDatamodels(repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[(String,Instant),(String,Mode.Value)]] = fetchDatamodels(List.empty, repoTable, session, executor)
  def fetchAllLatestDatamodels(repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[String,(String,Mode.Value)]] = fetchLatestDatamodels(List.empty, repoTable, session, executor)

  def deleteDatamodel(appName:String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    val statement = write.deleteStatement(repoTable.keyspace, repoTable.name, List(ScalarCondition[Object](NAME_COLUMN, ScalarComparison.EQ, appName)))
    cassandra.executeAsync(session, statement.build, executor)
  }
}
