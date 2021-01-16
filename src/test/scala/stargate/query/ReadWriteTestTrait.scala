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

package stargate.query

import com.typesafe.config.{Config, ConfigFactory}
import org.junit.Test
import stargate.schema.ENTITY_ID_COLUMN_NAME
import stargate.{CassandraTestSession, cassandra, model, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait ReadWriteTestTrait extends CassandraTestSession {

  import ReadWriteTestTrait._

  @Test
  def testCreateDelete = {
    val keyspace = newKeyspace()
    val model = stargate.schema.defaultOutputModel(inputModel, keyspace)
    util.await(model.createTables(session, executor)).get

    List.range(0, 100).foreach(_ => {
      model.input.entities.values.foreach(entity => {
        val tables = model.entityTables(entity.name)
        val random = stargate.model.generator.createEntity(model.input.entities, entity.name, 1)
        val (id, statements) = stargate.query.write.createEntity(tables, random)
        val payload = random.updated(ENTITY_ID_COLUMN_NAME, id)
        val future = statements.map(cassandra.executeAsync(session, _, executor))
        Await.result(Future.sequence(future), Duration.Inf)
        tables.foreach(table => {
          val conditions = stargate.query.read.tableConditionsForEntity(table, payload).map(_.get)
          val select = stargate.query.read.selectStatement(table, conditions).build
          val rs = session.execute(select)
          assert(rs.iterator.asScala.toList.length == 1)
        })
        val deleteStatements = stargate.query.write.deleteEntity(tables, payload)
        val deleted = deleteStatements.map(cassandra.executeAsync(session, _, executor))
        Await.result(Future.sequence(deleted), Duration.Inf)
        tables.foreach(table => {
          val conditions = stargate.query.read.tableConditionsForEntity(table, payload).map(_.get)
          val select = stargate.query.read.selectStatement(table, conditions).build
          val rs = session.execute(select)
          assert(rs.iterator.asScala.toList.isEmpty)
        })
      })
    })
  }

}


object ReadWriteTestTrait {

  val modelConfig: Config = ConfigFactory.parseResources("read-write-test-schema.conf")
  val inputModel: model.InputModel = stargate.model.parser.parseModel(modelConfig)
  implicit val executor: ExecutionContext = ExecutionContext.global

}