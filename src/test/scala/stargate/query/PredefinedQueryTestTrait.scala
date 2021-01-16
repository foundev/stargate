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

import com.typesafe.config.ConfigFactory
import org.junit.Assert._
import org.junit.Test
import stargate.{CassandraTestSession, query, util}
import stargate.model.{parser, queries}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

trait PredefinedQueryTestTrait extends CassandraTestSession {

  @Test
  def predefinedQueryTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("predefined-query-schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.defaultOutputModel(inputModel, keyspace)
    val executor = ExecutionContext.global
    val crud = stargate.model.unbatchedCRUD(model, this.session, executor)
    util.await(model.createTables(session, executor)).get

    List.range(0, 10).foreach(_ => {
      val entity = EntityCRUDTestTrait.createEntityWithIds(model, crud, "A", executor)
      Await.result(entity, Duration.Inf)
    })
    val req = queries.predefined.transform(model.input.queries("getAandB"),  Map((stargate.keywords.mutation.MATCH, Map.empty)))
    val entities = Await.result(stargate.query.getAndTruncate(query.Context(model, session, executor), "A", req, 10000), Duration.Inf)
    entities.foreach(a => {
      assertEquals(a.keySet, Set("entityId", "x", "y", "b"))
      a("b").asInstanceOf[List[Map[String,Object]]].foreach(b => {
        assertEquals(b.keySet, Set("entityId", "y", "z"))
      })
    })
  }
}
