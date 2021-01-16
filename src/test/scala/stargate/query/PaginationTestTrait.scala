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

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.junit.Test
import stargate.model.{CRUD, InputModel, OutputModel, parser}
import stargate.{CassandraTestSession, model, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait PaginationTestTrait extends CassandraTestSession {

  def generate(crud: CRUD, branching: Int, executor: ExecutionContext): Future[Unit] = {
    val c = List.range(0, branching).map(_ => Map.empty[String,Object])
    val b = List.range(0, branching).map(_ => Map(("c", c)))
    val a = List.range(0, branching).map(_ => Map(("b", b), ("c", c)))
    crud.create("A", a).map(_ => ())(executor)
  }

  def query(model: InputModel, crud: CRUD, limit: Int, branching: Int, executor: ExecutionContext): Unit = {
    val req = Map[String,Object](
      (stargate.keywords.mutation.MATCH, "all"),
      (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
      (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
      ("b", Map(
        (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
        (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        ("c", Map(
          (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
          (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        )),
      ))
    )
    val parsedReq = stargate.model.queries.parser.parseGet(model.entities, "A", req)
    val async = crud.get("A", req)
    val (entities, continue, streams) = Await.result(pagination.truncate(model, "A", parsedReq.selection, async, limit, 0, executor), Duration.Inf)
    assert(entities.length <= limit)
    streams.values.foreach((ttl_stream) => Await.result(ttl_stream.entities.length(executor), Duration.Inf) == branching - limit)
  }

  def paginationTest(model: InputModel, crud: CRUD, branching: Int, limit: Int, executor: ExecutionContext): Unit = {
    Await.result(generate(crud, branching, executor), Duration.Inf)
    query(model, crud, limit, branching, executor)
  }

  @Test
  def paginationTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("pagination-schema.conf"))
    val executor = ExecutionContext.global
    val keyspace = newKeyspace()
    val model = stargate.schema.defaultOutputModel(inputModel, keyspace)
    val crud = stargate.model.unbatchedCRUD(model, this.session, executor)
    util.await(model.createTables(session, executor)).get
    paginationTest(model.input, crud, 5, 3, executor)
  }
}
