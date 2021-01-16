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
import org.junit.Test
import stargate.{CassandraTest, CassandraTestSession, keywords, schema, util}
import stargate.model.OutputModel

import scala.concurrent.ExecutionContext

trait NullKeysTestTrait extends CassandraTestSession {

  def checkPerfectMatch(model: OutputModel, entityName: String, conditions: List[Object]): Unit = {
    val groupedConditions = stargate.model.queries.parser.parseConditions(model.input.entities, List.empty, entityName, conditions)
    groupedConditions.foreach(pathConds => {
      val (path, conditions) = pathConds
      val targetEntityName = schema.traverseEntityPath(model.input.entities, entityName, path)
      val scores = schema.tableScores(conditions.map(_.named), model.entityTables(targetEntityName))
      assert(scores.keys.min.perfect)
    })
  }

  def longOf(l: Long): Object = java.lang.Long.valueOf(l)

  @Test
  def testNullKey: Unit = {
    val executor = ExecutionContext.global
    val keyspace = this.newKeyspace()
    val model = schema.defaultOutputModel(stargate.model.parser.parseModel(ConfigFactory.parseResources("optional-keys.conf")), keyspace)
    val crud = stargate.model.unbatchedCRUD(model, this.session, executor)
    val viewTable = model.entityTables("foo").filter(t => t.name != schema.baseTableName("foo")).head
    val keys = viewTable.columns.key.names.combined
    util.await(model.createTables(session, executor)).get

    val foos = List[Map[String,Object]](
      Map((keys(0), longOf(1)), (keys(1), longOf(1)), (keys(2), longOf(1))),
      Map((keys(0), longOf(1)), (keys(1), longOf(1))),
      Map((keys(0), longOf(1)), (keys(1), longOf(2)))
    )
    val res = util.await(util.sequence(foos.map(foo => crud.create("foo", foo)), executor)).get

    val conds1 = List[Object](keys(0), "=", longOf(1), keys(1), "=", longOf(1))
    val query1 = Map((keywords.mutation.MATCH, conds1))
    checkPerfectMatch(model, "foo", conds1)
    val res1 = util.await(crud.get("foo", query1).toList(executor)).get
    assert(res1.length == 2)

    val conds2 = List[Object](keys(0), "=", longOf(1), keys(1), "IN", List(longOf(1), longOf(2)))
    val query2 = Map((keywords.mutation.MATCH, conds2))
    checkPerfectMatch(model, "foo", conds2)
    val res2 = util.await(crud.get("foo", query2).toList(executor)).get
    assert(res2.length == 3)

  }

}
