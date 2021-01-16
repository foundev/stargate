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

import java.util.UUID
import java.util.concurrent.Executors

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.junit.Assert._
import org.junit.Test
import stargate.model._
import stargate.schema.ENTITY_ID_COLUMN_NAME
import stargate.{CassandraTestSession, cassandra, keywords, query, schema, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Random, Try}


trait EntityCRUDTestTrait extends CassandraTestSession {

  import EntityCRUDTestTrait._

  // checks that two entity trees are the same, ignoring missing or empty-list relations
  def diff(expected: Map[String,Object], queried: Map[String,Object]): Unit = {
    queried.keys.foreach(field => {
      val getVal = queried(field)
      if(getVal.isInstanceOf[List[Object]]) {
        val createVal = expected.get(field).map(_.asInstanceOf[List[Map[String,Object]]]).getOrElse(List.empty).sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        val getValList = getVal.asInstanceOf[List[Map[String,Object]]].sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        assertEquals(createVal.length, getValList.length)
        createVal.zip(getValList).map(cg => diff(cg._1, cg._2))
      } else if(!Set(schema.TRANSACTION_ID_COLUMN_NAME, schema.TRANSACTION_DELETED_COLUMN_NAME).contains(field)) {
        assertEquals(getVal, expected(field))
      }
    })
  }

  def chooseRandomRelation(model: OutputModel, entityName: String, entity: Map[String,Object]): RelationField = {
    val relations = model.input.entities(entityName).relations.values.filter(r => entity.contains(r.name)).toList
    relations(Random.nextInt(relations.length))
  }

  def updateNestedScalars(model: OutputModel, crud: CRUD, entityName: String, entity: Map[String,Object], executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val newValues = stargate.model.generator.entityFields(model.input.entities(randomRelation.targetEntityName).fields.values.toList)
    val updaateReq = newValues ++ Map((stargate.keywords.mutation.MATCH, List(randomRelation.inverseName + "." + ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val updateRes = crud.update(randomRelation.targetEntityName, updaateReq)
    updateRes.map(_ => {
      val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      val updatedChildren = children.map(_ ++ newValues)
      entity.updated(randomRelation.name, updatedChildren)
    })(executor)
  }

  def linkNestedRelation(model: OutputModel, crud: CRUD, entityName: String, entity: Map[String,Object], executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val newChildren = List.range(1, Random.between(2, 5)).map(_ => stargate.model.generator.createEntity(model.input.entities, randomRelation.targetEntityName, 1))
    val requestMatch = Map((stargate.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestLink = Map((randomRelation.name, Map((stargate.keywords.relation.LINK, Map((stargate.keywords.mutation.CREATE, newChildren))))))
    val request = requestMatch ++ requestLink
    val updateRes = crud.update(entityName,request)
    updateRes.map(response => {
      val linkedEntities = response(0)(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      linkedEntities.foreach(e => assertEquals(e(keywords.response.RELATION), keywords.response.RELATION_LINK))
      val linkedIds = linkedEntities.map(_(ENTITY_ID_COLUMN_NAME))
      assert(linkedIds.length == newChildren.length)
      val childrenWithIds = newChildren.zip(linkedIds).map(ci => ci._1.updated(ENTITY_ID_COLUMN_NAME, ci._2))
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]] ++ childrenWithIds))
    })(executor)
  }

  def unlinkNestedRelation(model: OutputModel, crud: CRUD, entityName: String, entity: Map[String,Object], executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((stargate.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestUnlink = Map((randomRelation.name, Map((stargate.keywords.relation.UNLINK, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))))
    val request = requestMatch ++ requestUnlink
    val updateRes = crud.update(entityName, request)
    updateRes.map(response => {
      assert(response.length == 1)
      val unlinkedEntities = response(0)(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      unlinkedEntities.foreach(e => assertEquals(e(keywords.response.RELATION), keywords.response.RELATION_UNLINK))
      val unlinkedIds = unlinkedEntities.map(_(ENTITY_ID_COLUMN_NAME))
      assert(unlinkedIds == List(randomChild))
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) != randomChild)))
    })(executor)
  }

  def replaceNestedRelation(model: OutputModel, crud: CRUD, entityName: String, entity: Map[String,Object], executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((stargate.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestUnlink = Map((randomRelation.name, Map((stargate.keywords.relation.UNLINK, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))))
    val request = requestMatch ++ requestUnlink
    val updateRes = crud.update(entityName, request)
    updateRes.map(response => {
      assert(response.length == 1)
      val relationResponse = response(0)(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      val linkedIds = relationResponse.filter(_(keywords.response.RELATION) == keywords.response.RELATION_LINK).map(_(ENTITY_ID_COLUMN_NAME))
      val unlinkedIds = relationResponse.filter(_(keywords.response.RELATION) == keywords.response.RELATION_UNLINK).map(_(ENTITY_ID_COLUMN_NAME))
      assert(linkedIds == List(randomChild))
      assert(unlinkedIds.length == children.length - 1)
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) == randomChild)))
    })(executor)
  }

  def deleteNestedEntity(model: OutputModel, crud: CRUD, entityName: String, entity: Map[String,Object], executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((stargate.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))
    val deleteRes = crud.delete(randomRelation.targetEntityName, requestMatch)
    deleteRes.flatMap(response => {
      val deleted = getEntities(model.input, crud, randomRelation.targetEntityName, randomChild, executor)
      deleted.map(deleted => { assert(deleted.isEmpty); response})(executor)
    })(executor).map(_ => {
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) != randomChild)))
    })(executor)
  }

  def crudTest(model: OutputModel, crud: CRUD, executor: ExecutionContext): Unit = {
    implicit val ec: ExecutionContext = EntityCRUDTestTrait.executor
    model.input.entities.keys.foreach(entityName => {
      List.range(0, 20).foreach(_ => {
        val created = Await.result(createEntityWithIds(model, crud, entityName, executor), Duration.Inf)
        val get1 = Await.result(getEntity(model.input, crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(created, get1)

        val updated = Await.result(updateNestedScalars(model, crud, entityName, created, executor), Duration.Inf)
        val get2 = Await.result(getEntity(model.input,crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(updated, get2)

        val linked = Await.result(linkNestedRelation(model, crud, entityName, updated, executor), Duration.Inf)
        val get3 = Await.result(getEntity(model.input, crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(linked, get3)

        val unlinked = Await.result(unlinkNestedRelation(model, crud, entityName, linked, executor), Duration.Inf)
        val get4 = Await.result(getEntity(model.input, crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(unlinked, get4)

        val replaced = Await.result(unlinkNestedRelation(model, crud, entityName, unlinked, executor), Duration.Inf)
        val get5 = Await.result(getEntity(model.input, crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(replaced, get5)

        val deleted = Await.result(deleteNestedEntity(model, crud, entityName, replaced, executor), Duration.Inf)
        val get6 = Await.result(getEntity(model.input, crud, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], executor), Duration.Inf)
        diff(deleted, get6)
      })
    })
  }

  @Test
  def crudTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.defaultOutputModel(inputModel, keyspace)
    util.await(model.createTables(session, executor)).get
    val crud = stargate.model.unbatchedCRUD(model, this.session, executor)
    crudTest(model, crud, executor)
  }

  @Test
  def batchedCrudTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.defaultOutputModel(inputModel, keyspace)
    util.await(model.createTables(session, executor)).get
    val crud = stargate.model.batchedCRUD(model, this.session, executor)
    crudTest(model, crud, executor)
  }

  @Test
  def rampCrudTest: Unit = {
    val executor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.rampOutputModel(inputModel, keyspace)
    util.await(model.createRampTables(session, executor)).get
    val crud = stargate.model.rampCRUD(model, this.session, executor)
    EntityCRUDTestTrait.session = this.session
    EntityCRUDTestTrait.keyspace = keyspace
    val t = Try(crudTest(model, crud, executor))
    if(t.isFailure) logger.error("failed", t.failed.get)
    t.get
  }
}

object EntityCRUDTestTrait {

  val executor: ExecutionContext = ExecutionContext.global
  var session: CqlSession = _
  var keyspace: String = _

  def create(model: OutputModel, crud: CRUD, entityName: String, executor: ExecutionContext): (Map[String,Object], Future[Map[String,Object]]) = {
    val request = stargate.model.generator.createEntity(model.input.entities, entityName)
    val response = crud.create(entityName, request).map(_(0))(executor)
    (request, response)
  }

  def zipEntityIds(model: InputModel, entityName: String, request: Map[String,Object], response: Map[String,Object]): Map[String,Object] = {
    val entity = model.entities(entityName)
    val id = Map((ENTITY_ID_COLUMN_NAME, response(ENTITY_ID_COLUMN_NAME)))
    val withFields = request.keys.filter(entity.fields.contains).foldLeft(id)((merge, field) => merge.updated(field, request.get(field).orNull))
    val withRelations = entity.relations.foldLeft(withFields)((merge, relation) => {
      assertEquals(request.contains(relation._1), response.contains(relation._1))
      if(!request.contains(relation._1)) {
        merge
      } else {
        val requestEntites = request(relation._1).asInstanceOf[List[Map[String,Object]]]
        val linkedEntities = response(relation._1).asInstanceOf[List[Map[String,Object]]]
        linkedEntities.foreach(e => assertEquals(e(keywords.response.RELATION), keywords.response.RELATION_LINK))
        assertEquals(requestEntites.length, linkedEntities.length)
        val zipped = requestEntites.zip(linkedEntities).map((req_resp) => zipEntityIds(model, relation._2.targetEntityName, req_resp._1, req_resp._2))
        merge ++ Map((relation._1, zipped))
      }
    })
    withRelations
  }

  def createEntityWithIds(model: OutputModel, crud: CRUD, entityName: String, executor: ExecutionContext): Future[Map[String,Object]] = {
    val (request, response) = create(model, crud, entityName, executor)
    response.map(zipEntityIds(model.input, entityName, request, _))(executor)
  }

  // get whole entity tree without looping back to parent entities
  def getRequestRelations(model: InputModel, entityName: String, visited: Set[String]): Map[String, Object] = {
    val relations = model.entities(entityName).relations.filter(r => !visited.contains(r._2.targetEntityName))
    relations.map(r => (r._1, getRequestRelations(model, r._2.targetEntityName, visited ++ Set(r._2.targetEntityName))))
  }
  def getRequestByEntityId(model: InputModel, entityName: String, entityId: UUID): Map[String, Object] = {
    val conditions = Map((stargate.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entityId)))
    conditions ++ getRequestRelations(model, entityName, Set(entityName))
  }
  def getEntities(model: InputModel, crud: CRUD, entityName: String, request: Map[String,Object], executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val parsedRequest = stargate.model.queries.parser.parseGet(model.entities, entityName, request)
    crud.getAndTruncate(entityName, request, 1000)
  }
  def getEntities(model: InputModel, crud: CRUD, entityName: String, entityId: UUID, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    getEntities(model, crud, entityName, getRequestByEntityId(model, entityName, entityId), executor)
  }
  def getEntity(model: InputModel, crud: CRUD, entityName: String, entityId: UUID, executor: ExecutionContext): Future[Map[String,Object]] = {
    import scala.jdk.CollectionConverters._
    getEntities(model, crud, entityName, entityId, executor).map(list => {
      assert(list.length == 1,
        entityId.toString + "\n" +
        session.execute(s"""select * from ${keyspace}."${entityName}" """).iterator.asScala.toList.map(cassandra.rowToMap).mkString("\n"))
      list(0)})(executor)
  }
  def getAllEntities(model: InputModel, crud: CRUD, entityName: String, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    getEntities(model, crud, entityName, getRequestRelations(model, entityName, Set(entityName)).updated(stargate.keywords.mutation.MATCH, "all"), executor)
  }

}
