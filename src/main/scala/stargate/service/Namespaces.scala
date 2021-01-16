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

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.LazyLogging
import io.swagger.v3.oas.models.info.{Contact, Info, License}
import io.swagger.v3.oas.models.media.{Content, MediaType, Schema}
import io.swagger.v3.oas.models.parameters.{PathParameter, QueryParameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{OpenAPI, Operation, PathItem, Paths}
import stargate.cassandra.CassandraTable
import stargate.model.{CRUD, Mode, OutputModel, generator}
import stargate.util

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.util.Try
import java.{util => ju}

class Namespaces(datamodelRepoTable: CassandraTable, cqlSession: CqlSession) 
extends LazyLogging{

  //I thought about making these the same map with a compound type, but for purposes of cleaness I decided to go ahead
  //and keep them separate..if someone prefers later go ahead and use a ConcurrentHashMap and 
  //put OutputModel and Swagger together
  private val namespaces = scala.collection.mutable.Map[String, (OutputModel, CRUD)]()
  private val swaggerInstances = scala.collection.mutable.Map[String, OpenAPI]()
  private val lock = new ReentrantReadWriteLock()

  def reload(session: CqlSession, executor: ExecutionContext): Unit = {
    try {
      lock.writeLock ().lock ()
      // reload previous datamodels
      Await.result (datamodelRepository.fetchAllLatestDatamodels (datamodelRepoTable, cqlSession, ExecutionContext.global), Duration.Inf).foreach (name_config => {
        val (name, (configString, mode)) = name_config
        logger.info (s"reloading datamodel from cassandra: ${name}")
        val maybeModel = Try (stargate.schema.outputModel(stargate.model.parser.parseModel (configString), name, mode) )
        if (maybeModel.isSuccess) {
          val crud = Mode.crud(mode, maybeModel.get, session, executor)
          namespaces (name_config._1) = (maybeModel.get, crud)
        } else {
          logger.error (s"failed to load model${name_config._1}: ", maybeModel.failed.get)
        }
      })
      //load all the previous models as swagger configuration
      namespaces.foreach (f => swaggerInstances.put(f._1, toSwagger(f._1, f._2._1)))
    } finally {
      lock.writeLock ().unlock ()
    }
  }

  private def toSwagger(name: String, outputModel: OutputModel): OpenAPI = {
    val oas: OpenAPI = new OpenAPI()
    val paths = new Paths()
    operations(name, outputModel).foreach(x=>paths.addPathItem(x._1, x._2))
    oas.info(buildInfo()).paths(paths)
    oas
  }
  
  private def buildInfo(): Info = {
    val info = new Info()
    info.title("The Stargate API")
    info.description("Docs for the Stargate API")
    info.setVersion(StargateVersion)
    //"https://github.com/datastax/stargate",
    val contact = new Contact()
    contact.name("DataStax")
    contact.url("https://github.com/datastax/stargate")
    contact.email("sales@datastax.com")
    info.contact(contact)
    val license = new License()
    license.name("Apache 2.0")
    license.url("https://apache.org/licenses/LICENSE-2.0")
    info.license(license)
    info
  }

  private def operations(ns: String, outputModel: OutputModel) :List[(String, PathItem)]= {
    val pathItems = mutable.ArrayBuffer[(String, PathItem)]()

    outputModel.input.entities.foreach(ev=>{
      val path = new PathItem()
      val deleteSample = util.toPrettyJson(generator.randomDeleteRequest(outputModel.input.entities, ev._1))
      path.delete(new Operation()
        .tags(List(ev._1).asJava)
          .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")))
        .requestBody(new RequestBody()
          .description("payload to delete entities")
          .content(new Content()
            .addMediaType("application/json", new MediaType()
              .schema(new Schema().`type`("string").example(deleteSample))
          ))))
      val createSample = util.toPrettyJson(generator.randomCreateRequest(outputModel.input.entities, ev._1))
      path.post(new Operation()
          .tags(List(ev._1).asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
             // .schema(new Schema().$ref(s"#/components/schemas/${ev._1}"))
              ))))
        .requestBody(new RequestBody()
          .description("payload to create entity")
          .content(new Content()
            .addMediaType("application/json", new MediaType()
              .schema(new Schema().`type`("string").example(createSample))
          ))))
      val updateSample = util.toPrettyJson(generator.randomUpdateRequest(outputModel.input.entities, ev._1))
      path.put(new Operation()
        .tags(List(ev._1).asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
             // .schema(new Schema().$ref(s"#/components/schemas/${ev._1}"))
              ))))
        .requestBody(new RequestBody()
          .description("payload to update entity")
          .content(new Content()
            .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(updateSample))
          ))))
      val getSample = util.toPrettyJson(generator.randomGetRequest(outputModel.input.entities, ev._1, 100))
      path.get(new Operation()
        .tags(List(ev._1).asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
         //     .schema(new Schema().$ref(s"#/components/schemas/$ev._1"))
          ))))
          .addParametersItem(new QueryParameter()
            .name("payload")
            .required(true)
          .description("query payload")
            .content(new Content()
              .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(getSample)))
          )))
      val url = s"/$StargateApiVersion/api/$ns/entity/${ev._1}"
      pathItems.addOne(url -> path)
      //paths for entityId ..everything but post
      val entityIdPath = new PathItem()
      val sampleEntityId = ju.UUID.randomUUID();
      entityIdPath.get(new Operation()
        .tags(List(ev._1).asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
         //     .schema(new Schema().$ref(s"#/components/schemas/$ev._1"))
          ))))
          .addParametersItem(new PathParameter()
            .name("entityId")
            .required(true)
          .description("entityId to get")
            .content(new Content()
              .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(sampleEntityId.toString)))
          ))
      )
      //TODO: this looks like a bug but intentionally using create here as it will 
      //generate a valid enough body for updates and will not include 
      //the match conditions that the updateSample does, this is 
      //a temporary hack and should be removed as soon as we have a new generator
      //written for updates that does not do the parent match condition.
      val updateEntitySample = util.toPrettyJson(generator.randomCreateRequest(outputModel.input.entities, ev._1))
      entityIdPath.put(new Operation()
        .tags(List(ev._1).asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
             // .schema(new Schema().$ref(s"#/components/schemas/${ev._1}"))
              ))))
        .requestBody(new RequestBody()
          .description("payload to update entity")
          .content(new Content()
            .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(updateSample))
          )))
        .addParametersItem(new PathParameter()
            .name("entityId")
            .required(true)
          .description("entityId to get")
            .content(new Content()
              .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(sampleEntityId.toString)))
          ))
      )
      entityIdPath.delete(new Operation()
        .tags(List(ev._1).asJava)
          .responses(new ApiResponses().addApiResponse("200", new ApiResponse().description("success")))
        .addParametersItem(new PathParameter()
            .name("entityId")
            .required(true)
          .description("entityId to delete")
            .content(new Content()
              .addMediaType("application/json", new MediaType()
                .schema(new Schema().`type`("string").example(sampleEntityId.toString)))
          ))
      )
      val entityIdUrl = s"/$StargateApiVersion/api/$ns/entity/${ev._1}/{entityId}"
      pathItems.addOne(entityIdUrl -> entityIdPath)
    })

    val continuePath = new PathItem()
      continuePath.get(new Operation()
        .summary("continue paging through a result set")
        .description("Can continue existing queries by passing an id from a previous query")
      .tags(List("Continue Query").asJava)
          .responses(new ApiResponses().addApiResponse("200", new ApiResponse()
              .description("success")
              .content(new Content().addMediaType("application/json",
              new MediaType()
              //
              // .schema(new Schema().$ref(s"#/components/schemas/${ev._1}"))
          ))))
          .addParametersItem(new PathParameter()
            .name("id")
            .required(true)
            .description("id of the original query")
              .content(new Content()
              .addMediaType("text/plain", new MediaType()
              .schema(new Schema().`type`("string")
                .example("5ed0b80b-2934-4c58-acba-830d8bce7d13"))
              )))
      )
    val continueUrl = s"/$StargateApiVersion/api/$ns/continueQuery/{id}"
    pathItems.addOne(continueUrl, continuePath)
    val storedSample = """
                         | {
                         |   "-match":{
                         |       "customerName": "name"
                         |    }
                         | }
                         |""".stripMargin
    outputModel.input.queries.foreach(ev=>{
      val path = new PathItem()
      path.get(new Operation()
        .tags(List("Stored Queries").asJava)
        .responses(new ApiResponses().addApiResponse("200", new ApiResponse()
          .description("success")
          .content(new Content().addMediaType("application/json",
            new MediaType()
            // .schema(new Schema().$ref(s"#/components/schemas/${ev._1}"))
          ))))
        .addParametersItem(new QueryParameter()
          .required(true)
          .name("payload")
          .description("payload to query")
          .content(new Content()
            .addMediaType("application/json", new MediaType()
              .schema(new Schema().`type`("string").example(storedSample))
            )))
      )
      val url = s"/$StargateApiVersion/api/$ns/query/${ev._1}"
      pathItems.addOne(url -> path)
    })
    pathItems.toList
  }

  def get(key: String): (OutputModel, CRUD) = {
    try{
      lock.readLock().lock()
      namespaces(key)
    }finally{
      lock.readLock().unlock()
    }
  }

  def remove(key: String) = {
    try{
      lock.writeLock().lock()
      namespaces.remove(key)
      swaggerInstances.remove(key)
    } finally {
      lock.writeLock().unlock()
    }
  }

  def put(key: String, outputModel: OutputModel, crud: CRUD) = {
    try{
      lock.writeLock().lock()
      namespaces.put(key, (outputModel, crud))
      swaggerInstances.put(key, toSwagger(key, outputModel))
    } finally {
      lock.writeLock().unlock()
    }
  }
  
  def getSwagger(key: String): Option[OpenAPI] = {
    try{
      lock.readLock().lock()
      swaggerInstances.get(key)
    } catch {
      case e: Exception => Option[OpenAPI](null)
    } finally{
      lock.readLock().unlock()
    }
  }
}
