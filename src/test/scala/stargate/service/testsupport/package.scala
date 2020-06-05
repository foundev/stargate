package stargate.service

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import stargate.cassandra.CassandraTable
import stargate.model._
import stargate.service.config._

import scala.concurrent.ExecutionContextExecutor
import scala.util.Random
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.servlet.DefaultServlet
import com.typesafe.scalalogging.Logger
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse.BodyHandlers
import scala.io.Source
import java.{util => ju}
import at.favre.lib.crypto.bcrypt.BCrypt

/**
 * provides test aide support
 */
package object testsupport {

  /**
   * for help with testing
   * @param entity entity to use
   * @param namespace namespace name
   * @param rand random number generator to share the same seed information
   * @param sgConfig parsed configuration
   */
  final case class ServletContext(
    entity: String,
    namespace: String,
    rand: Random,
    sgConfig: StargateConfig,
    shutdown: ()=> Unit,
  )

  /**
   * simple response object for http, dont' have to work with the full scale of HttpResponse or a
   * pariticular http client 
   *
   * @param StatusCode http status code
   * @param StatusMessage http status message
   * @param Body http body, can be null
   */
  final case class SimpleResponse(
    val statusCode: Int,
    val body: Option[String],
    val contentType: Option[String]
  )

  /**
    * creates a based64 encoded auth header string
    *
    * @param userName
    * @param password
    * @return
    */
  def makeBasicAuthHeader(username: String, password: String): String = {
    val encoded = ju.Base64.getEncoder().encodeToString((s"$username:$password").getBytes("UTF-8"))
    s"Basic $encoded"
  }

  /**
    * custom http request
    *
    * @param url
    * @param contentype
    * @param body
    * @param method
    * @param username
    * @param password
    * @return
    */
  def httpRequest(url: String, contentType: String, body: String, method: String, username: String = "", password: String = ""): SimpleResponse = {
       val requestBuilderRaw = HttpRequest.newBuilder()
        .uri(new URI(url))
        .method(method, BodyPublishers.ofString(body))
        .header("Content-Type", contentType)
     val httpRequest = if (Option(username).isDefined && Option(password).isDefined){ 
       requestBuilderRaw.header("Authorization", makeBasicAuthHeader(username, password))
       requestBuilderRaw.build()
     } else requestBuilderRaw.build()
      val response = HttpClient.newHttpClient()
      .send(httpRequest, BodyHandlers.ofString())
      val responseContentType = response.headers().firstValue("Content-Type").orElse(null)
      SimpleResponse(response.statusCode(), Option(response.body()), Option(responseContentType))
  }

  /**
   * simple http post
   *
   * @param url
   * @param contentType
   * @param body 
   * @param username
   * @param password
   * @return SimpleResponse 
   */
  def httpPost(url: String, contentType: String, body: String, username: String = "", password: String = ""): SimpleResponse = {
    httpRequest(url, contentType, body, "POST", username, password)
  }

  /**
   *  simple http get
   *
   * @param url
   * @param contentType
   * @param body
   * @return SimpleResponse
   */
  def httpGet(url: String, contentType: String, body: String, username: String = "", password: String = ""): SimpleResponse = {
    httpRequest(url, contentType, body, "GET", username, password)
  }

  /**
   *  simple http put
   *
   * @param url
   * @param contentType
   * @param body
   * @return SimpleResponse
   */
  def httpPut(url: String, contentType: String, body: String, username: String = "", password: String = ""): SimpleResponse = {
    httpRequest(url, contentType, body, "PUT", username, password)
  }

  /**
   *  simple http delete
   *
   * @param url
   * @param contentType
   * @param body
   * @return SimpleResponse
   */
  def httpDelete(url: String, contentType: String, body: String, username: String = "", password: String = ""): SimpleResponse = {
    httpRequest(url, contentType, body, "DELETE", username, password)
  }

  def startServlet(port: Int, authEnabled: Boolean, logger: Logger, systemKeyspace: String, rand: Random, namespace: String, clientConfig: CassandraClientConfig): ServletContext = {
    val entity = "Customer"
    logger.info("ensuring cassandra is started")
    val parsedStargateConfig = StargateConfig(
      port,
      100,
      100,
      32000L,
      32000L,
      32000L,
      systemKeyspace,
      clientConfig,
      AuthConfig(authEnabled, "admin", "$2a$12$gXNcu.xX8VDCtj0Oc22bHenzT.uFd.zqR/hNzf5ed9XCx6Fo4WXF.")
    )

    //launch java servlet
    val t = new Thread {
      override def run(): Unit = {
        stargate.service.serverStart(parsedStargateConfig)
      }
    }
    t.start()

    //need to hack in a sentinel
    Thread.sleep(5000)
    logger.info((s"attempting to create namespace ${namespace}"))
    val resource = Source.fromResource("schema.conf")
    if (resource.isEmpty) {
      throw new RuntimeException("missing schema.conf cannot initialize servlet")
    }
    val hoconBody = resource.getLines().mkString("\n")
    logger.info(s"posting body ${hoconBody}")
    var r: Option[SimpleResponse] = None
    if (authEnabled){
     r = Option(
      httpPost(
        s"http://localhost:${port}/${StargateApiVersion}/api/${namespace}/schema",
        "application/hocon",
        hoconBody,
        "admin",
        "sgAdmin1234"
        )
      )
    } else {
     r = Option(
      httpPost(
        s"http://localhost:${port}/${StargateApiVersion}/api/${namespace}/schema",
        "application/hocon",
        hoconBody
        )
      )
    }
    if (r.isEmpty) {
      throw new RuntimeException(
        s"unable to setup servlet test env as there was no response from the attempt to create a schema"
      )
    }
    if (r.get.statusCode != 200) {
      throw new RuntimeException(
        s"unable to setup servlet test env response code was ${r.get.statusCode}"
      )
    }
    logger.info("setting servlet context")
    val shutdown = ()=> {
      try{
        logger.info("starting servlet shutdown")
        t.interrupt()
      }catch {
        case _: Throwable => logger.info("shutdown of servlet thread")
      }
      ()
    }
    ServletContext(entity, namespace, rand, parsedStargateConfig, shutdown)
  }
}
