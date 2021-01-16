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
package stargate

import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.server.Server
import scala.concurrent.ExecutionContext
import com.datastax.oss.driver.api.core.CqlSession
import org.eclipse.jetty.servlet.DefaultServlet
import io.prometheus.client.jetty.JettyStatisticsCollector
import org.eclipse.jetty.server.handler.StatisticsHandler
import io.prometheus.client.hotspot.DefaultExports
import stargate.service.config.StargateConfig
import org.eclipse.jetty.servlet.ServletHolder
import scala.reflect.api.Names
import javax.servlet.DispatcherType
import java.{util => ju}
import org.eclipse.jetty.servlet.FilterHolder
import stargate.service.security.BasicAuthFilter
import java.io.File
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.server.SecureRequestCustomizer
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.http2.HTTP2Cipher
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory

package object service {
  private val logger = Logger("stargage.service")
  val StargateApiVersion = "v1"
  // the getImplementationVersion is set by maven-jar-plugin
  val StargateVersion: String = getClass.getPackage().getImplementationVersion

  def logStartup() = {
    logger.info("Launch Mission To Stargate")
    logger.info(" -----------")
    logger.info("|     *   * |")
    logger.info("| *         |")
    logger.info("|    *      |")
    logger.info("|         * |")
    logger.info("|    *      |")
    logger.info(" -----------")
    logger.info("            ")
    logger.info("            ")
    logger.info("            ")
    logger.info("     ^^")
    logger.info("    ^^^^")
    logger.info("   ^^^^^^")
    logger.info("  ^^^^^^^^")
    logger.info(" ^^^^^^^^^^")
    logger.info("   ^^^^^^")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("        ")
    logger.info("      |  ")
    logger.info("0000     0000")
    logger.info("00000   00000")
    logger.info("============  ")
    logger.info(s"""StarGate Version: ${stargate.service.StargateVersion}""")
  }

  var registeredJettyMetrics = false
  var stats: StatisticsHandler = _

  def addSsl(server: Server, port: Int, keyStore: String, keyStorePass: String){
        // HTTP Configuration
        val httpConfig: HttpConfiguration = new HttpConfiguration()
        httpConfig.setSendServerVersion(false)
        httpConfig.setSecureScheme("https")
        httpConfig.setSecurePort(port)

        // SSL Context Factory for HTTPS and HTTP/2
        val sslContextFactory: SslContextFactory = new SslContextFactory()
        sslContextFactory.setKeyStorePath(keyStore) 
        sslContextFactory.setKeyStorePassword(keyStorePass)
        sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR)

        // HTTPS Configuration
        val httpsConfig: HttpConfiguration = new HttpConfiguration(httpConfig)
        httpsConfig.addCustomizer(new SecureRequestCustomizer())

        // HTTP/2 Connection Factory
        val h2: HTTP2ServerConnectionFactory = new HTTP2ServerConnectionFactory(httpsConfig)
        val alpn: ALPNServerConnectionFactory = new ALPNServerConnectionFactory()
        alpn.setDefaultProtocol("h2")

        // SSL Connection Factory
        val ssl: SslConnectionFactory = new SslConnectionFactory(sslContextFactory, alpn.getProtocol())

        // HTTP/2 Connector
        val http2Connector: ServerConnector = new ServerConnector(server, ssl, alpn, h2, new HttpConnectionFactory(httpsConfig))
        http2Connector.setPort(port)
        server.addConnector(http2Connector)
  }
  /**
    * strictly in here for testing
    */
  def serverStart(sgConfig: StargateConfig): Unit = {
    //initial configuration
    val cqlSession: CqlSession = cassandra.session(sgConfig.cassandra)
    val executor = ExecutionContext.global
    val datamodelRepoTable: cassandra.CassandraTable = datamodelRepository.createDatamodelRepoTable(sgConfig, cqlSession, executor)
    val namespaces = new Namespaces(datamodelRepoTable, cqlSession)
    namespaces.reload(cqlSession, executor)
    var server: Server = null
    //val server = new Server(sgConfig.httpPort)
    if (sgConfig.auth.getSsl()){
      server = new Server();
      addSsl(server, sgConfig.getHttpPort(), sgConfig.auth.getSslCert(), sgConfig.auth.getSslPass())
    } else {
      server = new Server(sgConfig.getHttpPort())
    }
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    if (sgConfig.auth.enabled){
      context.addFilter(new FilterHolder(new BasicAuthFilter(sgConfig.auth)), "/*" ,  ju.EnumSet.of(DispatcherType.INCLUDE,DispatcherType.REQUEST))
    }
    context.addServlet(new ServletHolder(new StargateServlet(sgConfig, cqlSession, namespaces, datamodelRepoTable, executor)), s"/${StargateApiVersion}/api/*")
    context.addServlet(new ServletHolder(new SwaggerServlet(namespaces, sgConfig)),"/api-docs/*")
    context.addServlet(classOf[DefaultServlet], "/")
     
    server.setHandler(context)
    //supposedly this can be run multiple times, but we are able to trigger this breaking in test
    DefaultExports.initialize()
    if (!registeredJettyMetrics){
      stats = new StatisticsHandler()
      //get current server handler and set it inside StatisticsHandler
      stats.setHandler(server.getHandler)
      //set StatisticsHandler as the handler for servlet
      server.setHandler(stats)
      new JettyStatisticsCollector(stats).register()
      // Register collector.
      registeredJettyMetrics = true
    }
    Runtime.getRuntime().addShutdownHook(new Thread(){
            override def run(): Unit  ={
                println("Stopping stargate")
                server.stop()
            }
    })

    logStartup()
    server.start
    server.join
  }
}
