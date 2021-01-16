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

import org.junit.{AfterClass, BeforeClass}
import stargate.CassandraTest
import stargate.service.config.StargateConfig
import stargate.service.testsupport._

import scala.io.Source
import scala.util.Random

object MergedServletTest extends CassandraTest {
  var sc: ServletContext = _
  val rand: Random = new Random()
  @BeforeClass
  def startup() = {
    this.init()
    val namespace = this.registerKeyspace(s"sgTest${rand.nextInt(10000)}")
    val systemKeyspace = this.newKeyspace()
    val clientConfig = this.clientConfig
    val authEnabled = false
    sc = startServlet(9090, false, logger, systemKeyspace, rand, namespace, this.clientConfig)
  }
  

  @AfterClass
  def shutdown() = {
    sc.shutdown() 
    this.cleanup()
    logger.info("test complete executing cleanup")
  }
}

class MergedServletTest extends QueryServletTest with StargateServletTest with SwaggerServletTest {

  override def registerKeyspace(keyspace: String): String = MergedServletTest.registerKeyspace(keyspace)

}
