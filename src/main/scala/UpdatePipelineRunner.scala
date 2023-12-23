import org.apache.log4j.LogManager
import session.{ResourceManager, SessionManager}
import utils.UniwareUtils

import java.io.FileNotFoundException
import java.util.Properties
import scala.io.Source

object UpdatePipelineRunner {

    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()
    val applicationProperties: Properties = ResourceManager.getApplicationProperties()
    val fromInclusiveDate: String = sparkSession.sparkContext.getConf.get("spark.pipeline.fromInclusiveDate")
    val tillExclusiveDate: String = sparkSession.sparkContext.getConf.get("spark.pipeline.tillExclusiveDate")
    val excludeServers: Set[String] = Set("db.myntra-in.unicommerce.infra", "db.lenskart-in.unicommerce.infra", "db.lenskartmp-in.unicommerce.infra", "db.ril-in.unicommerce.infra")


    def readTransformWriteInParallel(): Unit = {
        val prodServerSet = UniwareUtils.getProdServers(sparkSession, applicationProperties.getProperty("uniware.common.mongodb.uri"))

    }

    def main(args: Array[String]) = {
        log.info("Unifill Update Spark Pipeline")
        log.info("fromInclusiveDate: " + fromInclusiveDate)
        log.info("tillExclusiveDate: " + tillExclusiveDate)

        readTransformWriteInParallel()
    }
}
