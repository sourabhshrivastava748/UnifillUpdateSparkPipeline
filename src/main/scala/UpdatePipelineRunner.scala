import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import session.SessionManager
import utils.UniwareUtils

object UpdatePipelineRunner {

    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()
    val sparkConf: SparkConf = sparkSession.sparkContext.getConf

    val fromInclusiveDate: String = sparkConf.get("spark.pipeline.fromInclusiveDate")
    val tillExclusiveDate: String = sparkConf.get("spark.pipeline.tillExclusiveDate")
    val uniwareCommonMongoDbUri: String = sparkConf.get("spark.uniware.common.mongodb.uri")

    val excludeServers: Set[String] = Set("db.myntra-in.unicommerce.infra", "db.lenskart-in.unicommerce.infra", "db.lenskartmp-in.unicommerce.infra", "db.ril-in.unicommerce.infra")


    def readTransformWriteInParallel(): Unit = {
        val prodServerSet = UniwareUtils.getProdServers(sparkSession, uniwareCommonMongoDbUri)

    }

    def main(args: Array[String]) = {
        log.info("Unifill Update Spark Pipeline")
        log.info("fromInclusiveDate: " + fromInclusiveDate)
        log.info("tillExclusiveDate: " + tillExclusiveDate)

        readTransformWriteInParallel()
    }
}
