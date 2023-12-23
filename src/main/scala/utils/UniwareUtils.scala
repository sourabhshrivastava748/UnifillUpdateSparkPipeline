package utils

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object UniwareUtils {
    val log = LogManager.getLogger(this.getClass.getName)

    def getProdServers(sparkSession: SparkSession, uniwareCommonMongoDbUri: String): Set[String] = {
        log.info("Getting prod server list")
        val mongodbOptions = Map(
            "spark.mongodb.read.connection.uri" -> uniwareCommonMongoDbUri,
            "database" -> "uniwareConfig",
            "collection" -> "serverDetails"
        )

        val dataFrame = sparkSession.read
                .format("com.mongodb.spark.sql.DefaultSource")
                .options(mongodbOptions)
                .load()

        log.info("Prod Servers")
        dataFrame.show(false)
        Set()
    }

}
