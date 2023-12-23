package utils

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object UniwareUtils {
    val log = LogManager.getLogger(this.getClass.getName)

    def getProdServers(sparkSession: SparkSession, uniwareCommonMongoDbUri: String): Set[String] = {
        log.info("uniwareCommonMongoDbUri: " + uniwareCommonMongoDbUri)
        log.info("Getting prod server list")
        val mongodbOptions = Map(
            "spark.mongodb.input.uri" -> uniwareCommonMongoDbUri,
            "spark.mongodb.input.database" -> "uniwareConfig",
            "spark.mongodb.input.collection" -> "serverDetails"
        )

        val dataFrame = sparkSession.read
                .format("mongodb")
                .options(mongodbOptions)
                .load()

        dataFrame.show(false)
        Set()
    }

}
