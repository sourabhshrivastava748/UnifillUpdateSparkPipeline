package utils

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object UniwareUtils {
    val log = LogManager.getLogger(this.getClass.getName)

    def getProdServers(sparkSession: SparkSession, uniwareCommonMongoDbUri: String): Set[String] = {
        log.info("Getting prod server list")
        val mongodbOptions = Map(
            "spark.mongodb.input.uri" -> uniwareCommonMongoDbUri,
            "database" -> "uniwareConfig",
            "collection" -> "serverDetails"
        )

        val stage1 = "{'$match': {'production': 'true', 'active': 'true'}}"
        val stage2 = "{'$project': {'db': '$db'}}"

        val dataFrame = sparkSession.read
                .format("com.mongodb.spark.sql.DefaultSource")
                .options(mongodbOptions)
                .option("pipeline", stage1)
                .option("pipeline", stage2)
                .load()

        log.info("Prod Servers Count: " + dataFrame.count)
        dataFrame.show(false)
        Set()
    }

}
