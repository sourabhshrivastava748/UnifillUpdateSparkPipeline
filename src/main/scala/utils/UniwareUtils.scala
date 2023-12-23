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
        val stage = "[ {'$match': {'production': 'true', 'active': 'true'}}, {'$project': {'db': '$db'}} ]"

        val prodServersDf = sparkSession.read
                .format("com.mongodb.spark.sql.DefaultSource")
                .options(mongodbOptions)
                .option("pipeline", stage)
                .load()

        import sparkSession.implicits._
        log.info("Prod Servers Count: " + prodServersDf.count)
        prodServersDf.select("db").as[String].collect().toSet
    }

}
