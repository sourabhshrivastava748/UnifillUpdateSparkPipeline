package session

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object SessionManager {

    val log = LogManager.getLogger(this.getClass.getName)

    def createSession(): SparkSession = {
        val sparkSession = SparkSession.builder
            .master("yarn")
            .appName("UnifillUpdateSparkPipeline")
            .config("spark.files.useFetchCache", false)
            .config("spark.scheduler.allocation.file", "file:///spark/fair.xml")
            .config("spark.sql.shuffle.partitions", 1000)
            .getOrCreate()

        sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "production-fair")
        sparkSession.sparkContext.setLocalProperty("spark.scheduler.allocation.file", "file:///spark/fair.xml")
        sparkSession
    }

}
