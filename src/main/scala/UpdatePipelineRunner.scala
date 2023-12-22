import org.apache.log4j.LogManager
import session.SessionManager

object UpdatePipelineRunner {

    def main(args: Array[String]) = {
        /*
            Compile:
                sbt test package

            Run pipeline:
                /spark/bin/spark-submit \
                   --conf spark.pipeline.fromInclusiveDate="2023-08-01 00:00:00" \
                   --conf spark.pipeline.tillExclusiveDate="2023-09-01 00:00:00" \
                   --master yarn \
                   target/scala-2.12/unifillupdatesparkpipeline*.jar
         */

        val log = LogManager.getLogger(this.getClass.getName)
        val sparkSession = SessionManager.createSession()
        val fromInclusiveDate: String = sparkSession.sparkContext.getConf.get("spark.pipeline.fromInclusiveDate")
        val tillExclusiveDate: String = sparkSession.sparkContext.getConf.get("spark.pipeline.tillExclusiveDate")

        log.info("Unifill Update Spark Pipeline")
        log.info("fromInclusiveDate: " + fromInclusiveDate)
        log.info("tillExclusiveDate: " + tillExclusiveDate)

        /*
            Check from date and to date - DONE


         */

    }
}
