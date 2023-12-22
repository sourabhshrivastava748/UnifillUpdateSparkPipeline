import org.apache.log4j.LogManager
import session.SessionManager

object UpdatePipelineRunner {

    def main(args: Array[String]) = {
        /*
        Compile:
            sbt test package

        Command to run pipeline:
            /spark/bin/spark-submit \
               --conf fromInclusiveDate="2023-08-01 00:00:00"
               --conf tillExclusiveDate="2023-09-01 00:00:00"
               --master yarn
               target/scala-2.12/updatepipelinerunner*.jar
         */

        val log = LogManager.getRootLogger
        val sparkSession = SessionManager.createSession()
        val fromInclusiveDate: String = sparkSession.sparkContext.getConf.get("fromInclusiveDate")
        val tillExclusiveDate: String = sparkSession.sparkContext.getConf.get("tillExclusiveDate")

        log.info("Unifill Update Spark Pipeline")
        log.info("fromInclusiveDate: " + fromInclusiveDate)
        log.info("tillExclusiveDate: " + tillExclusiveDate)

        /*
            Check from date and to date

         */

    }
}
