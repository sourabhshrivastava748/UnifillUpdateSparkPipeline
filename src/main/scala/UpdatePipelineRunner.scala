import entity.UniwareShippingPackage
import org.apache.log4j.LogManager
import org.apache.spark.sql.Dataset
import session.SessionManager
import utils.{UnifillUtils, UniwareUtils}

import scala.collection.mutable.ListBuffer

object UpdatePipelineRunner {

    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()

    val excludeServers: Set[String] = Set("db.myntra-in.unicommerce.infra", "db.lenskart-in.unicommerce.infra", "db.lenskartmp-in.unicommerce.infra", "db.ril-in.unicommerce.infra")

    def readTransformWrite(serverName: String): Unit = {
        val shippingPackageAddressDataset: Dataset[UniwareShippingPackage] = UniwareUtils.readUniwareJDBC(sparkSession, serverName)
        // shippingPackageAddressDataset.show(false)
        UnifillUtils.writeUnifillJDBC(sparkSession, shippingPackageAddressDataset);
    }

    def readTransformWriteInParallel(): Unit = {
        val prodDbServerSet = UniwareUtils.getProdServers(sparkSession).diff(excludeServers)
        log.info("Prod server count: " + prodDbServerSet.size)
        // log.info("Prod servers: " + prodDbServerSet)
        // readTransformWrite("db.ecloud1-in.unicommerce.infra")

        val listThreads: ListBuffer[Thread] = ListBuffer[Thread]()
        for (servername: String <- prodDbServerSet) {
            val thread = new Thread {
                override
                def run: Unit = readTransformWrite(servername)
            }
            thread.start()
            listThreads.append(thread)
        }

        for (thread <- listThreads) {
            thread.join()
        }
    }

    def main(args: Array[String]) = {
        log.info("Unifill Update Spark Pipeline")

        readTransformWriteInParallel()
        /*
            readUniwareJDBC
                - fetch query
                - read in spark, display output
                - update query
                - spark update test
                - bulk update query
                - mysql tuning
         */
    }
}
