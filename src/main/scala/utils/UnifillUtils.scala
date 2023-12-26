package utils

import entity.UniwareShippingPackage
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.UniwareUtils.{getJdbcUrlFromServerName, log}

object UnifillUtils {

    def getUnifillJdbcUrlFromUrl(url: String): String = {
        url + "?useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.3"
    }

    /**
     * rewriteBatchedStatements :
     *      https://sqlrelease.com/optimize-spark-dataframe-write-performance-for-jdbc
     *      https://stackoverflow.com/questions/26307760/mysql-and-jdbc-with-rewritebatchedstatements-true
     *
     * isolationLevel :
     *      https://stackoverflow.com/questions/71031178/how-to-make-spark-commit-in-each-batch-when-using-batchsize-and-writing-into-rdb
     */
    def writeUnifillJDBC(sparkSession: SparkSession, uniwareShippingPackageAddressDataset: Dataset[UniwareShippingPackage]): Unit = {
        val start = System.currentTimeMillis()
        val sparkConf: SparkConf = sparkSession.sparkContext.getConf
        val jdbcOptions = Map(
            "url" -> getUnifillJdbcUrlFromUrl(sparkConf.get("spark.unifill.mysqldb.url")),
            "user" -> sparkConf.get("spark.unifill.mysqldb.user"),
            "password" -> sparkConf.get("spark.unifill.mysqldb.password"),
            "driver" -> sparkConf.get("spark.unifill.mysqldb.driver"),
            "dbtable" -> sparkConf.get("spark.unifill.mysqldb.table"),
            "batchsize" -> sparkConf.get("spark.unifill.mysqldb.batchSize"),
            "rewriteBatchedStatements" -> "true",
            "isolationLevel" -> "NONE"
        )

        uniwareShippingPackageAddressDataset.write
                .format("jdbc")
                .options(jdbcOptions)
                .mode("append")
                .save()

        log.info("Completed writeUnifillJDBC, count: " + uniwareShippingPackageAddressDataset.count +
                " in " + ((System.currentTimeMillis() - start)/1000) + " sec")
    }

}
