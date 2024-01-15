package utils

import entity.UniwareShippingPackage
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object UniwareUtils {

    def containsShippingCourier(sparkSession: SparkSession, server: String): Boolean = {
        val sparkConf: SparkConf = sparkSession.sparkContext.getConf
        val query = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA = 'uniware' " +
                "and TABLE_NAME = 'shipping_package' and COLUMN_NAME = 'shipping_courier'"

        val jdbcOptions = Map(
            "url" -> getJdbcUrlFromServerName(server),
            "user" -> sparkConf.get("spark.uniware.mysqldb.user"),
            "password" -> sparkConf.get("spark.uniware.mysqldb.password"),
            "driver" -> sparkConf.get("spark.uniware.mysqldb.driver"),
            "query" -> query,
            "header" -> "true",
            "inferSchema" -> "true",
            "mode" -> "failfast",
            "fetchSize" -> sparkConf.get("spark.uniware.mysqldb.fetchSize"),
        )
        val dataset = sparkSession.read
                .format("jdbc")
                .options(jdbcOptions)
                .load()

        dataset.show(false)
        dataset.count() > 0
    }


    val log = LogManager.getLogger(this.getClass.getName)

    def getProdServers(sparkSession: SparkSession): Set[String] = {
        log.info("Getting prod server list")
        val uniwareCommonMongoDbUri = sparkSession.sparkContext.getConf.get("spark.uniware.common.mongodb.uri")
        val mongodbOptions = Map(
            "spark.mongodb.input.uri" -> uniwareCommonMongoDbUri,
            "database" -> "uniwareConfig",
            "collection" -> "serverDetails"
        )
        val stage = "[ {'$match': {'production': 'true', 'active': 'true'}}, {'$project': {'db': '$db'}} ]"

        try{
            val prodServersDf = sparkSession.read
                    .format("com.mongodb.spark.sql.DefaultSource")
                    .options(mongodbOptions)
                    .option("pipeline", stage)
                    .load()
            import sparkSession.implicits._
            log.info("Prod Servers Count: " + prodServersDf.count)
            prodServersDf.select("db").as[String].collect().toSet
        } catch {
            case ex: Exception => {
                log.error("Exception occurred while reading prod db server from mongo")
                ex.printStackTrace()
                null
            }
        }
    }

    def getJdbcUrlFromServerName(serverName: String): String = {
        "jdbc:mysql://" + serverName + ":3306/uniware?useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.3"
    }

    def getUniwareShippingPackageAddressQuery(fromInclusiveDate: String, tillExclusiveDate: String): String = {
        """SELECT
          |       sp.created                        AS uniware_sp_created,
          |       tenant.code                       AS tenant_code,
          |       party.code                        AS facility_code,
          |       sp.code                           AS shipping_package_code,
          |       spro.shipping_source_code         AS shipping_provider_source_code,
          |       sp.shipping_courier               AS shipping_courier,
          |       so.payment_method_code            AS payment_method,
          |       sum(ii.total)                     AS gmv,
          |       CAST(sum(ii.quantity) AS SIGNED)  AS quantity
          |FROM   shipping_package sp
          |        STRAIGHT_JOIN address_detail ad
          |            ON ad.id = sp.shipping_address_id
          |        STRAIGHT_JOIN sale_order so
          |            ON sp.sale_order_id = so.id
          |        STRAIGHT_JOIN tenant
          |            ON tenant.id = so.tenant_id
          |        STRAIGHT_JOIN facility
          |            ON facility.id = sp.facility_id
          |        STRAIGHT_JOIN party
          |            ON facility.id = party.id
          |        LEFT JOIN shipping_provider spro
          |            ON (spro.code = sp.shipping_provider_code AND spro.tenant_id = so.tenant_id)
          |        LEFT JOIN invoice_item ii
          |            ON sp.invoice_id = ii.invoice_id
          | WHERE sp.status_code IN ('CANCELLED','RETURNED','RETURN_ACKNOWLEDGED','RETURN_EXPECTED','SHIPPED','DISPATCHED','MANIFESTED','DELIVERED')
          |       AND
          |        tenant.code NOT IN ('lenskart91', 'lenskartcom77', 'lenskartcom97','myntracom70','myntracom73')
          |       AND
          |        sp.created >= '""".stripMargin + fromInclusiveDate +
        """'
          |       AND
          |        sp.created < '""".stripMargin + tillExclusiveDate +
        """'
          |       AND
          |        ad.country_code = "IN"
          |       AND
          |        ad.phone NOT LIKE '*%'
          |       AND
          |        ad.phone NOT IN ('9999999999', '0000000000', '8888888888', '1111111111', '9898989898', '0123456789', '1234567890', '0987654321', '09999999999')
          |   GROUP BY sp.code""".stripMargin
    }

    /**
     * fetchsize: The JDBC fetch size, which determines how many rows to fetch per round trip. This can help
     * performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows). This option applies
     * only to reading.
     *
     * https://stackoverflow.com/questions/1318354/what-does-statement-setfetchsizensize-method-really-do-in-sql-server-jdbc-driv
     */
    def readUniwareJDBC(sparkSession: SparkSession, serverName: String): Dataset[UniwareShippingPackage] = {
        val start = System.currentTimeMillis()
        val sparkConf: SparkConf = sparkSession.sparkContext.getConf

        val fromInclusiveDate = sparkConf.get("spark.pipeline.fromInclusiveDate")
        val tillExclusiveDate = sparkConf.get("spark.pipeline.tillExclusiveDate")
        val query = getUniwareShippingPackageAddressQuery(fromInclusiveDate, tillExclusiveDate)

        import sparkSession.implicits._
        val jdbcOptions = Map(
            "url" -> getJdbcUrlFromServerName(serverName),
            "user" -> sparkConf.get("spark.uniware.mysqldb.user"),
            "password" -> sparkConf.get("spark.uniware.mysqldb.password"),
            "driver" -> sparkConf.get("spark.uniware.mysqldb.driver"),
            "query" -> query,
            "header" -> "true",
            "inferSchema" -> "true",
            "mode" -> "failfast",
            "fetchSize" -> sparkConf.get("spark.uniware.mysqldb.fetchSize"),
        )
        val uniwareShippingPackageAddressDataset: Dataset[UniwareShippingPackage] = sparkSession.read
                .format("jdbc")
                .options(jdbcOptions)
                .load()
                .as[UniwareShippingPackage]

        log.info("Completed readUniwareJDBC for serverName: " + serverName + ", count: " + uniwareShippingPackageAddressDataset.count +
                " in " + ((System.currentTimeMillis() - start)/1000) + " sec")

        uniwareShippingPackageAddressDataset
    }



}
