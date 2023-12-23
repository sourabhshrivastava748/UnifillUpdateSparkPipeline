package utils

import org.scalatest.funsuite.AnyFunSuite

class UniwareUtilsTest extends AnyFunSuite {

    test("UniwareUtils.getUniwareShippingPackageAddressQuery") {
        val fromInclusiveDate = "2023-11-01 00:00:00"
        val tillExclusiveDate = "2023-11-01 00:01:00"
        println(UniwareUtils.getUniwareShippingPackageAddressQuery (fromInclusiveDate, tillExclusiveDate))
    }

}
