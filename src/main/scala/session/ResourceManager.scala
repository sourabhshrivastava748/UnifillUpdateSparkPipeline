package session

import org.apache.log4j.LogManager

import java.io.FileNotFoundException
import java.util.Properties
import scala.io.Source

object ResourceManager {

    val log = LogManager.getLogger(this.getClass.getName)

    def getApplicationProperties(): Properties = {
        val url = getClass.getResource("application.properties")
        val properties: Properties = new Properties()

        if (url != null) {
            val source = Source.fromURL(url)
            properties.load(source.bufferedReader())
        }
        else {
            log.error("Properties file cannot be loaded")
            throw new FileNotFoundException("Properties file cannot be loaded");
        }
        properties
    }

}
