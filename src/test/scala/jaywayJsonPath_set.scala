import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider

// set 修改已有key的value
object jaywayJsonPath_set {

    private def  configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build()


    def main(args: Array[String]): Unit = {
        val js =
            """
              |{
              |  "key1" : "value1",
              |  "key2" : {
              |    "key21" : 123,
              |    "key22" : true,
              |    "key23" : [ "alpha", "beta", "gamma"],
              |    "key24" : {
              |      "key241" : 234.123,
              |      "key242" : "value242"
              |    }
              |  },
              |  "key3" : 234
              |}
              |""".stripMargin

        val  updatedJson = JsonPath.using(configuration).parse(js).set("$.key2.key21", "newValue").jsonString()

        System.out.println(updatedJson)
    }

}
