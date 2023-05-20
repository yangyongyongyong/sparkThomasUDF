import com.jayway.jsonpath.{Configuration, JsonPath}

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider

// put 添加kv对
object jaywayJsonPath_put {

    private def  configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    def main(args: Array[String]): Unit = {
        val js =
            """
              |{
              |    "key1":"value1",
              |    "key2":[
              |        {
              |            "k1":"v1",
              |            "priority":"0"
              |        },
              |        {
              |            "recordid":"xxxxxx",
              |            "priority":"1"
              |        },
              |        {
              |            "k1":"v1",
              |            "priority":"2"
              |        }
              |    ],
              |    "key3":234
              |}
              |""".stripMargin

        val updatedJson = JsonPath.using(configuration).parse(js)
          .put("$.key2[*][?(!(@.recordid))]", "recordid", "fakeRecordid").jsonString()

        System.out.println(updatedJson)
        /*
            {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0",
                    "recordid":"fakeRecordid"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2",
                    "recordid":"fakeRecordid"
                }
            ],
            "key3":234
        }
         */
    }

}
