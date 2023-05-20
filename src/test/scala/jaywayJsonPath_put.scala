import com.jayway.jsonpath.{Configuration, JsonPath}

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import scala.collection.JavaConverters._

/*
put 向json中添加kv对
方式:
    1 k使用jsonpath定位 value自己指定
    2 k使用jsonpath定位 value也是通过jsonpath提取原始数据中的某个值
 */
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

        // 对应需求: 数组中如果某些对象没有recordid,则尝试从兄弟对象中复制recordid 否则报错(如果异常情况返回原始json,可能误导用户 所以使用前用户需要人工确保数据符合预期)
        val json = Configuration.defaultConfiguration().jsonProvider().parse(js)
        // 这里必须指明类型(包不知道你输出类型是什么 需要你手动指定),否则报错:  java.lang.String cannot be cast to scala.runtime.Nothing
        // 这里语法表示 key2下所有value中, 包含recordid这个key的对象 的recordid组成的数组
        val new_v: java.util.List[Any] = JsonPath.read(json, "$.key2[*][?(@.recordid)].recordid")
        println(new_v.asScala)
        val updatedJson1 = JsonPath.using(configuration).parse(js)
          .put("$.key2[*][?(!(@.recordid))]", "recordid", new_v.asScala.head).jsonString()
        println(updatedJson1)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0",
                    "recordid":"xxxxxx"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2",
                    "recordid":"xxxxxx"
                }
            ],
            "key3":234
        }
         */
    }

}
