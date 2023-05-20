import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import scala.collection.JavaConverters._

object jaywayJsonPath_read {


    def main(args: Array[String]): Unit = {
        val js =
            """
              |{
              |  "key1" : "value1",
              |  "key2" : [
              |  {"k1":"v1","priority":"0"},
              |  {"recordid":"xxxxxx","priority":"1"},
              |  {"k1":"v2","priority":"2"}
              |  ],
              |  "key3" : 234
              |}
              |""".stripMargin
        val json = Configuration.defaultConfiguration().jsonProvider().parse(js)
        // 这里必须指明类型(包不知道你输出类型是什么 需要你手动指定),否则报错:  java.lang.String cannot be cast to scala.runtime.Nothing
        val res:String = JsonPath.read(json,"$.key1") // value1
        val res1:java.util.List[String] = JsonPath.read(json,"$.key2[*].k1") // [v1,v2]
        val res2:java.util.List[String] = JsonPath.read(json,"$.key2[*].kkkkk") // []

        println(res)
        println(res1)
        println(res2)

    }

}
