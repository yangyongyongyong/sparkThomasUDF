package org.yy.udf.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf, variance}
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import scala.util.Try

/*
desc: 使用jsonpath修改已有key的value
 */
object ChangeValueViaJsonPath {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build()

    // 输入：json字符串，jsonpath，值, 返回值类型 int double float
    // 暂不支持array[T]
    val change_value_via_jsonpath = udf(
        (js:String,jspath:String,value:String,typename:String) => {
            // JsonPath.using(configuration).parse(js).set("$.key2.key21", "newValue").jsonString()
            typename.toLowerCase match {
                case "str" | "string" => JsonPath.using(configuration).parse(js).set(jspath, value).jsonString()
                case "int" | "integer" => JsonPath.using(configuration).parse(js).set(jspath, value.toInt).jsonString()
                case "long" | "bigint" => JsonPath.using(configuration).parse(js).set(jspath, value.toLong).jsonString()
                case "double" => JsonPath.using(configuration).parse(js).set(jspath, value.toDouble).jsonString()
                case "float" => JsonPath.using(configuration).parse(js).set(jspath, value.toFloat).jsonString()
                // 默认按照string处理
                case _ => throw new Exception("udf not support type")
            }
        }
    )


    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getName)
            .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._



        val js1 =
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

        spark.udf.register("change_value_via_jsonpath", change_value_via_jsonpath)

        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', 'newValue','str')  """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":{
                "key21":"newValue",
                "key22":true,
                "key23":[
                    "alpha",
                    "beta",
                    "gamma"
                ],
                "key24":{
                    "key241":234.123,
                    "key242":"value242"
                }
            },
            "key3":234
        }
         */

        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', '99999','int')  """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":{
                "key21":99999,
                "key22":true,
                "key23":[
                    "alpha",
                    "beta",
                    "gamma"
                ],
                "key24":{
                    "key241":234.123,
                    "key242":"value242"
                }
            },
            "key3":234
        }
         */


        spark.stop()
    }

}
