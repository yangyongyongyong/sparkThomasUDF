package org.yy.udf.json
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf, variance}
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider

import scala.util.Try
import scala.collection.JavaConverters._
import scala.collection.Seq


/*
    desc: 向json中添加kv对 使用两个jsonpath(修改位置的jsonpath 和 jsonpath提取值作为新的value)修改json
        方式:
            1 k使用jsonpath定位 value自己指定
            2 k使用jsonpath定位 value也是通过jsonpath提取原始数据中的某个值(找不到会报错)
    使用场景:
        比如数组是人类,业务需求是: 如果某个具体的人没有性别,就从同层级其他人里随便捞一个性别填充进去
 */
object ModVViaJsonPath2 {

    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build()

    // 输入：json字符串，jsonpath，值
    val change_value_via_2jsonpath = udf(
        // (原始json, 需要新增的key,oldjsonpath限定新增的key的位置,newjsonpath获取需要填充的值)
        (js: String, k:String,oldJsPath: String, newJsPath: String) => {
            val json = Configuration.defaultConfiguration().jsonProvider().parse(js)
            // 这里必须指明类型(包不知道你输出类型是什么 需要你手动指定),否则报错:  java.lang.String cannot be cast to scala.runtime.Nothing
            // 这里语法表示 key2下所有value中, 包含recordid这个key的对象 的recordid组成的数组
            val new_v: java.util.List[Any] = JsonPath.read(json, newJsPath)
            JsonPath.using(configuration).parse(js)
              .put(oldJsPath, k, new_v.asScala.head).jsonString()
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

        spark.udf.register("change_value_via_2jsonpath",change_value_via_2jsonpath)

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

        spark.sql(
            s"""
              |select
              | change_value_via_2jsonpath('${js}','recordid','$$.key2[*][?(!(@.recordid))]','$$.key2[*][?(@.recordid)].recordid') as col1
              |""".stripMargin)
          .show(false)
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



        spark.stop()
    }


}
