package org.yy.udf.json.jsonObj.delete

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.util.Try


/**
 *  通过jsonpath定位并 删除jsonObj中的某个key
 *  注意:
 *      如果我们给的jsonPath没有匹配的数据 则返回原始数据(注意 这里是解析后重新写入的 所以之前的换行 空格可能都会没了)
 *
 */
object DeleteK {

    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    val delete_k = udf(
        (js: String, jspath: String) => {
            val context = JsonPath.using(configuration).parse(js)
            Try(context.delete(jspath).jsonString()).getOrElse(context.jsonString())
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
        spark.udf.register("delete_k",delete_k)

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
              |            "k1":"v99",
              |            "priority":"2"
              |        }
              |    ],
              |    "key3":234
              |}
              |""".stripMargin


        // 这里含义是: key2对应数组内所有元素中 没有recordid的元素的key=priority的kv对给删除掉
        spark.sql(
            s"""
                    select
                        delete_k('${js}',"$$.key2[*][?(!(@.recordid))].priority") as col1
                    """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v99"
                }
            ],
            "key3":234
        }
         */


        // 如果我们给的jsonPath没有匹配的数据 则返回原始数据(注意 这里是解析后重新写入的 所以之前的换行 空格可能都会没了)
        spark.sql(
            s"""
            select
                delete_k('${js}',"$$.key2[*][?(!(@.recordid))].ssss") as col2
            """)
          .show(false)
        // {"key1":"value1","key2":[{"k1":"v1","priority":"0"},{"recordid":"xxxxxx","priority":"1"},{"k1":"v99","priority":"2"}],"key3":234}


        // 如果数组中部分对象匹配 则只处理该部分对象 其余保持不变
        val js2 =
            """
              |{
              |    "key1":"value1",
              |    "key2":[
              |        {
              |            "k1":"v1",
              |            "priority999":"0"
              |        },
              |        {
              |            "recordid":"xxxxxx",
              |            "priority":"1"
              |        },
              |        {
              |            "k1":"v99",
              |            "priority":"2"
              |        }
              |    ],
              |    "key3":234
              |}
              |""".stripMargin
        spark.sql(
            s"""
                          select
                              delete_k('${js2}',"$$.key2[*][?(!(@.recordid))].priority") as col3
                          """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority999":"0"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v99"
                }
            ],
            "key3":234
        }
         */



        spark.stop()
    }

}
