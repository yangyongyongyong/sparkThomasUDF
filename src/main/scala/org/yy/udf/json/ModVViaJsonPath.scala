package org.yy.udf.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf, variance}
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import scala.util.Try
import scala.collection.JavaConverters._

/*
desc:
    使用jsonpath找到k 修改json中已有key的value value值是手动传入的
    value 支持基础数据类型
    value 支持sql中的array类型
 */
object ModVViaJsonPath {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build()

    // 输入：json字符串，jsonpath，值
    val change_value_via_jsonpath = udf(
        (js: String, jspath: String, v: Any) => {
            if (!v.isInstanceOf[Seq[Any]]) {
                // JsonPath.using(configuration).parse(js).set("$.key2.key21", "newValue").jsonString()
                JsonPath.using(configuration).parse(js).set(jspath, v).jsonString()
            } else {
                JsonPath.using(configuration).parse(js).set(jspath, v.asInstanceOf[Seq[Any]].asJava).jsonString()
            }
        }
    )


    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName(this.getClass.getName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', 'newValue')  as col1""")
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

        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', 99999) as col1 """)
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

        // 支持数组
        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', array("aaa","bb") ) as col1 """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":{
                "key21":[
                    "aaa",
                    "bb"
                ],
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

        // 支持数组
        spark.sql(s""" select  change_value_via_jsonpath('${js1}','$$.key2.key21', array(33,789) ) as col1 """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":{
                "key21":[
                    33,
                    789
                ],
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
