package org.yy.udf.json.JsonArrAdd

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._

/**
 * jsonarr 中添加 value 注意:没有key
 *      value 支持基础数据类型
 *      value 支持sql中的array类型
 * 注意:
 *      jsonArray中只能新增 无法更新,不存在更新的概念
 */
object AddVviaJsonPath {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    val put_v_via_jsonpath = udf(
        (js:String,jspath:String,v:Any) => {
            if (!v.isInstanceOf[Seq[Any]]) {
                JsonPath.using(configuration).parse(js).add(jspath,v).jsonString()
            }else {
                // 处理数组
                JsonPath.using(configuration).parse(js).add(jspath, v.asInstanceOf[Seq[Any]].asJava).jsonString()
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
        spark.udf.register("put_v_via_jsonpath",put_v_via_jsonpath)


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


        // jsonarray 中新增 value; value支持常见的基础数据类型
        spark.sql(
            s"""
        select
            put_v_via_jsonpath('${js}',"$$.key2", 99.88) as col2
        """)
          .show(false)
        /*
       {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2"
                },
                99.88
            ],
            "key3":234
        }
         */

        // jsonarray 中新增 value; value可以是数组
        spark.sql(
            s"""
            select
                put_v_via_jsonpath('${js}',"$$.key2", array('a','b')) as col3
            """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0"
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2"
                },
                [
                    "a",
                    "b"
                ]
            ],
            "key3":234
        }
         */

        // jsonarray 中新增 value; v可以使数组; $ 表示从跟下的jsonarray添加元素
        val js1 =
            """
              |[
              | 1 , 9.99
              |]
              |""".stripMargin
        spark.sql(
            s"""
            select
                put_v_via_jsonpath('${js1}',"$$",  array(3.33,10.10)) as col4
            """)
          .show(false)
        /*
        [1,9.99,[3.33,10.1]]
         */

// 报错: 该udf不支持jsonpath解析后是jsonObj 然后去只添加value
//        val js2 =
//            """
//              |{"k1":"v1"}
//              |""".stripMargin
//        spark.sql(
//            s"""
//            select
//                put_v_via_jsonpath('${js2}',"$$",  array(1,2,3)) as col5
//            """)
//          .show(false)

        spark.stop()
    }

}
