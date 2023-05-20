package org.yy.udf.json.jsonObj.addOrUpdateKV

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._

/**
 * jsonobj(不支持jsonArray) 添加kv对
 *      value 支持基础数据类型 用户指定具体值
 *      value 支持sql中的array类型
 * 特别注意:
 *      jsonPath解析后 不要是jsonarray,该函数只针对jsonObj  如果是jsonArray则会报错
 *      put的含义: 如果k存在则更新value,如果不存在则新增kv
 */
object PutKV1 {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    val put_kv_via_jsonpath = udf(
        (js:String,jspath:String,k:String,v:Any) => {
            if (!v.isInstanceOf[Seq[Any]]) {
                JsonPath.using(configuration).parse(js).put(jspath, k, v).jsonString()
            }else {
                // value是 sql中数组类型
                JsonPath.using(configuration).parse(js).put(jspath,k, v.asInstanceOf[Seq[Any]].asJava).jsonString()
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
        spark.udf.register("put_kv_via_jsonpath",put_kv_via_jsonpath)


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

        // jsonobj 中新增 kv 对; value支持其他基本数据类型
        spark.sql(s"""
    select
        put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", "fakeRecordid") as col1
        """)
          .show(false)
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

        // jsonobj 中新增 kv 对; v支持常见的基础数据类型
        spark.sql(
            s"""
        select
            put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", 99.88) as col2
        """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0",
                    "recordid":99.88
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2",
                    "recordid":99.88
                }
            ],
            "key3":234
        }
         */


        // jsonobj 中新增 kv 对; v可以是数组
        spark.sql(
            s"""
            select
                put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", array('a','b')) as col3
            """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":"0",
                    "recordid":[
                        "a",
                        "b"
                    ]
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":"2",
                    "recordid":[
                        "a",
                        "b"
                    ]
                }
            ],
            "key3":234
        }
         */

        // jsonobj 中新增 kv 对; 如果k存在则更新value,如果不存在则新增kv
        spark.sql(
            s"""
            select
                put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "priority", 999) as col4
            """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                {
                    "k1":"v1",
                    "priority":999
                },
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                {
                    "k1":"v1",
                    "priority":999
                }
            ],
            "key3":234
        }
         */


// 报错 因为jsonpath解析后的数组中的元素是jsonarray  不支持 put kv
//        spark.sql(
//            s"""
//            select
//                put_kv_via_jsonpath('${js}',"$$.key2", "recordid", array(3.33,10.10)) as col4
//            """)
//          .show(false)


        spark.stop()
    }

}
