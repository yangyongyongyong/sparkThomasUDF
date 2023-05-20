package org.yy.udf.json

import org.apache.spark.sql.SparkSession
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConverters._

/**
 * json字符串中 添加kv对
 *      支持基础数据类型
 *      支持sql中的array类型
 */
object PutKVviaJsonPath {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    val put_kv_via_jsonpath = udf(
        (js:String,jspath:String,k:String,v:Any) => {
            if (!v.isInstanceOf[Seq[Any]]) {
                JsonPath.using(configuration).parse(js).put(jspath, k, v).jsonString()
            }else {
                JsonPath.using(configuration).parse(js).set(jspath, v.asInstanceOf[Seq[Any]].asJava).jsonString()
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

        spark.sql(
            s"""
        select
            put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", 99.88) as col1
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


        spark.sql(
            s"""
            select
                put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", array('a','b')) as col1
            """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                [
                    "a",
                    "b"
                ],
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                [
                    "a",
                    "b"
                ]
            ],
            "key3":234
        }
         */


        spark.sql(
            s"""
            select
                put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", array(3.33,10.10)) as col1
            """)
          .show(false)
        /*
        {
            "key1":"value1",
            "key2":[
                [
                    3.33,
                    10.1
                ],
                {
                    "recordid":"xxxxxx",
                    "priority":"1"
                },
                [
                    3.33,
                    10.1
                ]
            ],
            "key3":234
        }
         */


        spark.stop()
    }

}
