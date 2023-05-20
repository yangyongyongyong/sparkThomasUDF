package org.yy.udf.json

import org.apache.spark.sql.SparkSession
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.functions.udf


object PutKVviaJsonPath {
    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build();

    val put_kv_via_jsonpath = udf(
        (js:String,jspath:String,k:String,v:Any) => {
            JsonPath.using(configuration).parse(js)
              .put(jspath, k, v).jsonString()
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


        // 报错 不支持数组
        // 注意 这里的第三个参数是json中的数组
        spark.sql(
            s"""
            select
                put_kv_via_jsonpath('${js}',"$$.key2[*][?(!(@.recordid))]", "recordid", array('a','b')) as col1
            """)
          .show(false)


        spark.stop()
    }

}
