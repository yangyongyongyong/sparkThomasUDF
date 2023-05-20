package org.yy.udf.json.jsonObj.addOrUpdateKV

import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._


/**
 * desc:
 *      向json中添加kv对 使用两个jsonpath(修改位置的jsonpath 和 jsonpath提取值作为新的value)修改json
 *      jsonobj(不支持jsonArray) 添加kv对:
 *          value 支持基础数据类型 不是用户指定具体值 而是通过用户提供的jsonPath从原始数据中提取
 *          value 支持sql中的array类型
 * 方式:
 *      k使用jsonpath定位 value也是通过jsonpath提取原始数据中的某个值(找不到会报错)
 *      kv不在则新增 存在则更新
 * 使用场景:
 *      比如数组是人类,业务需求是: 如果某个具体的人没有性别,就从同层级其他人里随便捞一个性别填充进去
 * 注意:
 *      如果value的jsonPath 无法从原始数据中提取到值  则会报错
 *      value的jsonPath 如果解析出来有多个,则自动选择第一个作为结果
 */
object PutKV2 {

    private def configuration = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .build()

    // 输入：json字符串，jsonpath，值
    val change_value_via_2jsonpath = udf(
        // (原始json, 需要新增的key,oldjsonpath限定新增的key的位置,newjsonpath获取需要填充的值)
        (js: String, k:String,oldJsPath: String, newJsPath: String) => {
            if (js != null) {
                val json = Configuration.defaultConfiguration().jsonProvider().parse(js)
                // 这里必须指明类型(包不知道你输出类型是什么 需要你手动指定),否则报错:  java.lang.String cannot be cast to scala.runtime.Nothing
                // 这里语法表示 key2下所有value中, 包含recordid这个key的对象 的recordid组成的数组
                val new_v: java.util.List[Any] = JsonPath.read(json, newJsPath)
                JsonPath.using(configuration).parse(js).put(oldJsPath, k, new_v.asScala.head).jsonString()
            } else {
                null
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

        /**
         *  需要put的key对应的path '$.key2[*][?(!(@.recordid))]'  找到key2下的所有value中, 不包含recordid这个jsonObject位置
         *  需要put的value对应的path '$.key2[*][?(@.recordid)].recordid' 找到key2下的所有value中, 包含recordid这个jsonObject位置,然后取出recordid的值中的第一个
         */
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

        spark.sql(
            s"""
               |select
               | change_value_via_2jsonpath(null,'recordid','$$.key2[*][?(!(@.recordid))]','$$.key2[*][?(@.recordid)].recordid') as col2
               |""".stripMargin)
          .show(false)
        /*
        +----+
        |col2|
        +----+
        |null|
        +----+
         */


//        // 报错 因为value的jsonpath必须有值 否则报错
//        spark.sql(
//            s"""
//               |select
//               | change_value_via_2jsonpath('${js}','recordid','$$.key2[*][?(!(@.recordid))]','$$.key2[*][?(@.recordid1)].recordid1') as col1111
//               |""".stripMargin)
//          .show(false)

        spark.stop()
    }


}
