package org.yy.udf.json.get

import com.alibaba.fastjson2.{JSON, JSONPath, JSONReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf}

import scala.util.Try


object JsonArrayViaPath {

    /*
    输入:
        jsonArray/jsonObject都可 String类型
        jsonpath String类型
    输出:
        类型: array[String]
    使用方法:
      spark.udf.register("arraystr_from_json_via_path",arraystr_from_json_via_path)
    注意: jsonpath解析不出来 或者列值为null 都返回空数组
     */
    val arraystr_from_json_via_path = udf(
        (js: String, jsPath: String) => {
            if (js == null || jsPath == null) {
                Array[String]()
            } else {
                Try {
                    // fastjson2 使用jsonpath解析
                    val jsarr = JSON.parseArray(JSONPath.of(jsPath).extract(JSONReader.of(js)).toString());
                    // jsarr 转 Array[String]
                    jsarr.toArray().map(_.toString)
                }
            }.getOrElse(Array[String]())
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

        spark.udf.register("arraystr_from_json_via_path", arraystr_from_json_via_path)
        spark.sql("select arraystr_from_json_via_path('[{\"k1\":11,\"k2\":22},{\"k1\":44,\"k2\":55}]','$[*].k1') as v")
          .withColumn("typename", expr("typeof(v)"))
          .withColumn("sz", expr("size(v)"))
          .show(false)
        /*
        +--------+-------------+---+
        |v       |typename     |sz |
        +--------+-------------+---+
        |[11, 44]|array<string>|2  |
        +--------+-------------+---+
         */

        spark.sql("select arraystr_from_json_via_path(null,'$[*].k1') as v")
          .withColumn("typename", expr("typeof(v)"))
          .withColumn("sz", expr("size(v)"))
          .show(false)
        /*
        +---+-------------+---+
        |v  |typename     |sz |
        +---+-------------+---+
        |[] |array<string>|0  |
        +---+-------------+---+
         */

        spark.sql("select arraystr_from_json_via_path('{\"k1\":[11,22,33],\"k2\":22}','$.k1') as v ")
          .withColumn("typename", expr("typeof(v)"))
          .withColumn("sz", expr("size(v)"))
          .show(false)
        /*
        +------------+-------------+---+
        |v           |typename     |sz |
        +------------+-------------+---+
        |[11, 22, 33]|array<string>|3  |
        +------------+-------------+---+
         */

        spark.sql("select arraystr_from_json_via_path('{\"k1\":[11,22,33],\"k2\":22}','$.uu') as v ")
          .withColumn("typename", expr("typeof(v)"))
          .withColumn("sz", expr("size(v)"))
          .show(false)
        /*
        +---+-------------+---+
        |v  |typename     |sz |
        +---+-------------+---+
        |[] |array<string>|0  |
        +---+-------------+---+
         */

        // 查找jsonobj的 k1下的所有数组中 y7=b3的jsonobj的m1组成的数组
        spark.sql("""select arraystr_from_json_via_path('{"k1":[{"s1":"b1","m1":"vvv"},{"y7":"b3","m1":"vv333"}],"k2":"v2"}','$.k1[*][?(@.y7 = "b3")].m1') as v """)
          .withColumn("typename", expr("typeof(v)"))
          .withColumn("sz", expr("size(v)"))
          .show(false)
        /*
        +-------+-------------+---+
        |v      |typename     |sz |
        +-------+-------------+---+
        |[vv333]|array<string>|1  |
        +-------+-------------+---+
         */


        spark.stop()
    }

}
