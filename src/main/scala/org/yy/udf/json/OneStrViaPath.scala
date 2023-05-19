package org.yy.udf.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf}
import com.alibaba.fastjson2.{JSONPath, JSONReader}

import scala.util.Try

object OneStrViaPath {
    /*
    输入:
        jsonArray/jsonObject都可 String类型
        jsonpath String类型
    输出:
        类型: String
    使用方法:
        spark.udf.register("onestr_from_json_via_path",oneonestr_from_json_via_path)
    注意: jsonpath解析不出来 或者列值为null 都返回 null
     */
    val oneonestr_from_json_via_path = udf(
        (js: String, jsPath: String) => {
            if (js == null || jsPath == null) {
                null
            } else {
                Try {
                    JSONPath.of(jsPath).extract(JSONReader.of(js)).toString()
                }
            }.getOrElse(null)
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
        spark.udf.register("onestr_from_json_via_path",oneonestr_from_json_via_path)

        spark.sql("""select onestr_from_json_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des[0]') as v """)
          .withColumn("typename", expr("typeof(v)"))
          .show(false)
        /*
        +------+--------+
        |v     |typename|
        +------+--------+
        |i am 0|string  |
        +------+--------+
         */


        spark.sql("""select onestr_from_json_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des[1]') as v """)
          .withColumn("typename", expr("typeof(v)"))
          .show(false)
        /*
        +----+--------+
        |v   |typename|
        +----+--------+
        |null|string  |
        +----+--------+
         */

        spark.sql("""select onestr_from_json_via_path(null,'$[?(@.id = 0)].des[1]') as v """)
          .withColumn("typename", expr("typeof(v)"))
          .show(false)
        /*
        +----+--------+
        |v   |typename|
        +----+--------+
        |null|string  |
        +----+--------+
         */


        spark.stop()
    }

}
