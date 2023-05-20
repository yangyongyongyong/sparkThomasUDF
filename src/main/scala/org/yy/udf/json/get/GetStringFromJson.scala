package org.yy.udf.json.get

import com.alibaba.fastjson2.{JSONPath, JSONReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf}

import scala.util.Try

object GetStringFromJson {
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
        spark.udf.register("onestr_from_json_via_path",oneonestr_from_json_via_path)

        spark.sql("""select onestr_from_json_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des[0]') as v1 """)
          .withColumn("typename", expr("typeof(v1)"))
          .show(false)
        /*
        +------+--------+
        |v1     |typename|
        +------+--------+
        |i am 0|string  |
        +------+--------+
         */



        spark.sql("""select onestr_from_json_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des[1]') as v2 """)
          .withColumn("typename", expr("typeof(v2)"))
          .show(false)
        /*
        +----+--------+
        |v2   |typename|
        +----+--------+
        |null|string  |
        +----+--------+
         */

        spark.sql("""select onestr_from_json_via_path(null,'$[?(@.id = 0)].des[1]') as v3 """)
          .withColumn("typename", expr("typeof(v3)"))
          .show(false)
        /*
        +----+--------+
        |v3   |typename|
        +----+--------+
        |null|string  |
        +----+--------+
         */


        // 值筛选 注意语法  @.id==0 和  @.id=="0" 是不一样的; = 和 == 都行,但是按照文档走,请使用 == ;
        // @.id == 0 或者 @.id==0 都行,空格不影响结果
        spark.sql("""select onestr_from_json_via_path('{"des":"i am 0","id":0}','$[?(@.id == 0)]') as v1_1 """)
          .show()
        /*
        +--------------------+
        |                v1_1|
        +--------------------+
        |{"des":"i am 0","...|
        +--------------------+
         */

        spark.sql("""select onestr_from_json_via_path('{"des":"i am 0","id":0}','$[?(@.id=="0")]') as v1_2 """)
          .show()
        /*
        +----+
        |v1_2|
        +----+
        |null|
        +----+
         */


        spark.stop()
    }

}
