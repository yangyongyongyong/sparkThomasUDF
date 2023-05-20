package org.yy.udf.json

import com.alibaba.fastjson2.{JSON, JSONArray}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, lit, udf}

/**
 * desc: 合并 array[jsonobj] 为 jsonarray
 * input: array( '{"b":"v2"}' , '{"a":"v1"}' )        sql的array[String]类型
 * output: [{"b":"v2"},{"a":"v1"}]                    string类型
 * 注意:
 *      如果输入的数组为空 则返回null . 不是字符串"null"
 */
object MergeJsonObjList2JsonArr {

    // collect_set(jsonobj列) 之后调用该函数实现 多个jsonobj 合并为 jsonarray
    val merge_jsonobj_list_2_jsonarr = udf(
        (list: Seq[String], typename: String) => {
            if (list != null) {
                val array = new JSONArray()
                typename.toLowerCase match {
                    case "jsonobj" | "jsonobject" | "obj" | "object" => list.filter(_.nonEmpty).foreach(s => array.add(JSON.parseObject(s)))
                    case "jsonarr" | "jsonarray" | "arr" | "array" => list.filter(_.nonEmpty).foreach(s => array.add(JSON.parseArray(s)))
                }
                if (array.size() > 0) array.toString() else null
            } else {
                null
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

        spark.udf.register("merge_jsonobj_list_2_jsonarr", merge_jsonobj_list_2_jsonarr)

        val df = Seq(
            ("{'a':'v1'}", 2)
            , ("{'b':'v2'}", 2)
            , (null, 1)
        ).toDF("c1", "c2")


        df.groupBy("c2")
          .agg(merge_jsonobj_list_2_jsonarr(collect_set($"c1"), lit("obj")).as("c1"))
          .show(false)
        /*
        +---+-----------------------+
        |c2 |c1                     |
        +---+-----------------------+
        |2  |[{"b":"v2"},{"a":"v1"}]|
        |1  |null                   |
        +---+-----------------------+
         */

        val df1 = Seq(
            ("[{'k1':'v1'}]", 2)
            , ("[{'k3':'v3','k4':'v4'}]", 2)
            , (null, 1)
        ).toDF("c1", "c2")
        df1.groupBy("c2")
          .agg(merge_jsonobj_list_2_jsonarr(collect_set($"c1"), lit("arr")).as("c1"))
          .show(false)
        /*
        +---+---------------------------------------+
        |c2 |c1                                     |
        +---+---------------------------------------+
        |2  |[[{"k1":"v1"}],[{"k3":"v3","k4":"v4"}]]|
        |1  |null                                   |
        +---+---------------------------------------+
         */

        df1.groupBy("c2")
          .agg(merge_jsonobj_list_2_jsonarr(collect_set($"c1"), lit("arr")).as("c1"))
          .filter('c1.isNull)
          .show(false)
        /*
        +---+----+
        |c2 |c1  |
        +---+----+
        |1  |null|
        +---+----+
         */


        spark.stop()

    }

}
