package org.yy.SparkDataType.struct

import org.apache.spark.sql.functions.{array_position, element_at, from_json, to_json, transform}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * desc: struct增加kv
 * 不方便 已废弃
 */
object addKV1 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName(this.getClass.getName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        val sc = spark.sparkContext
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val df1 = Seq(
            "{'country':'usa','peopleNum':'1'}"
            , "{'country':'china','peopleNum':'14'}"
        ).toDF()

        val st = StructType(
            StructField("country", StringType)
              :: StructField("peopleNum", StringType)
              :: Nil
        )
        // 构造struct 生成struct
        val df2 = df1.select(from_json('value, st).as("c1"))
        df2.show()
        /*
        +-----------+
        |         c1|
        +-----------+
        |   {usa, 1}|
        |{china, 14}|
        +-----------+
         */
        df2.printSchema()
        /*
        root
         |-- c1: struct (nullable = true)
         |    |-- country: string (nullable = true)
         |    |-- peopleNum: string (nullable = true)
         */


        val df3 = Seq(
            ("china", "beijing")
        ).toDF("country", "city")

        val df4 = df2.join(df3,df2("c1.country") === df3("country"),"left")
          // 把city写入 c1这个struct中
          // transform只能应用于 array类型. 所以这里套一层 后面再取出来
          // k太多 这个方法不适用
          .selectExpr(""" transform(array(c1),x -> named_struct("country",x.country,"peopleNum",x.peopleNum,"cityname",city)) as arr""")

        df4.show()
        df4.printSchema()
        /*
        +--------------------+
        |                 arr|
        +--------------------+
        |    [{usa, 1, null}]|
        |[{china, 14, beij...|
        +--------------------+

        root
         |-- arr: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- country: string (nullable = true)
         |    |    |-- peopleNum: string (nullable = true)
         |    |    |-- cityname: string (nullable = true)
         */

        val df5 = df4.select(
            element_at('arr, 1).as("struct_new")
        )
          df5.show(false)
        /*
        +--------------------+
        |          struct_new|
        +--------------------+
        |      {usa, 1, null}|
        |{china, 14, beijing}|
        +--------------------+
         */

        df5.select(to_json('struct_new)).show(false)
        /*
        +---------------------------------------------------------+
        |to_json(struct_new)                                      |
        +---------------------------------------------------------+
        |{"country":"usa","peopleNum":"1"}                        |
        |{"country":"china","peopleNum":"14","cityname":"beijing"}|
        +---------------------------------------------------------+
         */


        spark.stop()
    }

}
