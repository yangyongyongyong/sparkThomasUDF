//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{expr, udf, variance}
//import play.api.libs.json._
//
//import scala.util.Try


object playJson {

    def main(args: Array[String]): Unit = {
        val js =
            """
              |{
              |  "key1" : "value1",
              |  "key2" : {
              |    "key21" : 123,
              |    "key22" : true,
              |    "key23" : [ "alpha", "beta", "gamma"],
              |    "key24" : {
              |      "key241" : 234.123,
              |      "key242" : "value242"
              |    }
              |  },
              |  "key3" : 234
              |}
              |""".stripMargin
        //        val json = Json.parse(js)
        //        val m: JsPath = __ \ 'key24 \ 'key241
        //        val jsonTransformer = (__ \ 'key24 \ 'key241).json.update(JsNumber(456))
        //        val value = json.transform(jsonTransformer)
        //        println(value.get)

    }
}
