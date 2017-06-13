package com.aiso.spark.util

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{JValue, _}

object JsonUtils {

  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  }
  implicit val formats2 = Serialization.formats(ShortTypeHints(List()))


  def jValue2JsonString(obj: JValue): String = {
    compact(render(obj))
  }

  def jValue2PrettyJsonString(obj: JValue): String = {
    pretty(render(obj))
  }

  ///////////////////////////////////////////////////////////////////////////////////////////
  def jsonObjStr2Map(json: String): Map[String, Any] = {
    org.json4s.jackson.JsonMethods.parse(json, useBigDecimalForDouble = true).values.asInstanceOf[Map[String, Any]]
  }

  def jsonStr2ObjectDemo(): Unit = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats // Brings in default date formats etc.
    case class Child(name: String, age: Int, birthdate: Option[java.util.Date])
    case class Address(street: String, city: String)
    case class Person(name: String, address: Address, children: List[Child])
    val json = parse(
      """
         { "name": "joe",
           "address": {
             "street": "Bulevard",
             "city": "Helsinki"
           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
      """)
    println(json.extract[Person].name)

  }

  //////////////////////////////////////spark json///////////////////////////////////////////////
  def sparkReadFromJsonFile2DF(sc: SparkContext, jsonFilePath: String): DataFrame = {
    val sqlContext = new SQLContext(sc);
    val df = sqlContext.read.format("json").load(jsonFilePath);
    df
  }

  def sparkReadFromJsonFileFilter2DF(sc: SparkContext, jsonFilePath: String): DataFrame = {
    val sqlContext = new SQLContext(sc);
    val df = sqlContext.jsonFile(jsonFilePath).registerTempTable("jsonTable")
    val jsonQuery = sqlContext.sql("select * from jsonTable")
    jsonQuery.printSchema
    jsonQuery.queryExecution
    jsonQuery
  }


  ///////////////////////////////////////////////////////////test////////////////////////////////////////////////////

  def main(args: Array[String]): Unit = {
    tmp01()
  }

  def tmp01() = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    //    parse("""{"numbers":[1,2,3,4]}""").values
    //    parse("""{"name":"Toy","price":35.35}""", useBigDecimalForDouble = true)

    val json2 = parse(
      """
         {
           "name": "joe",
           "addresses": {
             "address1": {
               "street": "Bulevard",
               "city": "Helsinki"
             },
             "address2": {
               "street": "Soho",
               "city": "London"
             }
           }
         }""")

    val json = parse(
      """
         { "name": "joe",
           "children": [
             {
               "name": "Mary",
               "age": 5
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
      """)

    for (JArray(child) <- json) {
      println(child)
    }

    for {
      JObject(child) <- json
      JField("age", JInt(age)) <- child
    } yield age

    for {
      JObject(child) <- json
      JField("name", JString(name)) <- child
      JField("age", JInt(age)) <- child
      if age > 4
    } yield (name, age)

  }


  def tmp02() = {
    //    val json =
    //      ("person" ->
    //        ("name" -> "Joe") ~
    //          ("age" -> 35) ~
    //          ("spouse" ->
    //            ("person" ->
    //              ("name" -> "Marilyn") ~
    //                ("age" -> 33)
    //              )
    //            )
    //        )

  }


}
