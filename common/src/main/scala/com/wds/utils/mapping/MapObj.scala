package com.wds.utils.mapping

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object MapObj {
  val mapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule)

  def map2Obj[T](map: util.Map[String, AnyRef], clazz: Class[T]): T =
    mapper.convertValue(map, clazz)

  def obj2Map[T](t: T): util.Map[String, AnyRef] =
    mapper.convertValue(t, classOf[util.Map[String, AnyRef]])
}