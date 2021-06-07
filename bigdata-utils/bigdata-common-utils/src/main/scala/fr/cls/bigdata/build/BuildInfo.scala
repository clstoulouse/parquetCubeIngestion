package fr.cls.bigdata.build

import java.util.Properties

import scala.reflect._

/**
  * @param version Maven version of a build
  */
case class BuildInfo(version: String)

object BuildInfo {
  def apply(classLoader: ClassLoader): BuildInfo = {
    val properties = new Properties()
    properties.load(classLoader.getResourceAsStream("git.properties"))
    BuildInfo(properties.getProperty("git.build.version"))
  }

  def apply(clazz: Class[_]): BuildInfo = BuildInfo(clazz.getClassLoader)

  def apply[T: ClassTag]: BuildInfo = BuildInfo(classTag[T].runtimeClass)
}


