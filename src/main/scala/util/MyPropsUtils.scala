package util

import java.util.ResourceBundle

object MyPropsUtils {

  //ResourceBundle.getBundle（读取properties的资源文件，从工程根目录下找）
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String = {
    //getString : 从资源包或其父级获取给定键(key)的字符串,返回给定键的字符串
    bundle.getString(propsKey)
  }

  /*
  def main(args: Array[String]): Unit = {
    println(MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
  }
  */
}
