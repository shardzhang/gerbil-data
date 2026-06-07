package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

/**
 * @author Shard Zhang
 * @date 2021/3/18 09:37
 * @note 在Spark shell控制台, 彩色打印
 *       https://stackoverflow.com/questions/287871/how-to-print-colored-text-to-the-terminal
 */

case object PrintStyle {
  val HEADER = "\033[95m"
  val OKBLUE = "\033[94m"
  val OKCYAN = "\033[96m"
  val OKGREEN = "\033[92m"
  val WARNING = "\033[93m"
  val FAIL = "\033[91m"
  val ENDC = "\033[0m"
  val BOLD = "\033[1m"
  val UNDERLINE = "\033[4m"
}

object LogUtils {

  def setLogLevel(): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN) // 全局根日志器
    // Logger.getLogger("org").setLevel(Level.WARN)
    // Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    // Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Logger.getLogger("akka").setLevel(Level.WARN)
    // Logger.getLogger("io.netty").setLevel(Level.WARN)
  }

  def header(str: String): String = {
    PrintStyle.HEADER + str + PrintStyle.HEADER
  }

  def okblue(str: String): String = {
    PrintStyle.OKBLUE + str + PrintStyle.OKBLUE
  }

  def okcyan(str: String): String = {
    PrintStyle.OKCYAN + str + PrintStyle.OKCYAN
  }

  def bold(str: String): String = {
    PrintStyle.BOLD + str + PrintStyle.BOLD
  }

  def red(str: String): String = {
    "\u001B[31m " + str + "\u001B[0m"
  }

  // Green
  def green(str: String): String = {
    "\u001B[32m " + str + "\u001B[0m"
  }

  // Yellow
  def yellow(str: String): String = {
    "\u001B[33m " + str + "\u001B[0m"
  }

  // Cyan
  def cyan(str: String): String = {
    "\u001B[36m " + str + "\u001B[0m"
  }

  // Red
  def red_println(str: String): Unit = {
    //System.out.println("\u001B[31m>>>>\u001B[0m\u001B[31m" + str + "\u001B[0m")
    System.out.println(red(s">>> Transformed | ${str}"))
  }

  // Green
  def green_println(str: String): Unit = {
    System.out.println(green(s">>> Transformed | ${str}"))
  }

  // Yellow
  def yellow_println(str: String): Unit = {
    System.out.println(yellow(s">>> Transformed | ${str}"))
  }

  // Bold
  def bold_println(str: String): Unit = {
    System.out.println(bold(s">>> Transformed | ${str}"))
  }

  def printFieldInfo(dfData: DataFrame, colName: String, limit: Int): Unit = {
    dfData
      .select(colName)
      .rdd
      .map(r => r.getString(0))
      .take(limit)
      .foreach(r => green_println(r))
  }

  def main(args: Array[String]): Unit = {
    green_println(red("success!!!"))
    green_println(green("success!!!"))
    PrintStyle.getClass.getDeclaredField("BOLD")
  }
}
