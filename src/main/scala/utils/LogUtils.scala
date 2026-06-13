package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

/**
 * Color printing in Spark shell console
 *       https://stackoverflow.com/questions/287871/how-to-print-colored-text-to-the-terminal
 */

/** ANSI escape codes for terminal color output. */
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

/** Color-printing utilities for Spark-shell console output. */
object LogUtils {

  /** Sets root logger to WARN to suppress verbose Spark logs. */
  def setLogLevel(): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
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

  /** Wraps string in red ANSI codes for terminal output. */
  def red(str: String): String = {
    "\u001B[31m " + str + "\u001B[0m"
  }

  /** Wraps string in green ANSI codes. */
  def green(str: String): String = {
    "\u001B[32m " + str + "\u001B[0m"
  }

  /** Wraps string in yellow ANSI codes. */
  def yellow(str: String): String = {
    "\u001B[33m " + str + "\u001B[0m"
  }

  /** Wraps string in cyan ANSI codes. */
  def cyan(str: String): String = {
    "\u001B[36m " + str + "\u001B[0m"
  }

  /** Prints `str` in red prefixed with ">>> Transformed | ". */
  def red_println(str: String): Unit = {
    System.out.println(red(s">>> Transformed | ${str}"))
  }

  /** Prints `str` in green prefixed with ">>> Transformed | ". */
  def green_println(str: String): Unit = {
    System.out.println(green(s">>> Transformed | ${str}"))
  }

  /** Prints `str` in yellow prefixed with ">>> Transformed | ". */
  def yellow_println(str: String): Unit = {
    System.out.println(yellow(s">>> Transformed | ${str}"))
  }

  /** Prints `str` in bold prefixed with ">>> Transformed | ". */
  def bold_println(str: String): Unit = {
    System.out.println(bold(s">>> Transformed | ${str}"))
  }

  /** Prints the first `limit` values of a DataFrame column to console. */
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
