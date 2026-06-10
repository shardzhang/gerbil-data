package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @author Shard Zhang
 * @date 2022/4/28 17:33
 * @note
 */
/** Date arithmetic and formatting utilities. */
object DateUtils {
  /** Returns the date `diff` days before/after `date`. @param diff >0 = future, <0 = past */
  def getDay(diff: Long,
             date: String,
             pattern: String = "yyyy-MM-dd"): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(pattern)
    sdf.format(sdf.parse(date).getTime + diff * 24 * 60 * 60 * 1000L)
  }

  /** Returns today's date offset by `diff` days in "yyyy-MM-dd" format. */
  def getDate(diff: Int): String = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance;
    calendar.add(Calendar.DATE, diff);
    sdf.format(calendar.getTime)
  }

  /** Converts a Unix timestamp (seconds) into a formatted date string. */
  def getDateFromUnixTimestamp(timestamp: String, pattern: String = "yyyyMMdd"): String = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date(timestamp.toLong * 1000L))
  }
}
