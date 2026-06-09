package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @author Shard Zhang
 * @date 2022/4/28 17:33
 * @note
 */
object DateUtils {
  /**
   * 获取date的前后n天的日期, 且输出和输入的日期格式一致
   *
   * @param diff >0 表示未来, <0表示过去
   * @param date
   * @param pattern
   */
  def getDay(diff: Long,
             date: String,
             pattern: String = "yyyy-MM-dd"): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(pattern)
    sdf.format(sdf.parse(date).getTime + diff * 24 * 60 * 60 * 1000L)
  }

  /**
   * 从当前天add diff
   *
   * @param diff
   * @return
   */
  def getDate(diff: Int): String = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance;
    calendar.add(Calendar.DATE, diff);
    sdf.format(calendar.getTime)
  }

  /** 从unix timestamp转换为Java时间 */
  def getDateFromUnixTimestamp(timestamp: String, pattern: String = "yyyMMdd"): String = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date(timestamp.toLong * 1000L))
  }
}
