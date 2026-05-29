package sample

import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer

/**
 * @author shard zhang
 * @date 2026/5/29 18:02
 * @note
 */
class TrainSampleV1 extends Serializable {
  override def toString: String = {
    val str = new StringBuilder()
    // APP相关 9
    str.append(s"app_id: ${app_id}\n")
    str.append(s"app_type: ${app_type}\n")
    str.append(s"first_type: ${first_type}\n")
    str.append(s"second_type: ${second_type}\n")
    str.append(s"second_types: ${second_types.mkString(",")}\n")
    str.append(s"comment_count: ${comment_count}\n")
    str.append(s"download_count: ${download_count}\n")
    str.append(s"avg_comment: ${avg_comment}\n")
    str.append(s"app_embedding: ${app_embedding.map(s => s.toString).mkString(",")}\n")

    // 用户相关 5
    str.append(s"user_imei: ${user_imei}\n")
    str.append(s"model: ${model}\n")
    str.append(s"active_day: ${active_day}\n")
    str.append(s"miss_day: ${miss_day}\n")
    str.append(s"login_day: ${login_day}\n")

    // 上下文场景 4
    str.append(s"time_hour: ${time_hour}\n")
    str.append(s"time_area: ${time_area}\n")
    str.append(s"week_day: ${week_day}\n")
    str.append(s"position: ${position}\n")

    // 用户行为
    str.append(s"user_app_install: ${user_app_install.map(s => s._1 + ":" + s._2).mkString(",")}\n")          // 将(Int.Int)用:拼接起来，然后转换为逗号分隔的字符串
    str.append(s"user_app_install_1days: ${user_app_install_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_install_3days: ${user_app_install_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_install_7days: ${user_app_install_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_install_15days: ${user_app_install_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_install_embedding: ${user_install_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_install_embedding_1days: ${user_install_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_install_embedding_3days: ${user_install_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_install_embedding_7days: ${user_install_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_install_embedding_15days: ${user_install_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_delete: ${user_app_delete.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_delete_1days: ${user_app_delete_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_delete_3days: ${user_app_delete_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_delete_7days: ${user_app_delete_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_delete_15days: ${user_app_delete_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_delete_embedding: ${user_delete_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_delete_embedding_1days: ${user_delete_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_delete_embedding_3days: ${user_delete_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_delete_embedding_7days: ${user_delete_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_delete_embedding_15days: ${user_delete_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_download: ${user_app_download.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_download_1days: ${user_app_download_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_download_3days: ${user_app_download_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_download_7days: ${user_app_download_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_download_15days: ${user_app_download_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_download_embedding: ${user_download_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_download_embedding_1days: ${user_download_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_download_embedding_3days: ${user_download_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_download_embedding_7days: ${user_download_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_download_embedding_15days: ${user_download_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_duration: ${user_app_duration.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_duration_1days: ${user_app_duration_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_duration_3days: ${user_app_duration_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_duration_7days: ${user_app_duration_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_duration_15days: ${user_app_duration_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_duration_embedding: ${user_duration_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_duration_embedding_1days: ${user_duration_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_duration_embedding_3days: ${user_duration_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_duration_embedding_7days: ${user_duration_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_duration_embedding_15days: ${user_duration_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_abe: ${user_app_abe.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_abe_1days: ${user_app_abe_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_abe_3days: ${user_app_abe_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_abe_7days: ${user_app_abe_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_abe_15days: ${user_app_abe_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_abe_embedding: ${user_abe_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_abe_embedding_1days: ${user_abe_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_abe_embedding_3days: ${user_abe_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_abe_embedding_7days: ${user_abe_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_abe_embedding_15days: ${user_abe_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_click: ${user_app_click.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_click_1days: ${user_app_click_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_click_3days: ${user_app_click_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_click_7days: ${user_app_click_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_click_15days: ${user_app_click_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_click_embedding: ${user_click_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_click_embedding_1days: ${user_click_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_click_embedding_3days: ${user_click_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_click_embedding_7days: ${user_click_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_click_embedding_15days: ${user_click_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_update: ${user_app_update.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_update_1days: ${user_app_update_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_update_3days: ${user_app_update_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_update_7days: ${user_app_update_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_update_15days: ${user_app_update_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_update_embedding: ${user_update_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_update_embedding_1days: ${user_update_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_update_embedding_3days: ${user_update_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_update_embedding_7days: ${user_update_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_update_embedding_15days: ${user_update_embedding_15days.map(s => s.toString).mkString(",")}\n")

    str.append(s"user_app_comment: ${user_app_comment.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_comment_1days: ${user_app_comment_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_comment_3days: ${user_app_comment_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_comment_7days: ${user_app_comment_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_app_comment_15days: ${user_app_comment_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    str.append(s"user_comment_embedding: ${user_comment_embedding.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_comment_embedding_1days: ${user_comment_embedding_1days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_comment_embedding_3days: ${user_comment_embedding_3days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_comment_embedding_7days: ${user_comment_embedding_7days.map(s => s.toString).mkString(",")}\n")
    str.append(s"user_comment_embedding_15days: ${user_comment_embedding_15days.map(s => s.toString).mkString(",")}\n")
    str.append(s"target: ${target}\n")

    str.toString()
  }

  /** *****************************用户相关 ************************************/
  // 用户imei码
  var user_imei: String = ""

  // 机型
  var model: String = ""

  // 激活距离当前的天数, -1表示新用户
  var active_day: Int = 0

  // 离开距离当前的天数(多久没登陆)
  var miss_day: Int = 0

  // 登录活跃的天数(多久没有login)
  var login_day: Int = 0

  /** *****************************相关场景 ************************************/
  // 用户的app安装列表
  var user_app_install: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_install_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_install_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_install_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_install_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_install_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_install_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_install_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_install_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_install_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户的app卸载列表
  var user_app_delete: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_delete_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_delete_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_delete_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_delete_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_delete_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_delete_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_delete_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_delete_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_delete_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近下载的列表
  var user_app_download: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_download_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_download_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_download_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_download_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_download_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_download_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_download_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_download_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_download_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近使用的列表
  var user_app_duration: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_duration_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_duration_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_duration_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_duration_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_duration_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_duration_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_duration_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_duration_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_duration_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近活跃的列表
  var user_app_abe: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_abe_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_abe_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_abe_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_abe_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_abe_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_abe_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_abe_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_abe_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_abe_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近更新的列表
  var user_app_update: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_update_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_update_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_update_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_update_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_update_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_update_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_update_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_update_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_update_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近点击的列表
  var user_app_click: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_click_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_click_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_click_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_click_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_click_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_click_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_click_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_click_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_click_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  // 用户最近评论的列表
  var user_app_comment: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_comment_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_comment_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_comment_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_app_comment_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  var user_comment_embedding: ListBuffer[Double] = new ListBuffer[Double]
  var user_comment_embedding_1days: ListBuffer[Double] = new ListBuffer[Double]
  var user_comment_embedding_3days: ListBuffer[Double] = new ListBuffer[Double]
  var user_comment_embedding_7days: ListBuffer[Double] = new ListBuffer[Double]
  var user_comment_embedding_15days: ListBuffer[Double] = new ListBuffer[Double]

  /** *****************************场景相关 ************************************/
  //  场景
  var page: String = ""

  /** *****************************APP相关 9 ************************************/
  //  广告id
  var app_id: Int = 0 //注意类型：Int

  //  广告embedding
  var app_embedding: ListBuffer[Double] = new ListBuffer[Double]

  //  广告类目
  var app_type: Int = 0

  // 广告一级分类
  var first_type: Int = 0

  // 广告二级分类
  var second_type: Int = 0

  // 广告二级分类
  var second_types: ListBuffer[Int] = new ListBuffer[Int]

  // 应用历史评价数
  var comment_count: Long = 0

  // 应用累计下载量
  var download_count: Long = 0

  // 应用平均评分
  var avg_comment: Double = 0


  /** ***************************** 上下文场景4 ************************************/
  var time_hour: Int = 0 //小时

  var time_area: Int = 0 //时区

  var week_day: Int = 0 //星期

  var position: Int = 0 //

  /** ***************************** 目标值1 ************************************/
  var target: Int = 0
}

/**
 * 伴生对象（和伴生类之间可以互相访问对方的私有成员）
 */
object TrainSampleV1 {
  /** The seed to be used, copied from scala's murmurhash implementation. */
  final val SEED: Int = 0x3c074a61

  val embedding_dim = 32
  val zero_embedding = new Array[Double](embedding_dim)
  for (i <- 0 until embedding_dim) {
    zero_embedding(i) = 0
  }

  /**
   * embedding的sum pooling
   */
  def embedding(app_ids: ListBuffer[(Int, Int)],
                app_embedding: scala.collection.Map[String, Array[Double]]): Array[Double] = {

    val embedding = new Array[Double](embedding_dim)
    for (i <- 0 until embedding_dim) {
      embedding(i) = 0
    }

    for (app_id <- app_ids) {
      if (app_embedding.contains(app_id._1.toString)) {
        val emb = app_embedding.getOrElse(app_id._1.toString, zero_embedding) //有必要么？
        for (i <- 0 until embedding_dim) {
          embedding(i) = embedding(i) + emb(i) * app_id._2
        }
      }
    }
    embedding
  }

  /**
   * 按照app_info._1进行聚合（相当于对同一个app的次数，进行累加）
   * @param app_info
   * @return
   */
  def merge(app_info: ListBuffer[(Int, Int)]): ListBuffer[(Int, Int)] = {
    val list = app_info
      .groupBy(s => s._1)
      .map(s => (s._1, s._2.map(r => r._2).sum))
      .toList

    // List转换为ListBuffer
    val buf = new ListBuffer[(Int, Int)]
    for (t <- list) {
      buf.append(t)
    }
    buf
  }

  def LOG_DEBUG(log: String): Unit = {
    println(log)
  }

  /**
   * 解析join_sample中的样本
   * @param row ("label", "imei", "app_id", "algo_type", "cp", "time", "position", "score", "app_features", "user_features"  dm_cpd_recommend_joined_sample_day_v2 (应用市场首页训练数据表)是拼接以后的表)
   * @param app_mapping Map(appid, (app_package, first_type, second_types)) 用于黑名单应用过滤
   * @param app_embedding 用于计算emb
   * @return (TrainingSampleV2006, Boolean)
   */
  def parseSample(row: Row,
                  app_mapping: scala.collection.Map[String, (String, Int, Array[Int])],
                  app_embedding: scala.collection.Map[String, Array[Double]])
  : (TrainingSampleV2006, Boolean) = {

    val training_sample = new TrainingSampleV2006()

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val sdf1 = new SimpleDateFormat("yyyyMMdd")

    // 多分类的label就是app_id
    training_sample.target = row.getString(0).toInt
    var ret = true

    training_sample.app_id =
      try {
        row.getString(2).toInt
      } catch {
        case e: Throwable => {
          println("training_sample.app_id: " + e.toString + " " + row.toSeq.toString()) //将这一条row转换为序列，然后再转换为string
          ret = false
          0 //如果抛出异常，则app_id返回0，并且res为false。注意：0是case e的输出，而不是所有的输出
        }
      }

    val emb = app_embedding.getOrElse(training_sample.app_id.toString, zero_embedding)
    for (e <- emb) {
      training_sample.app_embedding.append(e)
    }

    // The hour
    // 20190412184432
    var sample_time_stamp = 0L
    var week_day = 0
    val hour =
      try {
        val t = sdf.parse(row.getString(5))
        week_day = t.getDay // 天
        sample_time_stamp = t.getTime // 毫秒
        t.getHours // 小时
      } catch {
        case e: Throwable => {
          println("training_sample.time_hour: " + e.toString + " " + row.toSeq.toString())
          ret = false
          0
        }
      }

    // 上下文场景 4
    training_sample.time_hour = hour
    training_sample.time_area = hour / 4
    training_sample.week_day = week_day
    training_sample.position =
      try {
        row.getString(6).toInt
      } catch {
        case e: Throwable => { //Throwable类是Error类和Exception类的基类，也是所有异常类的基类
          println("training_sample.position: " + e.toString + " " + row.toSeq.toString())
          0
        }
      }

    /**
     * app_features:
     * first_type;second_type;comment_count;download_count;avg_comment - 136;181;299072;595586129;4.45
     */
    if (row.getString(8) != "-") {
      val app_features_fields = row.getString(8).split(";")

      training_sample.first_type =
        try {
          app_features_fields(0).toInt
        } catch {
          case _: Throwable => 0
        }

      training_sample.second_type =
        try {
          app_features_fields(1)
            .split("_")
            .map(s => s.toInt)
            .max
        } catch {
          case _: Throwable => 0
        }

      try {
        val second_types = app_features_fields(1)
          .split("_")
          .map(s => s.toInt)
        for (t <- second_types)
          training_sample.second_types.append(t)
      } catch {
        case _: Throwable => 0
      }

      training_sample.comment_count =
        try {
          app_features_fields(2).toLong
        } catch {
          case _: Throwable => 0
        }

      training_sample.download_count =
        try {
          app_features_fields(3).toLong
        } catch {
          case _: Throwable => 0L
        }

      training_sample.avg_comment =
        try {
          app_features_fields(4).toDouble
        } catch {
          case _: Throwable => 0.0
        }
    }

    // user_features
    if (row.getString(9) != "-") {
      val user_features_fields = row.getString(9).split("\\|")

      // user_active
      if (user_features_fields(0) != "-") {
        val user_active_fields = user_features_fields(0).split(":") //Array(VIVOZ5X, 60, 3, 37)
        training_sample.model = user_active_fields(0) //机型
        training_sample.active_day = user_active_fields(1).toInt // 激活距离当前的天数
        training_sample.miss_day = user_active_fields(2).toInt // 离开距离当前的天数(多久没登陆)
        training_sample.login_day = user_active_fields(3).toInt // 登录活跃的天数
      }

      // user_app_install 用户安装
      if (user_features_fields(1) != "-") {
        val user_app_install_items = user_features_fields(1).split(",")
        for (item <- user_app_install_items) {
          val words = item.split(":") //Array(54710,1,20190829)
          val id = words(0).toInt //appid
          val cnt = words(1).toInt //安装次数
          val time = words(2) //安装时间

          // 过滤（这里开始调包，FilterApp.filter_app()）
          if (FilterApp.filter_app(words(0), app_mapping)) { //如果通过筛选，则加入training_sample.user_app_install
            training_sample.user_app_install.append((id, cnt)) //(app_id, cnt)

            val dur =
              try {
                (sample_time_stamp - sdf1.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1.0 //YYYYMMDD   天
              } catch {
                case e: Throwable => {
                  println("Parse user_app_install: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false //中间只要出现一个异常抛出，结果ret就为false
                  Double.MaxValue //这是返回值  这是一个非常大的数
                }
              }

            LOG_DEBUG(s"user_app_install time ${time} dur ${dur}") //打印一下log

            if (dur <= 1) {
              training_sample.user_app_install_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_install_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_install_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_install_15days.append((id, cnt))
            }
          }
        }

        // 用户app安装
        // 对相同id的安装次数进行聚合，即累加去重（列表的维度表示app的个数）
        training_sample.user_app_install = merge(training_sample.user_app_install)
        training_sample.user_app_install_1days = merge(training_sample.user_app_install_1days)
        training_sample.user_app_install_3days = merge(training_sample.user_app_install_3days)
        training_sample.user_app_install_7days = merge(training_sample.user_app_install_7days)
        training_sample.user_app_install_15days = merge(training_sample.user_app_install_15days)

        // 计算安装的embedding向量
        var emb = embedding(training_sample.user_app_install, app_embedding)
        for (e <- emb) {
          training_sample.user_install_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_install_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_install_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_install_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_install_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_install_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_install_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_install_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_install_embedding_15days.append(e)
        }
      }

      // 以下同上
      // user_app_delete 卸载
      if (user_features_fields(2) != "-") {
        val user_app_delete_items = user_features_fields(2).split(",")
        for (item <- user_app_delete_items) {
          val words = item.split(":") //Array(1748536, 1, 20190829)
          val id = words(0).toInt
          val cnt = words(1).toInt
          val time = words(2)

          // 过滤
          if (FilterApp.filter_app(words(0), app_mapping)) { //还是不清楚app_mapping长什么样子？？
            training_sample.user_app_delete.append((id, cnt))

            val dur =
              try {
                (sample_time_stamp - sdf1.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1.0 //转换为天   为什么要-1？
              } catch {
                case e: Throwable => {
                  println("Parse user_app_delete: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_delete time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_delete_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_delete_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_delete_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_delete_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_delete = merge(training_sample.user_app_delete)
        training_sample.user_app_delete_1days = merge(training_sample.user_app_delete_1days)
        training_sample.user_app_delete_3days = merge(training_sample.user_app_delete_3days)
        training_sample.user_app_delete_7days = merge(training_sample.user_app_delete_7days)
        training_sample.user_app_delete_15days = merge(training_sample.user_app_delete_15days)

        var emb = embedding(training_sample.user_app_delete, app_embedding)
        for (e <- emb) {
          training_sample.user_delete_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_delete_1days, app_embedding)
        training_sample.user_delete_embedding_1days.appendAll(emb)

        emb = embedding(training_sample.user_app_delete_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_delete_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_delete_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_delete_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_delete_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_delete_embedding_15days.append(e)
        }
      }

      // user_app_duration  使用时间
      if (user_features_fields(3) != "-") {
        val user_app_duration_items = user_features_fields(3).split(",")
        for (item <- user_app_duration_items) {
          val items = item.split(":") //Array(47925, 2, 20190831)  (appid,使用次数，使用时间）

          val id = items(0).toInt
          val cnt = try {
            items(1).toInt
          } catch {
            case e: Throwable => {
              println("Parse user_app_duration: " + e.toString + " " + item + "\n" + row.toSeq.toString())
              ret = false
              0
            }
          }
          val time = items(2)

          // 过滤
          if (FilterApp.filter_app(items(0), app_mapping)) {
            training_sample.user_app_duration.append((id, cnt))
            val dur =
              try {
                (sample_time_stamp - sdf1.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1.0
              } catch {
                case e: Throwable => {
                  println("Parse user_app_duration: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }
            LOG_DEBUG(s"user_app_duration time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_duration_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_duration_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_duration_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_duration_15days.append((id, cnt))
            }
          }
        }
        training_sample.user_app_duration = merge(training_sample.user_app_duration)
        training_sample.user_app_duration_1days = merge(training_sample.user_app_duration_1days)
        training_sample.user_app_duration_3days = merge(training_sample.user_app_duration_3days)
        training_sample.user_app_duration_7days = merge(training_sample.user_app_duration_7days)
        training_sample.user_app_duration_15days = merge(training_sample.user_app_duration_15days)

        var emb = embedding(training_sample.user_app_duration, app_embedding)
        for (e <- emb) {
          training_sample.user_duration_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_duration_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_duration_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_duration_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_duration_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_duration_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_duration_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_duration_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_duration_embedding_15days.append(e)
        }
      }


      // user_app_abe 活跃
      if (user_features_fields(4) != "-") {
        val user_app_abe_items = user_features_fields(4).split(",")
        for (item <- user_app_abe_items) {
          val items = item.split(":")
          val id = items(0).toInt //
          val cnt = items(1).toInt //活跃次数
          val time = items(2) //活跃时间

          // 过滤
          if (FilterApp.filter_app(items(0), app_mapping)) {
            training_sample.user_app_abe.append((id, cnt))
            val dur =
              try {
                (sample_time_stamp - sdf1.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1.0
              } catch {
                case e: Throwable => {
                  println("Parse user_app_delete: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_delete time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_abe_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_abe_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_abe_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_abe_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_abe = merge(training_sample.user_app_abe)
        training_sample.user_app_abe_1days = merge(training_sample.user_app_abe_1days)
        training_sample.user_app_abe_3days = merge(training_sample.user_app_abe_3days)
        training_sample.user_app_abe_7days = merge(training_sample.user_app_abe_7days)
        training_sample.user_app_abe_15days = merge(training_sample.user_app_abe_15days)

        var emb = embedding(training_sample.user_app_abe, app_embedding)
        for (e <- emb) {
          training_sample.user_abe_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_abe_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_abe_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_abe_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_abe_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_abe_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_abe_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_abe_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_abe_embedding_15days.append(e)
        }
      }

      // user_app_download 下载
      if (user_features_fields(5) != "-") {
        val user_app_download_items = user_features_fields(5).split(",")
        for (item <- user_app_download_items) {
          val words = item.split(":", -1)
          val id = words(0).toInt
          val cnt = words(1).toInt
          val time = words(2)

          // 过滤
          if (FilterApp.filter_app(words(0), app_mapping)) {
            training_sample.user_app_download.append((id, cnt))

            val dur =
              try {
                (sample_time_stamp - sdf.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1
              } catch {
                case e: Throwable => {
                  println("Parse user_app_download: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_download time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_download_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_download_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_download_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_download_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_download = merge(training_sample.user_app_download)
        training_sample.user_app_download_1days = merge(training_sample.user_app_download_1days)
        training_sample.user_app_download_3days = merge(training_sample.user_app_download_3days)
        training_sample.user_app_download_7days = merge(training_sample.user_app_download_7days)
        training_sample.user_app_download_15days = merge(training_sample.user_app_download_15days)

        var emb = embedding(training_sample.user_app_download, app_embedding)
        for (e <- emb) {
          training_sample.user_download_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_download_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_download_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_download_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_download_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_download_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_download_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_download_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_download_embedding_15days.append(e)
        }
      }

      // user_app_click 点击
      if (user_features_fields(6) != "-") {
        val user_app_click_items = user_features_fields(6).split(",")
        for (item <- user_app_click_items) {
          val words = item.split(":", -1)
          val id = words(0).toInt
          val cnt = words(1).toInt
          val time = words(2)

          // 过滤
          if (FilterApp.filter_app(words(0), app_mapping)) {
            training_sample.user_app_click.append((id, cnt))

            val dur =
              try {
                (sample_time_stamp - sdf.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1
              } catch {
                case e: Throwable => {
                  println("Parse user_app_click: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_click time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_click_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_click_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_click_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_click_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_click = merge(training_sample.user_app_click)
        training_sample.user_app_click_1days = merge(training_sample.user_app_click_1days)
        training_sample.user_app_click_3days = merge(training_sample.user_app_click_3days)
        training_sample.user_app_click_7days = merge(training_sample.user_app_click_7days)
        training_sample.user_app_click_15days = merge(training_sample.user_app_click_15days)

        var emb = embedding(training_sample.user_app_click, app_embedding)
        for (e <- emb) {
          training_sample.user_click_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_click_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_click_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_click_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_click_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_click_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_click_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_click_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_click_embedding_15days.append(e)
        }
      }

      // user_app_update  更新
      if (user_features_fields(7) != "-") {
        val user_app_update_items = user_features_fields(7).split(",")
        for (item <- user_app_update_items) {
          val words = item.split(":", -1)
          val id = words(0).toInt
          val cnt = words(1).toInt
          val time = words(2)

          // 过滤(预安装应用和系统应用)
          if (FilterApp.filter_app(words(0), app_mapping)) {
            training_sample.user_app_update.append((id, cnt))

            val dur =
              try {
                (sample_time_stamp - sdf.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1
              } catch {
                case e: Throwable => {
                  println("Parse user_app_update: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_update time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_update_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_update_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_update_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_update_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_update = merge(training_sample.user_app_update)
        training_sample.user_app_update_1days = merge(training_sample.user_app_update_1days)
        training_sample.user_app_update_3days = merge(training_sample.user_app_update_3days)
        training_sample.user_app_update_7days = merge(training_sample.user_app_update_7days)
        training_sample.user_app_update_15days = merge(training_sample.user_app_update_15days)

        var emb = embedding(training_sample.user_app_update, app_embedding)
        for (e <- emb) {
          training_sample.user_update_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_update_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_update_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_update_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_update_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_update_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_update_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_update_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_update_embedding_15days.append(e)
        }
      }

      // user_app_comment 评论
      if (user_features_fields(8) != "-") {
        val user_app_comment_items = user_features_fields(8).split(",")
        for (item <- user_app_comment_items) {
          val words = item.split(":", -1)
          val id = words(0).toInt //appid
          val score = words(1).toDouble //评论分数
          val cnt = words(2).toInt //评论次数
          val time = words(3) //评论时间

          // 过滤
          if (FilterApp.filter_app(words(0), app_mapping)) {
            training_sample.user_app_comment.append((id, cnt))

            val dur =
              try {
                (sample_time_stamp - sdf.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1
              } catch {
                case e: Throwable => {
                  println("Parse user_app_comment: " + e.toString + " " + item + "\n" + row.toSeq.toString())
                  ret = false
                  Double.MaxValue
                }
              }

            LOG_DEBUG(s"user_app_comment time ${time} dur ${dur}")

            if (dur <= 1) {
              training_sample.user_app_comment_1days.append((id, cnt))
            }
            if (dur <= 3) {
              training_sample.user_app_comment_3days.append((id, cnt))
            }
            if (dur <= 7) {
              training_sample.user_app_comment_7days.append((id, cnt))
            }
            if (dur <= 15) {
              training_sample.user_app_comment_15days.append((id, cnt))
            }
          }
        }

        training_sample.user_app_comment = merge(training_sample.user_app_comment)
        training_sample.user_app_comment_1days = merge(training_sample.user_app_comment_1days)
        training_sample.user_app_comment_3days = merge(training_sample.user_app_comment_3days)
        training_sample.user_app_comment_7days = merge(training_sample.user_app_comment_7days)
        training_sample.user_app_comment_15days = merge(training_sample.user_app_comment_15days)

        var emb = embedding(training_sample.user_app_comment, app_embedding)
        for (e <- emb) {
          training_sample.user_comment_embedding.append(e)
        }

        emb = embedding(training_sample.user_app_comment_1days, app_embedding)
        for (e <- emb) {
          training_sample.user_comment_embedding_1days.append(e)
        }

        emb = embedding(training_sample.user_app_comment_3days, app_embedding)
        for (e <- emb) {
          training_sample.user_comment_embedding_3days.append(e)
        }

        emb = embedding(training_sample.user_app_comment_7days, app_embedding)
        for (e <- emb) {
          training_sample.user_comment_embedding_7days.append(e)
        }

        emb = embedding(training_sample.user_app_comment_15days, app_embedding)
        for (e <- emb) {
          training_sample.user_comment_embedding_15days.append(e)
        }
      }
    }
    (training_sample, ret) //ret表示解析的结果，只要有一个参数为解析成功，即为解析失败
  }
}
