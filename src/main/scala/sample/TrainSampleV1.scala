//package sample
//
//import java.text.SimpleDateFormat
//import org.apache.spark.sql.Row
//import scala.collection.mutable.ListBuffer
//
///**
// * @author shard zhang
// * @date 2026/5/29 18:02
// * @note
// */
//class TrainSampleV1 extends Serializable {
//  override def toString: String = {
//    val str = new StringBuilder()
//    // user
//    str.append(s"user_id: ${user_id}\n")
//
//    // item
//    str.append(s"item_id: ${item_id}\n")
//    str.append(s"title: ${title}\n")
//    str.append(s"genres: ${genres}\n")
//
//    // context
//    str.append(s"time_hour: ${time_hour}\n")
//    str.append(s"time_area: ${time_area}\n")
//    str.append(s"week_day: ${week_day}\n")
//
//    // 用户行为
//    str.append(s"user_movie_rate: ${user_movie_rate.map(s => s._1 + ":" + s._2).mkString(",")}\n") // 将(Int.Int)用:拼接起来，然后转换为逗号分隔的字符串
//    str.append(s"user_movie_rate_3days: ${user_movie_rate_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
//
//    str.append(s"target: ${target}\n")
//    str.toString()
//  }
//
//  /** ***************************** user *********************************** */
//  var user_id: String = ""
//
//
//  /** ***************************** item *********************************** */
//  var item_id: Int = 0
//
//  var title: Int = 0
//
//  var genres: Int = 0
//
//  var rate_count: Long = 0
//
//  var avg_rate: Double = 0
//
//  /** ***************************** user behavior *********************************** */
//  var user_movie_rate: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
//  var user_movie_rate_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
//  var user_movie_rate_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
//  var user_movie_rate_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
//  var user_movie_rate_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
//
//  /** ***************************** context *********************************** */
//  var week_day: Int = 0
//  var time_hour: Int = 0
//  var time_area: Int = 0
//
//  var target: Int = 0
//}
//
//object TrainSampleV1 {
//  /** The seed to be used, copied from scala's murmurhash implementation. */
//  final val SEED: Int = 0x3c074a61
//
//  val embedding_dim = 32
//  val zero_embedding = new Array[Double](embedding_dim)
//  for (i <- 0 until embedding_dim) {
//    zero_embedding(i) = 0
//  }
//
//  /**
//   * embedding的sum pooling
//   */
//  def embedding(app_ids: ListBuffer[(Int, Int)],
//                app_embedding: scala.collection.Map[String, Array[Double]]): Array[Double] = {
//
//    val embedding = new Array[Double](embedding_dim)
//    for (i <- 0 until embedding_dim) {
//      embedding(i) = 0
//    }
//
//    for (app_id <- app_ids) {
//      if (app_embedding.contains(app_id._1.toString)) {
//        val emb = app_embedding.getOrElse(app_id._1.toString, zero_embedding) //有必要么？
//        for (i <- 0 until embedding_dim) {
//          embedding(i) = embedding(i) + emb(i) * app_id._2
//        }
//      }
//    }
//    embedding
//  }
//
//  /**
//   * 按照app_info._1进行聚合（相当于对同一个app的次数，进行累加）
//   *
//   * @param app_info
//   * @return
//   */
//  def merge(app_info: ListBuffer[(Int, Int)]): ListBuffer[(Int, Int)] = {
//    val list = app_info
//      .groupBy(s => s._1)
//      .map(s => (s._1, s._2.map(r => r._2).sum))
//      .toList
//
//    // List转换为ListBuffer
//    val buf = new ListBuffer[(Int, Int)]
//    for (t <- list) {
//      buf.append(t)
//    }
//    buf
//  }
//
//
//  /**
//   * 解析join_sample中的样本
//   *
//   * @param row           ("label", "imei", "app_id", "algo_type", "cp", "time", "position", "score", "app_features", "user_features"  dm_cpd_recommend_joined_sample_day_v2 (应用市场首页训练数据表)是拼接以后的表)
//   * @param app_mapping   Map(appid, (app_package, first_type, second_types)) 用于黑名单应用过滤
//   * @param app_embedding 用于计算emb
//   * @return (TrainingSampleV2006, Boolean)
//   */
//  def parseSample(row: Row,
//                  app_mapping: scala.collection.Map[String, (String, Int, Array[Int])],
//                  app_embedding: scala.collection.Map[String, Array[Double]])
//  : (TrainSampleV1, Boolean) = {
//
//    val training_sample = new TrainSampleV1()
//
//    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
//    val sdf1 = new SimpleDateFormat("yyyyMMdd")
//
//    // 多分类的label就是app_id
//    training_sample.target = row.getString(0).toInt
//    var ret = true
//
//    training_sample.app_id =
//      try {
//        row.getString(2).toInt
//      } catch {
//        case e: Throwable => {
//          println("training_sample.app_id: " + e.toString + " " + row.toSeq.toString()) //将这一条row转换为序列，然后再转换为string
//          ret = false
//          0 //如果抛出异常，则app_id返回0，并且res为false。注意：0是case e的输出，而不是所有的输出
//        }
//      }
//
//    val emb = app_embedding.getOrElse(training_sample.app_id.toString, zero_embedding)
//    for (e <- emb) {
//      training_sample.app_embedding.append(e)
//    }
//
//    // The hour
//    // 20190412184432
//    var sample_time_stamp = 0L
//    var week_day = 0
//    val hour =
//      try {
//        val t = sdf.parse(row.getString(5))
//        week_day = t.getDay // 天
//        sample_time_stamp = t.getTime // 毫秒
//        t.getHours // 小时
//      } catch {
//        case e: Throwable => {
//          println("training_sample.time_hour: " + e.toString + " " + row.toSeq.toString())
//          ret = false
//          0
//        }
//      }
//
//    // 上下文场景 4
//    training_sample.time_hour = hour
//    training_sample.time_area = hour / 4
//    training_sample.week_day = week_day
//    training_sample.position =
//      try {
//        row.getString(6).toInt
//      } catch {
//        case e: Throwable => { //Throwable类是Error类和Exception类的基类，也是所有异常类的基类
//          println("training_sample.position: " + e.toString + " " + row.toSeq.toString())
//          0
//        }
//      }
//
//    /**
//     * app_features:
//     * first_type;second_type;comment_count;download_count;avg_comment - 136;181;299072;595586129;4.45
//     */
//    if (row.getString(8) != "-") {
//      val app_features_fields = row.getString(8).split(";")
//
//      training_sample.first_type =
//        try {
//          app_features_fields(0).toInt
//        } catch {
//          case _: Throwable => 0
//        }
//
//      training_sample.second_type =
//        try {
//          app_features_fields(1)
//            .split("_")
//            .map(s => s.toInt)
//            .max
//        } catch {
//          case _: Throwable => 0
//        }
//
//      try {
//        val second_types = app_features_fields(1)
//          .split("_")
//          .map(s => s.toInt)
//        for (t <- second_types)
//          training_sample.second_types.append(t)
//      } catch {
//        case _: Throwable => 0
//      }
//
//      training_sample.comment_count =
//        try {
//          app_features_fields(2).toLong
//        } catch {
//          case _: Throwable => 0
//        }
//
//      training_sample.download_count =
//        try {
//          app_features_fields(3).toLong
//        } catch {
//          case _: Throwable => 0L
//        }
//
//      training_sample.avg_comment =
//        try {
//          app_features_fields(4).toDouble
//        } catch {
//          case _: Throwable => 0.0
//        }
//    }
//
//    // user_features
//    if (row.getString(9) != "-") {
//      val user_features_fields = row.getString(9).split("\\|")
//
//      // user_active
//      if (user_features_fields(0) != "-") {
//        val user_active_fields = user_features_fields(0).split(":") //Array(VIVOZ5X, 60, 3, 37)
//        training_sample.model = user_active_fields(0) //机型
//        training_sample.active_day = user_active_fields(1).toInt // 激活距离当前的天数
//        training_sample.miss_day = user_active_fields(2).toInt // 离开距离当前的天数(多久没登陆)
//        training_sample.login_day = user_active_fields(3).toInt // 登录活跃的天数
//      }
//
//      // user_app_install 用户安装
//      if (user_features_fields(1) != "-") {
//        val user_app_install_items = user_features_fields(1).split(",")
//        for (item <- user_app_install_items) {
//          val words = item.split(":") //Array(54710,1,20190829)
//          val id = words(0).toInt //appid
//          val cnt = words(1).toInt //安装次数
//          val time = words(2) //安装时间
//
//          // 过滤（这里开始调包，FilterApp.filter_app()）
//          if (FilterApp.filter_app(words(0), app_mapping)) { //如果通过筛选，则加入training_sample.user_app_install
//            training_sample.user_app_install.append((id, cnt)) //(app_id, cnt)
//
//            val dur =
//              try {
//                (sample_time_stamp - sdf1.parse(time).getTime) / 1000.0 / 3600.0 / 24.0 - 1.0 //YYYYMMDD   天
//              } catch {
//                case e: Throwable => {
//                  println("Parse user_app_install: " + e.toString + " " + item + "\n" + row.toSeq.toString())
//                  ret = false //中间只要出现一个异常抛出，结果ret就为false
//                  Double.MaxValue //这是返回值  这是一个非常大的数
//                }
//              }
//
//            LOG_DEBUG(s"user_app_install time ${time} dur ${dur}") //打印一下log
//
//            if (dur <= 1) {
//              training_sample.user_app_install_1days.append((id, cnt))
//            }
//            if (dur <= 3) {
//              training_sample.user_app_install_3days.append((id, cnt))
//            }
//            if (dur <= 7) {
//              training_sample.user_app_install_7days.append((id, cnt))
//            }
//            if (dur <= 15) {
//              training_sample.user_app_install_15days.append((id, cnt))
//            }
//          }
//        }
//
//        // 用户app安装
//        // 对相同id的安装次数进行聚合，即累加去重（列表的维度表示app的个数）
//        training_sample.user_app_install = merge(training_sample.user_app_install)
//        training_sample.user_app_install_1days = merge(training_sample.user_app_install_1days)
//        training_sample.user_app_install_3days = merge(training_sample.user_app_install_3days)
//        training_sample.user_app_install_7days = merge(training_sample.user_app_install_7days)
//        training_sample.user_app_install_15days = merge(training_sample.user_app_install_15days)
//
//        // 计算安装的embedding向量
//        var emb = embedding(training_sample.user_app_install, app_embedding)
//        for (e <- emb) {
//          training_sample.user_install_embedding.append(e)
//        }
//
//        emb = embedding(training_sample.user_app_install_1days, app_embedding)
//        for (e <- emb) {
//          training_sample.user_install_embedding_1days.append(e)
//        }
//
//        emb = embedding(training_sample.user_app_install_3days, app_embedding)
//        for (e <- emb) {
//          training_sample.user_install_embedding_3days.append(e)
//        }
//
//        emb = embedding(training_sample.user_app_install_7days, app_embedding)
//        for (e <- emb) {
//          training_sample.user_install_embedding_7days.append(e)
//        }
//
//        emb = embedding(training_sample.user_app_install_15days, app_embedding)
//        for (e <- emb) {
//          training_sample.user_install_embedding_15days.append(e)
//        }
//      }
//    }
//    (training_sample, ret) //ret表示解析的结果，只要有一个参数为解析成功，即为解析失败
//  }
//}
