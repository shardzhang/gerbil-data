package feature

/**
 * @author shard zhang
 * @date 2026/6/4 11:42
 * @note
 */
object ML1MMovieStatFeature {
  //     var json = row.getAs[String]("item_feature")
  //    if (json != null && json != "{}") {
  //      val item_feature = JSON.parseObject(json)
  //      train_sample.movie_title = item_feature.getString("title")
  //      train_sample.movie_publish_year = try {
  //        val pattern = "\\((\\d{4})\\)".r
  //        pattern.findFirstMatchIn(train_sample.movie_title).map(_.group(1).toInt).getOrElse(1990)
  //      } catch {
  //        case _: Exception => 0
  //      }
  //      val genresArray = item_feature.getJSONArray("genres")
  //      if (genresArray != null) {
  //        train_sample.movie_genres ++= genresArray.toJavaList(classOf[String]).asScala
  //        train_sample.movie_genres.map(r => r.trim.toLowerCase())
  //        train_sample.movie_genre_cnt = train_sample.movie_genres.size
  //      }
  //      train_sample.movie_rate_count = item_feature.getLong("rate_count")
  //      train_sample.movie_avg_rate = item_feature.getDouble("avg_rate")
  //      train_sample.movie_hot_rank = item_feature.getIntValue("hot_rank")
  //    }

  // movie_title: 电影标题
  // movie_publish_year: 电影发布年份
  // movie_genres: 电影类型
  // movie_genre_cnt: 电影类型数
  // movie_rate_count: 电影
  // movie_avg_rate
  // movie_hot_rank
  def main(args: Array[String]): Unit = {

  }
}
