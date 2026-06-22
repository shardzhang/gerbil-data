package featurizer.ml1m

import org.scalatest.{Matchers, WordSpec}
import featurizer.core.FieldType

class ML1MContextFeaturesTest extends WordSpec with Matchers {

  private def sample(hour: Int, weekDay: Int): ML1MSample = {
    val s = new ML1MSample()
    s.time_hour = hour
    s.time_area = hour / 4
    s.week_day = weekDay
    s
  }

  "ContextTimeHour" should {
    "parse hour correctly" in {
      val f = new ContextTimeHour(201, "context_time_hour")
      f.parse(sample(14, 3))
      assert(f.raw_list.head === "14")
      assert(f.feature_list.head === 15)
      assert(f.value_list.head === 1.0F)
    }

    "handle hour 0" in {
      val f = new ContextTimeHour(201, "context_time_hour")
      f.parse(sample(0, 1))
      assert(f.feature_list.head === 1)
    }

    "handle hour 23" in {
      val f = new ContextTimeHour(201, "context_time_hour")
      f.parse(sample(23, 5))
      assert(f.feature_list.head === 24)
    }

    "append on each parse (no auto-clear)" in {
      val f = new ContextTimeHour(201, "context_time_hour")
      f.parse(sample(10, 1))
      f.parse(sample(20, 2))
      assert(f.raw_list.size === 2)
      assert(f.feature_list.last === 21)
    }
  }

  "ContextTimeArea" should {
    "parse time area correctly" in {
      val f = new ContextTimeArea(202, "context_time_area")
      f.parse(sample(14, 3))
      assert(f.raw_list.head === "3")
      assert(f.feature_list.head === 4)
    }

    "handle dawn (hour 0-3)" in {
      val f = new ContextTimeArea(202, "context_time_area")
      f.parse(sample(2, 1))
      assert(f.feature_list.head === 1)
    }

    "handle night (hour 20-23)" in {
      val f = new ContextTimeArea(202, "context_time_area")
      f.parse(sample(22, 5))
      assert(f.feature_list.head === 6)
    }
  }

  "ContextTimeWeek" should {
    "parse week day directly" in {
      val f = new ContextTimeWeek(203, "context_time_week")
      f.parse(sample(10, 3))
      assert(f.raw_list.head === "3")
      assert(f.feature_list.head === 3)
    }

    "handle week day 7" in {
      val f = new ContextTimeWeek(203, "context_time_week")
      f.parse(sample(10, 7))
      assert(f.feature_list.head === 7)
    }
  }

  "IsWeekend" should {
    "return 1 for weekdays" in {
      val f = new IsWeekend(204, "context_is_weekend")
      f.parse(sample(10, 3))
      assert(f.raw_list.head === "1")
      assert(f.feature_list.head === 1)
    }

    "return 2 for Saturday" in {
      val f = new IsWeekend(204, "context_is_weekend")
      f.parse(sample(10, 6))
      assert(f.raw_list.head === "2")
      assert(f.feature_list.head === 2)
    }

    "return 2 for Sunday" in {
      val f = new IsWeekend(204, "context_is_weekend")
      f.parse(sample(10, 7))
      assert(f.raw_list.head === "2")
      assert(f.feature_list.head === 2)
    }
  }

  "All context features" should {
    "have correct field type" in {
      val hour = new ContextTimeHour(201, "context_time_hour")
      val area = new ContextTimeArea(202, "context_time_area")
      val week = new ContextTimeWeek(203, "context_time_week")
      val weekend = new IsWeekend(204, "context_is_weekend")
      assert(hour.field_type === FieldType.Categorical)
      assert(area.field_type === FieldType.Categorical)
      assert(week.field_type === FieldType.Categorical)
      assert(weekend.field_type === FieldType.Categorical)
    }
  }
}
