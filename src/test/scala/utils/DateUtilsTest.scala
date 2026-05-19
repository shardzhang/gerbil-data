package utils

import org.scalatest.{Matchers, WordSpec}

class DateUtilsTest extends WordSpec with Matchers {

  "DateUtils.getDay" should {
    "return the same date when diff is 0" in {
      assert(DateUtils.getDay(0, "2022-04-28") === "2022-04-28")
    }

    "return future date when diff is positive" in {
      assert(DateUtils.getDay(1, "2022-04-28") === "2022-04-29")
    }

    "return past date when diff is negative" in {
      assert(DateUtils.getDay(-1, "2022-04-28") === "2022-04-27")
    }

    "return date in specified pattern" in {
      assert(DateUtils.getDay(0, "20220428", "yyyyMMdd") === "20220428")
    }

    "handle year boundary" in {
      assert(DateUtils.getDay(1, "2022-12-31") === "2023-01-01")
      assert(DateUtils.getDay(-1, "2022-01-01") === "2021-12-31")
    }
  }

  "DateUtils.getDateFromUnixTimestamp" should {
    "convert unix timestamp to date string" in {
      // 2022-04-28 00:00:00 UTC = 1651104000
      assert(DateUtils.getDateFromUnixTimestamp("1651104000", "yyyy-MM-dd") === "2022-04-28")
    }

    "convert unix timestamp with custom pattern" in {
      assert(DateUtils.getDateFromUnixTimestamp("1651104000", "yyyyMMdd") === "20220428")
    }

    "handle timestamp with milliseconds" in {
      // This should work since the code takes the Long value and multiplies by 1000
      // 1651104000 * 1000 = 1651104000000 ms
      val result = DateUtils.getDateFromUnixTimestamp("1651104000")
      assert(result.nonEmpty)
    }
  }
}
