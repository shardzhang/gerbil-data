package pipeline.stats

import org.scalatest.{Matchers, WordSpec}

class DataQualityTrackerTest extends WordSpec with Matchers {

  "StageQuality" should {
    "create with full stats" in {
      val sq = StageQuality("test_stage", 100L, Some(95L), Seq((1, 50), (2, 30), (3, 15)))
      assert(sq.stage === "test_stage")
      assert(sq.totalCount === 100L)
      assert(sq.validCount === Some(95L))
      assert(sq.targetDistribution.size === 3)
    }

    "create with count only" in {
      val sq = StageQuality("count_only", 200L)
      assert(sq.totalCount === 200L)
      assert(sq.validCount === None)
      assert(sq.targetDistribution === Seq.empty)
    }
  }

  "DataQualityTracker" should {
    "record stages with full stats" in {
      val tracker = new DataQualityTracker()
      tracker.record("train_stats", 1000L, 980L, Seq((0, 500), (1, 480)))

      val reports = tracker.reports
      assert(reports.size === 1)
      assert(reports.head.stage === "train_stats")
      assert(reports.head.totalCount === 1000L)
      assert(reports.head.validCount === Some(980L))
    }

    "record stages with count only" in {
      val tracker = new DataQualityTracker()
      tracker.recordCounts("val_split", 500L)

      assert(tracker.reports.size === 1)
      assert(tracker.reports.head.stage === "val_split")
      assert(tracker.reports.head.validCount === None)
    }

    "accumulate multiple stages in order" in {
      val tracker = new DataQualityTracker()
      tracker.recordCounts("stage_a", 100L)
      tracker.record("stage_b", 200L, 180L, Seq((1, 90), (2, 90)))
      tracker.recordCounts("stage_c", 150L)

      val reports = tracker.reports
      assert(reports.size === 3)
      assert(reports(0).stage === "stage_a")
      assert(reports(1).stage === "stage_b")
      assert(reports(2).stage === "stage_c")
    }

    "handle empty target distribution" in {
      val tracker = new DataQualityTracker()
      tracker.record("empty_stage", 100L, 100L, Seq.empty)

      assert(tracker.reports.head.targetDistribution === Seq.empty)
    }

    "handle multiple calls for same stage" in {
      val tracker = new DataQualityTracker()
      tracker.record("stats", 100L, 90L, Seq((0, 50)))
      tracker.record("stats", 200L, 190L, Seq((1, 100)))

      assert(tracker.reports.size === 2)
      assert(tracker.reports.map(_.stage) === Seq("stats", "stats"))
    }

    "print report without throwing" in {
      val tracker = new DataQualityTracker()
      tracker.record("train_stats", 1000L, 950L, Seq((0, 500), (1, 450)))
      tracker.recordCounts("val_split", 200L)

      // Should not throw
      tracker.printReport()
    }
  }
}
