package datagen

import org.scalatest.funsuite.AnyFunSuite

class TpcdiToDeltaTest extends AnyFunSuite {
  test("standard mode reads the official Batch2 and Batch3") {
    assert(TpcdiToDelta.SourceBatches(1, 0) == Seq(1))
    assert(TpcdiToDelta.SourceBatches(2, 0) == Seq(2))
    assert(TpcdiToDelta.SourceBatches(3, 0) == Seq(3))
  }

  test("daily mode folds N days into Batch2 and reserves the next day for Batch3") {
    assert(TpcdiToDelta.SourceBatches(1, 7) == Seq(1))
    assert(TpcdiToDelta.SourceBatches(2, 7) == (2 to 8))
    assert(TpcdiToDelta.SourceBatches(3, 7) == Seq(9))
  }

  test("negative daily windows are rejected") {
    assertThrows[IllegalArgumentException] {
      TpcdiToDelta.SourceBatches(2, -1)
    }
  }
}
