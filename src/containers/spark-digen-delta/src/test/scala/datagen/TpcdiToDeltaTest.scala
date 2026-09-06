package datagen

import java.time.LocalDate
import org.scalatest.funsuite.AnyFunSuite

class TpcdiToDeltaTest extends AnyFunSuite {
  test("augmented windows start where the Databricks workload starts") {
    assert(TpcdiToDelta.AugmentedStart == LocalDate.parse("2016-07-06"))
  }

  test("Batch 2 end is exclusive and Batch 3 starts on the following day") {
    assert(TpcdiToDelta.AugmentedEnd(37) == LocalDate.parse("2016-08-12"))
    assert(TpcdiToDelta.AugmentedEnd(183) == LocalDate.parse("2017-01-05"))
  }

  test("daily windows outside the Databricks horizon are rejected") {
    assertThrows[IllegalArgumentException] {
      TpcdiToDelta.AugmentedEnd(0)
    }
    assertThrows[IllegalArgumentException] {
      TpcdiToDelta.AugmentedEnd(366)
    }
  }
}
