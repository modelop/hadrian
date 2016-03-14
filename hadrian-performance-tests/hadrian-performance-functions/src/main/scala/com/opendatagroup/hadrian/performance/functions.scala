package com.opendatagroup.hadrian.performance

import scala.collection.JavaConversions._
import scala.language.postfixOps

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.errors.PFARuntimeException

package functions {
  object Main {
    val warmupSamples = 30    // number of samples to repeat at the beginning before counting anything
    val numberOfWarmups = 9   // number of cycles to spend warming up on one sample (9 milliseconds)
    val nsToSpend = 1000000L  // minimum time to spend measuring the function (1 millisecond)

    def doTests(reader: PFATestsReader) {
      while (reader.hasNext) {
        val TestFunction(function, engine, samples) = reader.next

        samples.zipWithIndex foreach {case (sample, sampleIndex) =>
          val sampleInput = sample.input

          // To keep the garbage collector from interfering with any measurements,
          // force garbage collection JUST BEFORE it would happen anyway (with -Xmx8g -Xmn8g)
          // and wait 5 seconds (longer than garbage collection would EVER take).
          if (java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed >
            5.0/8.0 * java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax ) {
            System.gc()
            Thread.sleep(5000)
          }

          val nsToSpendWarming = numberOfWarmups * nsToSpend
          var startTime = System.nanoTime
          var repetitions = 0L
          var warming = true
          var now = startTime

          sample match {
            case x: SampleSuccess =>
              while (warming  ||  now - startTime < nsToSpend) {
                if (warming  &&  now - startTime >= nsToSpendWarming) {
                  startTime = now
                  repetitions = 0L
                  warming = false
                }
                engine.action(sampleInput)
                repetitions += 1L
                now = System.nanoTime
              }

            case x: SampleFailure =>
              while (warming  ||  now - startTime < nsToSpend) {
                if (warming  &&  now - startTime >= nsToSpendWarming) {
                  startTime = now
                  repetitions = 0L
                  warming = false
                }
                try {
                  engine.action(sampleInput)
                }
                catch {
                  case _: PFARuntimeException =>
                }
                repetitions += 1L
                now = System.nanoTime
              }
          }

          val timeInPFA = (System.nanoTime - startTime) * 1e-9 / repetitions

          if (sampleIndex >= warmupSamples)
            sample match {
              case SampleSuccess(sampleInput, jsonInput, value) =>
                println(s"$function\t${engine.inputType.toJson}\t$jsonInput\tsuccess\t$value\t$timeInPFA\t$repetitions")

              case SampleFailure(sampleInput, jsonInput, code) =>
                println(s"$function\t${engine.inputType.toJson}\t$jsonInput\tfailure\t$code\t$timeInPFA\t$repetitions")
            }
        }
      }
    }

    def main(args: Array[String]) {
      // doTests(new PFATestsReader(new java.io.File("baseline-functions.json")), false)    // yet more warm-up
      doTests(new PFATestsReader(new java.io.File("baseline-functions.json")))           // baseline to subtract
      doTests(new PFATestsReader(new java.io.File("../../../pfa-specification/conformance-tests/pfa-tests.json")))

      // doTests(new PFATestsReader(new java.io.File("tmp-tests.json")))
    }
  }
}
