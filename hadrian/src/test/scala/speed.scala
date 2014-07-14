package test.scala.speed

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class SpeedSuite extends FlatSpec with Matchers {
  "tree test" must "run a lot of data through a tree" taggedAs(Speed) in {
    val engine = PFAEngine.fromJson(getClass.getResourceAsStream("/resources/hipparcos_numerical_10.pfa")).head

    val dataset =
      (for (line <- new java.util.Scanner(getClass.getResourceAsStream("/resources/hipparcos_numerical.csv")).useDelimiter("\\n")) yield {
        val words = line.split(",")
        engine.fromJson(s"""{
    "ra": ${words(0)},
    "dec": ${words(1)},
    "dist": ${words(2)},
    "mag": ${words(3)},
    "absmag": ${words(4)},
    "x": ${words(5)},
    "y": ${words(6)},
    "z": ${words(7)},
    "vx": ${words(8)},
    "vy": ${words(9)},
    "vz": ${words(10)},
    "spectrum": "${words(11)}"
}""", engine.inputType)
      }).toList

    val datasetSize = dataset.size

    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))

    var cumulative = 0.0
    for (iteration <- 0 until 50) {
      val before = System.currentTimeMillis
      dataset.foreach(engine.action(_))
      val after = System.currentTimeMillis

      cumulative += after - before
      println(s"""${cumulative / 1000.0}, ${(iteration + 1) * datasetSize}""")
    }

  }

  "tree test" must "run a lot of data through a forest" taggedAs(Speed) in {
    println("loading engine")
    val before = System.currentTimeMillis
    val engine = PFAEngine.fromJson(new java.util.zip.GZIPInputStream(getClass.getResourceAsStream("/resources/hipparcos_segmented_10.pfa.gz"))).head
    val after = System.currentTimeMillis
    println("done", (after - before))

    val dataset =
      (for (line <- new java.util.Scanner(getClass.getResourceAsStream("/resources/hipparcos_numerical.csv")).useDelimiter("\\n")) yield {
        val words = line.split(",")
        engine.fromJson(s"""{
    "ra": ${words(0)},
    "dec": ${words(1)},
    "dist": ${words(2)},
    "mag": ${words(3)},
    "absmag": ${words(4)},
    "x": ${words(5)},
    "y": ${words(6)},
    "z": ${words(7)},
    "vx": ${words(8)},
    "vy": ${words(9)},
    "vz": ${words(10)},
    "spectrum": "${words(11)}"
}""", engine.inputType)
      }).toList

    println("loaded dataset")

    val datasetSize = dataset.size

    dataset.foreach(engine.action(_))

    println("first foreach")

    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))

    var cumulative = 0.0
    for (iteration <- 0 until 50) {
      val before = System.currentTimeMillis
      dataset.foreach(engine.action(_))
      val after = System.currentTimeMillis

      cumulative += after - before
      println(s"""${cumulative / 1000.0}, ${(iteration + 1) * datasetSize}""")
    }

  }

}
