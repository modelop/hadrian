package test.scala.lib1.prob.dist

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1ProbDistSuite extends FlatSpec with Matchers {
  "normal distribution" must "have the right log likelihoods" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
""").head

    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.612 +- 0.01)
    engine.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (-2.112 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-14.11 +- 0.01)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (-4.737 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-2.112 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 2.0]
""").head

    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.612 +- 0.01)
    engine2.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (-2.112 +- 0.01)
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-14.11 +- 0.01)
    engine2.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (-4.737 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-2.112 +- 0.01)

  }
}
