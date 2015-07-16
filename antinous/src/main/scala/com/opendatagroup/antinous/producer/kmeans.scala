// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// 
// Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.antinous.producer

import java.lang.management.ManagementFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

import org.python.core.PyInteger
import org.python.core.PyFloat
import org.python.core.PyFunction
import org.python.core.PyList

import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField

package kmeans {
  object Cluster {
    def avroType = AvroRecord(Seq(AvroField("center", AvroArray(AvroDouble())), AvroField("weight", AvroDouble())), "Cluster")
  }
  case class Cluster(center: java.util.List[Double], weight: Double) extends ModelRecord {
    def pfa = JsonObject("center" -> JsonArray(center: _*), "weight" -> weight)
    def avroType = Cluster.avroType
  }

  object ClusterSet {
    def avroType = AvroArray(Cluster.avroType)
  }
  case class ClusterSet(clusters: java.util.List[Cluster]) extends Model {
    def pfa = JsonArray(clusters map {_.pfa}: _*)
    def avroType = ClusterSet.avroType
  }

  class VectorSet extends Dataset {
    private class Datum(val pos: Array[Double]) {
      override def equals(other: Any): Boolean = other match {
        case that: Datum =>
          this.pos sameElements that.pos
        case _ => false
      }
      val _hashCode = pos.toSeq.hashCode
      override def hashCode(): Int = _hashCode
    }

    private var _dimension = 0
    private var _numberOfDataPoints = 0
    private var _numberOfUniquePoints = 0
    private var _counter = 0
    def dimension = _dimension
    def numberOfDataPoints = _numberOfDataPoints
    def numberOfUniquePoints = _numberOfUniquePoints

    private var posToWeight: mutable.Map[Datum, Double] = null
    private var reservoir: Array[Datum] = null
    private var posArray: Array[Array[Double]] = null
    private var weightArray: Array[Double] = null

    def revert() {
      _dimension = 0
      _numberOfDataPoints = 0
      _numberOfUniquePoints = 0
      _counter = 0
      posToWeight = mutable.HashMap[Datum, Double]()
      reservoir = null
      posArray= null
      weightArray = null
    }
    revert()

    val GB_PER_BYTE = 1.0 / 1024.0 / 1024.0 / 1024.0
    def heapMemoryGB = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed * GB_PER_BYTE

    private var _maxNumberOfUniquePoints = -1
    private var _maxHeapMemoryGB = -1.0
    def maxNumberOfUniquePoints = _maxNumberOfUniquePoints
    def maxHeapMemoryGB = _maxHeapMemoryGB
    def setMaxNumberOfUniquePoints(n: Int) {_maxNumberOfUniquePoints = n}
    def setMaxHeapMemoryGB(n: Double) {_maxHeapMemoryGB = n}

    def isFull =
      if (maxNumberOfUniquePoints >= 0  &&  numberOfUniquePoints >= maxNumberOfUniquePoints)
        true
      else if (maxHeapMemoryGB >= 0.0  &&  heapMemoryGB >= maxHeapMemoryGB)
        true
      else
        false

    private def actuallyAdd(datum: Datum, weight: Double) {
      _numberOfDataPoints += 1
      posToWeight.get(datum) match {
        case None =>
          posToWeight(datum) = weight
          _numberOfUniquePoints += 1
          _counter += 1
        case Some(x) =>
          posToWeight(datum) = x + weight
      }
    }

    private def maybeAdd(datum: Datum, weight: Double) {
      if (isFull  &&  !posToWeight.contains(datum)) {
        if (reservoir == null)
          reservoir = posToWeight.keys.toArray

        val index = random.nextInt(_counter + 1)
        if (index < reservoir.size) {
          val removeMe = reservoir(index)
          posToWeight.remove(removeMe)
          _numberOfDataPoints -= 1     // we have just removed a unique point
          _numberOfUniquePoints -= 1

          actuallyAdd(datum, weight)   // increments _counter because datum is unique
          reservoir(index) = datum
        }
        else
          _counter += 1                // increments _counter: note that datum is unique
      }
      else
        actuallyAdd(datum, weight)     // increments _counter if datum is unique
    }

    def add(pos: Array[Double], weight: Double) {
      if (posToWeight == null)
        throw new UnsupportedOperationException("a VectorSet can only be filled before it is assigned to a KMeans object")

      if (_numberOfDataPoints == 0)
        _dimension = pos.size
      if (pos.size != _dimension)
        throw new IllegalArgumentException(s"dimension of data points disagree (datum ${numberOfDataPoints} has dimension ${pos.size}, but all previous have dimension ${dimension})")
      if (pos exists {x => java.lang.Double.isNaN(x)  ||  java.lang.Double.isInfinite(x)})
        throw new IllegalArgumentException(s"datum ${numberOfDataPoints} contains a non-finite number")
      
      if (weight > 0)
        maybeAdd(new Datum(pos), weight)
    }

    def add(pos: Array[Double]): Unit = add(pos, 1.0)
    def add(pos: java.lang.Iterable[Double], weight: Double): Unit = add(pos.toArray, weight)
    def add(pos: java.lang.Iterable[Double]): Unit = add(pos.toArray, 1.0)
    def add(pos: Iterable[Double], weight: Double): Unit = add(pos.toArray, weight)
    def add(pos: Iterable[Double]): Unit = add(pos.toArray, 1.0)

    private[kmeans] def toArrays = {
      if (posArray == null  ||  weightArray == null) {
        posArray = Array.fill[Array[Double]](numberOfUniquePoints)(null)
        weightArray = Array.fill[Double](numberOfUniquePoints)(0.0)

        posToWeight.zipWithIndex foreach {case ((k, v), i) =>
          posArray(i) = k.pos
          weightArray(i) = v
        }

        posToWeight = null
        reservoir = null
      }
      (posArray, weightArray)
    }

    def getPos(i: Int): java.util.List[Double] =
      if (posArray == null)
        throw new UnsupportedOperationException("a VectorSet can only be queried after it is assigned to a KMeans object")
      else
        seqAsJavaList(posArray(i))

    def getWeight(i: Int): Double =
      if (weightArray == null)
        throw new UnsupportedOperationException("a VectorSet can only be queried after it is assigned to a KMeans object")
      else
        weightArray(i)
  }

  trait Metric extends Function2[Array[Double], Array[Double], Double]

  object Euclidean extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double = Math.sqrt(SquaredEuclidean(x, y))
  }

  object SquaredEuclidean extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double = {
      var i = 0
      var sum = 0.0
      while (i < x.size) {
        val diff = x(i) - y(i)
        sum += diff * diff
        i += 1
      }
      sum
    }
  }

  object Chebyshev extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double =
      x zip y map {case (xi, yi) => Math.abs(xi - yi)} max
  }

  object Taxicab extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double =
      x zip y map {case (xi, yi) => Math.abs(xi - yi)} sum
  }

  class Minkowski(p: Double) extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double =
      Math.pow(x zip y map {case (xi, yi) => Math.pow(Math.abs(xi - yi), p)} sum, 1.0/p)
  }

  class M(f: PyFunction) extends Metric {
    def apply(x: Array[Double], y: Array[Double]): Double =
      f.__call__(new PyList(x.iterator map {new PyFloat(_)}), new PyList(y.iterator map {new PyFloat(_)})).asInstanceOf[PyFloat].asDouble
  }

  trait StoppingCondition extends Function3[Int, ClusterSet, Array[Array[Double]], Boolean]

  class MaxIterations(max: Int) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean =
      iterationNumber >= max
  }

  class Moving extends BelowThreshold(1e-15)

  class BelowThreshold(threshold: Double) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean =
      changes forall {dx => dx.map(Math.abs).sum < threshold}
  }

  class HalfBelowThreshold(threshold: Double) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean =
      (changes count {dx => dx.map(Math.abs).sum < threshold}) >= (0.5 * changes.size)
  }

  class WhenAll(conditions: java.lang.Iterable[StoppingCondition]) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean =
      conditions forall {c => c(iterationNumber, clusterSet, changes)}
  }

  class WhenAny(conditions: java.lang.Iterable[StoppingCondition]) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean =
      conditions exists {c => c(iterationNumber, clusterSet, changes)}
  }

  class PrintValue(numberFormat: String = "%g") extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean = {
      var first = true
      clusterSet.clusters foreach {cluster =>
        if (first)
          print("%-5d".format(iterationNumber))
        else
          print("     ")
        first = false

        cluster.center foreach {x => print(" " + numberFormat.format(x))}
        println()
      }
      false
    }
  }

  class PrintChange(numberFormat: String = "%g") extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean = {
      var first = true
      changes foreach {change =>
        if (first)
          print("%-5d".format(iterationNumber))
        else
          print("     ")
        first = false

        change foreach {x => print(" " + numberFormat.format(x))}
        println()
      }
      false
    }
  }

  class S(f: PyFunction) extends StoppingCondition {
    def apply(iterationNumber: Int, clusterSet: ClusterSet, changes: Array[Array[Double]]): Boolean = {
      f.__call__(
        new PyInteger(iterationNumber),
        new PyList(clusterSet.clusters.iterator map {x => new PyList(x.center.iterator map {new PyFloat(_)})}),
        new PyList(changes.iterator map {x => new PyList(x.iterator map {new PyFloat(_)})})).__nonzero__
    }
  }

  class KMeans(val numberOfClusters: Int, val dataset: VectorSet) extends Producer[VectorSet, ClusterSet] {
    private val (pos, weight) = dataset.toArrays

    if (dataset.numberOfUniquePoints == 0)
      throw new IllegalArgumentException("number of data points must be greater than zero")
    if (numberOfClusters > dataset.numberOfUniquePoints)
      throw new IllegalArgumentException(s"requested number of clusters is $numberOfClusters but dataset has only $dataset.numberOfUniquePoints unique points")
    if (dataset.dimension == 0)
      throw new IllegalArgumentException("dataset.dimension of data points must be greater than zero")

    var _model: ClusterSet = null
    randomClusters()
    def model = _model

    var _metric: Metric = Euclidean
    def metric = _metric
    def setMetric(m: Metric) {_metric = m}

    var _stoppingCondition: StoppingCondition = new Moving
    def stoppingCondition = _stoppingCondition
    def setStoppingCondition(s: StoppingCondition) {_stoppingCondition = s}

    def randomClusters() {
      val out = mutable.Set[Cluster]()
      def select = Cluster(seqAsJavaList(pos(random.nextInt(dataset.numberOfUniquePoints))), 0)
      0 until numberOfClusters map {i =>
        var trial = select
        while (out.contains(trial))
          trial = select
        out.add(trial)
      }
      _model = ClusterSet(seqAsJavaList(out.toSeq))
    }
    
    def optimize(subsampleSize: Int) {
      val (subpos, subweight) =
        if (subsampleSize > dataset.numberOfUniquePoints)
          throw new IllegalArgumentException(s"cannot select $subsampleSize from a set of ${dataset.numberOfUniquePoints} unique points")
        else if (subsampleSize < numberOfClusters)
          throw new IllegalArgumentException(s"cannot optimize $numberOfClusters using only $subsampleSize unique points")
        else if (subsampleSize == dataset.numberOfUniquePoints)
          (pos, weight)
        else {
          // reservoir sampling
          val sp = pos.take(subsampleSize)
          val sw = weight.take(subsampleSize)
          subsampleSize until dataset.numberOfUniquePoints foreach {i =>
            val j = random.nextInt(i)
            if (j < subsampleSize) {
              sp(j) = pos(i)
              sw(j) = weight(i)
            }
          }
          (sp, sw)
        }

      val numer = Array.fill(numberOfClusters)(Array.fill(dataset.dimension)(0.0))
      val denom = Array.fill(numberOfClusters)(0.0)
      val changes = Array.fill(numberOfClusters)(Array.fill(dataset.dimension)(0.0))

      val centersAndIndexes = Vector.fill(numberOfClusters)(Array.fill(dataset.dimension)(0.0)).zipWithIndex

      var iterationNumber = 0
      while (iterationNumber == 0  ||  !stoppingCondition(iterationNumber, model, changes)) {
        for (i <- 0 until numberOfClusters; j <- 0 until dataset.dimension)
          (centersAndIndexes(i)._1)(j) = model.clusters(i).center(j)

        for (i <- 0 until numberOfClusters) {
          for (j <- 0 until dataset.dimension)
            numer(i)(j) = 0.0
          denom(i) = 0.0
        }

        var i = 0
        while (i < subsampleSize) {
          val clusterIndex = centersAndIndexes.minBy({case (center, clusterIndex) => metric(center, subpos(i))})._2

          var j = 0
          while (j < dataset.dimension) {
            numer(clusterIndex)(j) += subweight(i) * (subpos(i)(j) - centersAndIndexes(clusterIndex)._1(j))
            denom(clusterIndex) += subweight(i)
            j += 1
          }

          i += 1
        }

        for (i <- 0 until numberOfClusters; j <- 0 until dataset.dimension)
          changes(i)(j) = numer(i)(j) / denom(i)

        val newClusters = centersAndIndexes map {case (cluster, clusterIndex) =>
          Cluster(seqAsJavaList(cluster zip changes(clusterIndex) map {case (x, delta) => x + delta}), denom(clusterIndex))
        }

        _model = ClusterSet(seqAsJavaList(newClusters))

        iterationNumber += 1
      }
    }

    def optimize() {
      optimize(dataset.numberOfUniquePoints)
    }
  }

}
