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

package com.opendatagroup.hadrian.lib1

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion

import scala.math.round
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.LocalDateTime


package object time {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)
  val prefix = "time."
///////////////////////////////////////////// Year /////////////////////// 
////   year (Year)
  object Year extends LibFcn with Function1[Double, Int] {
    val name = prefix + "year"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the year that the timestamp falls within.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
    return dt.getYear()
    }
  }
  provide(Year)

///////////////////////////////////////////// MonthOfYear /////////////////////// 
////   monthOfYear (MonthOfYear)
  object MonthOfYear extends LibFcn with Function1[Double, Int] {
    val name = prefix + "monthOfYear"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the month that the timestamp falls within.  Timestamp is assumed to be in UTC.  January equals 1, December equals 12.</desc>
    <param name="ts">Timestamp.</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      return dt.getMonthOfYear()
    }
  }
  provide(MonthOfYear)


///////////////////////////////////////////// DayOfYear /////////////////////// 
////   dayOfYear (DayOfYear)
  object DayOfYear extends LibFcn with Function1[Double, Int] {
    val name = prefix + "dayOfYear"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the day of the year that the timestamp falls within.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      return dt.getDayOfYear() + 1
    }
  }
  provide(DayOfYear)


///////////////////////////////////////////// DayOfMonth /////////////////////// 
////   dayOfMonth (DayOfMonth)
  object DayOfMonth extends LibFcn with Function1[Double, Int] {
    val name = prefix + "dayOfMonth"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the day of the month that the timestamp falls within.  Timestamp is assumed to be in UTC.  The first day of the month equals 1.</desc>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      return dt.getDayOfMonth()
    }
  }
  provide(DayOfMonth)


///////////////////////////////////////////// DayOfWeek /////////////////////// 
////   dayOfWeek (DayOfWeek)
  object DayOfWeek extends LibFcn with Function1[Double, Int] {
    val name = prefix + "dayOfWeek"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the day of the week that the timestamp falls within.  Timestamp is assumed to be in UTC.  Monday equals 0, Sunday = 6.</desc>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   

      return dt.getDayOfWeek() - 1
    }
  }
  provide(DayOfWeek)

///////////////////////////////////////////// HourOfDay /////////////////////// 
////   hourOfDay (HourOfDay)
  object HourOfDay extends LibFcn with Function1[Double, Int] {
    val name = prefix + "hourOfDay"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the hour of the day that the timestamp falls within.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   

      return dt.getHourOfDay()
    }
  }
  provide(HourOfDay)

///////////////////////////////////////////// MinuteOfHour /////////////////////// 
////   minuteOfHour (MinuteOfHour)
  object MinuteOfHour extends LibFcn with Function1[Double, Int] {
    val name = prefix + "minuteOfHour"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the minute of the hour that the timestamp falls within.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      return dt.getMinuteOfHour()
    }
  }
  provide(MinuteOfHour)

///////////////////////////////////////////// SecondOfMinute /////////////////////// 
////   secondOfMinute (SecondOfMinute)
  object SecondOfMinute extends LibFcn with Function1[Double, Int] {
    val name = prefix + "secondOfMinute"
    val sig = Sig(List("ts" -> P.Double), P.Int)
    val doc =
    <doc>
    <desc>Return the second of the minute that the timestamp falls within.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    </doc>
    def apply(ts: Double): Int = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      return dt.getSecondOfMinute()
    }
  }
  provide(SecondOfMinute)

///////////////////////////////////////////// MakeTimestamp /////////////////////// 
////   makeTimestamp (MakeTimestamp)
  object MakeTimestamp extends LibFcn with Function7[Int, Int, Int, Int, Int, Int, Int, Double] {
    val name = prefix + "makeTimestamp"
    val sig = Sig(List("year" -> P.Int, "month" -> P.Int, "day" -> P.Int, "hour" -> P.Int, 
                       "minute" -> P.Int, "second" -> P.Int, "millisecond" -> P.Int), P.Double)
    val doc =
    <doc>
    <desc>Given the date and time in UTC that this time occurs in, return the timestamp in UTC of this date and time.</desc>
    <param name="year">The year, e.g. 2015.</param>
    <param name="month">The month of the year.</param>
    <param name="day">The day of the month.</param>
    <param name="hour">The hour of the day.</param>
    <param name="minute">The minute of the hour.</param>
    <param name="second">The second of the minute.</param>
    <param name="millisecond">The millisecond of the second.</param>
    <error>Raises "timestamp undefined for given parameters" if any one (or more) of the inputs have impossible values.</error>
    </doc>

    def apply(year: Int, month: Int, day: Int, hour: Int,  minute: Int, second: Int, millisecond: Int): Double = {
      try{
        val dt: DateTime = new LocalDateTime(year, month, day, hour, minute, second, millisecond).toDateTime(DateTimeZone.UTC)
        return dt.getMillis().toDouble/1000.0
      } catch {
        case e: Exception => throw new PFARuntimeException("timestamp undefined for given parameters");
      }
    }
  }
  provide(MakeTimestamp)


///////////////////////////////////////////// IsSecondOfMinute /////////////////////// 
////   IsSecondOfMinute (IsSecondOfMinute)
  object IsSecondOfMinute extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isSecondOfMinute"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of seconds of the minute.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val second: Int = dt.getSecondOfMinute()
      if ((second >= low) && (second < high))
        return true
      else 
        return false
    }
  }
  provide(IsSecondOfMinute)


///////////////////////////////////////////// IsMinuteOfHour /////////////////////// 
////   IsMinuteOfHour (IsMinuteOfHour)
  object IsMinuteOfHour extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isMinuteOfHour"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of minutes of the hour.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val minute: Int = dt.getMinuteOfHour()
      if ((minute >= low) && (minute < high))
        return true
      else 
        return false
    }
  }
  provide(IsMinuteOfHour)


///////////////////////////////////////////// IsHourOfDay /////////////////////// 
////   IsHourOfDay (IsHourOfDay)
  object IsHourOfDay extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isHourOfDay"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of hours of the day.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val hour: Int = dt.getHourOfDay()
      if ((hour >= low) && (hour < high))
        return true
      else 
        return false
    }
  }
  provide(IsHourOfDay)


///////////////////////////////////////////// IsDayOfWeek /////////////////////// 
////   IsDayOfWeek (IsDayOfWeek)
  object IsDayOfWeek extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isDayOfWeek"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of days of the week.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val day: Int = dt.getDayOfWeek() - 1
      if ((day >= low) && (day < high))
        return true
      else 
        return false
    }
  }
  provide(IsDayOfWeek)


///////////////////////////////////////////// IsDayOfMonth /////////////////////// 
////   IsDayOfMonth (IsDayOfMonth)
  object IsDayOfMonth extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isDayOfMonth"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of days of the month.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val day: Int = dt.getDayOfMonth()
      if ((day >= low) && (day < high))
        return true
      else 
        return false
    }
  }
  provide(IsDayOfMonth)


///////////////////////////////////////////// IsMonthOfYear /////////////////////// 
////   IsMonthOfYear (IsMonthOfYear)
  object IsMonthOfYear extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isMonthOfYear"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of months of the year.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val month: Int = dt.getMonthOfYear()
      if ((month >= low) && (month < high))
        return true
      else 
        return false
    }
  }
  provide(IsMonthOfYear)


///////////////////////////////////////////// IsDayOfYear /////////////////////// 
////   IsDayOfYear (IsDayOfYear)
  object IsDayOfYear extends LibFcn with Function3[Double, Double, Double, Boolean] {
    val name = prefix + "isDayOfYear"
    val sig = Sig(List("ts" -> P.Double, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Checks if a timestamp falls within a time range, specified in terms of days of the year.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double, low: Double, high: Double): Boolean = { 
      if (low >= high)
          throw new PFARuntimeException("bad time range")
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val day: Int = dt.getDayOfYear()
      if ((day >= low) && (day < high))
        return true
      else 
        return false
    }
  }
  provide(IsDayOfYear)


///////////////////////////////////////////// IsWeekend /////////////////////// 
////   IsWeekend (IsWeekend)
  object IsWeekend extends LibFcn with Function1[Double, Boolean] {
    val name = prefix + "isWeekend"
    val sig = Sig(List("ts" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Returns True if the timestamp falls on a Saturday or Sunday, False otherwise.  Timestamp is assumed to be in UTC.</desc>
    <param name="ts">Timestamp.</param>
    <param name="low">Lower boundary of the range.</param>
    <param name="high">Upper boundary of the range.</param>
    <error>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
    </doc>

    def apply(ts: Double): Boolean = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val day: Int = dt.getDayOfWeek() - 1
      if (day >= 5)
        return true
      else 
        return false
    }
  }
  provide(IsWeekend)


///////////////////////////////////////////// IsWorkHours /////////////////////// 
////   IsWorkHours (IsWorkHours)
  object IsWorkHours extends LibFcn with Function1[Double, Boolean] {
    val name = prefix + "isWorkHours"
    val sig = Sig(List("ts" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Returns True if the timestamp falls between 9AM (inclusive) and 5PM (exclusive) on Monday through Friday, otherwise False.  Timestamp is assumed to be in UTC.</desc>
    </doc>

    def apply(ts: Double): Boolean = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val hour: Int = dt.getHourOfDay()
      val day: Int = dt.getDayOfWeek() - 1
      if ((day < 5) && ((hour >= 9) && (hour < 17)))
        return true
      else 
        return false
    }
  }
  provide(IsWorkHours)


///////////////////////////////////////////// IsNonWorkHours /////////////////////// 
////   IsNonWorkHours (IsNonWorkHours)
  object IsNonWorkHours extends LibFcn with Function1[Double, Boolean] {
    val name = prefix + "isNonWorkHours"
    val sig = Sig(List("ts" -> P.Double), P.Boolean)
    val doc =
    <doc>
    <desc>Returns True if the timestamp falls before 9AM (exclusive) or after 5PM (inclusive) on Monday through Friday, otherwise False.  Also returns false if the timestamp falls at any time on weekend days.  Timestamp is assumed to be in UTC.</desc>
    </doc>

    def apply(ts: Double): Boolean = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))   
      val hour: Int = dt.getHourOfDay()
      val day: Int = dt.getDayOfWeek() - 1
      if ((day < 5) && ((hour >= 9) && (hour < 17)))
        return false
      else 
        return true
    }
  }
  provide(IsNonWorkHours)


///////////////////////////////////////////// FromUTCToLocal /////////////////////// 
////   FromUTCToLocal (FromUTCToLocal)
  object FromUTCToLocal extends LibFcn with Function2[Double, Double, Double] {
    val name = prefix + "fromUTCToLocal"
    val sig = Sig(List("ts" -> P.Double, "offset" -> P.Double), P.Double)
    val doc =
    <doc>
    <desc>Converts a timestamp specified in the local time via an offset from UTC to UTC.</desc>
    <param name="ts">Timestamp in UTC.</param>
    <param name="offset">The UTC offset in the local time.</param>
    <ret>A timestamp in the local time.</ret>
    </doc>

    def apply(ts: Double, offset: Double): Double = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))
      return dt.plusMillis(round(offset*3600000.0).toInt).getMillis().toDouble/1000 
    }
  }
  provide(FromUTCToLocal)


///////////////////////////////////////////// FromLocalToUTC /////////////////////// 
////   FromLocalToUTC (FromLocalToUTC)
  object FromLocalToUTC extends LibFcn with Function2[Double, Double, Double] {
    val name = prefix + "fromLocalToUTC"
    val sig = Sig(List("ts" -> P.Double, "offset" -> P.Double), P.Double)
    val doc =
    <doc>
    <desc>Converts a timestamp specified in UTC via an offset in the local time to the local time.</desc>
    <param name="ts">Timestamp in the local time.</param>
    <param name="offset">The offset from UTC in the local time.</param>
    <ret>A timestamp in UTC.</ret>
    </doc>

    def apply(ts: Double, offset: Double): Double = { 
      val dt: DateTime = new DateTime(ts.toLong*1000L).withZone(DateTimeZone.forOffsetMillis(0))
      return dt.minusMillis(round(offset*3600000.0).toInt).getMillis().toDouble/1000
    }
  }
  provide(FromLocalToUTC)
}
