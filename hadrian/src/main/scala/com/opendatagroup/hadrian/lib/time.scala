// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian.lib

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

package object time {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)
  val prefix = "time."

  val tsparam = <param name="ts">Number of seconds since the beginning (just after midnight) of Jan 1, 1970 C.E. in UTC.</param>
  val zoneparam = <param name="zone">Timezone name from the Olson timezone database, version 2015f (UTC if blank).</param>
  val tsdetails = <detail>The earliest expressible date is the beginning (just after midnight) of Jan 1, 1 C.E. in UTC on the proleptic Gregorian calendar, which is a timestamp of -62135596800. The latest expressible date is the end (just before midnight) of Dec 31, 9999 C.E. in UTC on the Gregorian calendar, which is a timestamp of 253402300799.</detail>
  def dateerror(code: Int) = <error code={s"${code}"}>Raises "timestamp out of range" if <p>ts</p> less than -62135596800 or greater than 253402300799.</error>
  def zoneerror(code: Int) = <error code={s"${code}"}>Raises "unrecognized timezone string" if <p>zone</p> is not in the Olson 2015f database.</error>

  def tz(zone: String, code: Int, fcnName: String, pos: Option[String]) =
    if (zone == "")
      DateTimeZone.forOffsetMillis(0)
    else
      try {
        DateTimeZone.forID(zone)
      }
      catch {
        case err: java.lang.IllegalArgumentException =>
          throw new PFARuntimeException("unrecognized timezone string", code, fcnName, pos)
      }

  def tscheck(ts: Double, code: Int, fcnName: String, pos: Option[String]): Long = {
    if (java.lang.Double.isNaN(ts)  ||  ts < -62135596800.0  ||  ts > 253402300799.0)
      throw new PFARuntimeException("timestamp out of range", code, fcnName, pos)
    Math.floor(ts * 1000.0).toLong
  }

  class Year(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "year"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the four-digit year that the timestamp falls within.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40000
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getYear
  }
  provide(new Year)

  class MonthOfYear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "monthOfYear"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the month that the timestamp falls within, with 1 being January and 12 being December.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40010
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getMonthOfYear
  }
  provide(new MonthOfYear)

  class DayOfYear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "dayOfYear"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the day of the year that the timestamp falls within, from 1 to 365 or 366 inclusive, depending on leap year.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40020
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getDayOfYear()
  }
  provide(new DayOfYear)

  class DayOfMonth(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "dayOfMonth"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the day of the month that the timestamp falls within, a number from 1 to 28, 29, 30, or 31, inclusive, depending on month.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40030
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getDayOfMonth
  }
  provide(new DayOfMonth)

  class DayOfWeek(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "dayOfWeek"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the day of the week that the timestamp falls within, with 0 being Monday and 6 being Sunday.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40040
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getDayOfWeek - 1
  }
  provide(new DayOfWeek)

  class HourOfDay(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "hourOfDay"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the hour of the day that the timestamp falls within, from 0 to 23 inclusive.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40050
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getHourOfDay
  }
  provide(new HourOfDay)

  class MinuteOfHour(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "minuteOfHour"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Get the minute of the hour that the timestamp falls within, from 0 to 59 inclusive.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40060
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getMinuteOfHour
  }
  provide(new MinuteOfHour)

  class SecondOfMinute(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "secondOfMinute"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Int)
    def doc =
    <doc>
      <desc>Get the second of the minute that the timestamp falls within, from 0 to 59 inclusive.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
    </doc>
    def errcodeBase = 40070
    def apply(ts: Double, zone: String): Int =
      new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getSecondOfMinute
  }
  provide(new SecondOfMinute)

  class MakeTimestamp(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "makeTimestamp"
    def sig = Sig(List("year" -> P.Int, "month" -> P.Int, "day" -> P.Int, "hour" -> P.Int, "minute" -> P.Int, "second" -> P.Int, "millisecond" -> P.Int, "zone" -> P.String), P.Double)
    def doc =
      <doc>
        <desc>Given the date and time that this time occurs in, return the timestamp.</desc>
        <param name="year">The four-digit year, from 1 to 9999 inclusive.</param>
        <param name="month">The month of the year, from 1 to 12 inclusive.</param>
        <param name="day">The day of the month, from 1 to 28, 29, 30, or 31 inclusive, depending on month.</param>
        <param name="hour">The hour of the day, from 0 to 23 inclusive.</param>
        <param name="minute">The minute of the hour, from 0 to 59 inclusive.</param>
        <param name="second">The second of the minute, from 0 to 59 inclusive.</param>
        <param name="millisecond">The millisecond of the second, from 0 to 999 inclusive.</param>
        {zoneparam}
        <ret>The number of seconds since the beginning (just after midnight) of Jan 1, 1970 C.E. in UTC.</ret>
        {tsdetails}{zoneerror(errcodeBase + 0)}
        <error code={s"${errcodeBase + 1}"}>Raises "timestamp undefined for given parameters" if any one (or more) of the inputs have impossible values.</error>
      </doc>
    def errcodeBase = 40080

    def apply(year: Int, month: Int, day: Int, hour: Int,  minute: Int, second: Int, millisecond: Int, zone: String): Double = {
      val timezone = tz(zone, errcodeBase + 0, name, pos)
      try{
        if (year < 1  ||  year > 9999)
          throw new Exception
        new DateTime(year, month, day, hour, minute, second, millisecond, timezone).toInstant.getMillis / 1000.0
      }
      catch {
        case e: Exception => throw new PFARuntimeException("timestamp undefined for given parameters", errcodeBase + 1, name, pos);
      }
    }
  }
  provide(new MakeTimestamp)

  class IsSecondOfMinute(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isSecondOfMinute"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified number of seconds in any minute.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum number of seconds (inclusive).</param>
        <param name="high">Maximum number of seconds (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
    </doc>
    def errcodeBase = 40090

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = {
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val second = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getSecondOfMinute
      (second >= low) && (second < high)
    }
  }
  provide(new IsSecondOfMinute)

  class IsMinuteOfHour(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isMinuteOfHour"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified number of minutes in any hour.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum number of minutes (inclusive)</param>
        <param name="high">Maximum number of minutes (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40100

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val minute = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getMinuteOfHour
      (minute >= low) && (minute < high)
    }
  }
  provide(new IsMinuteOfHour)

  class IsHourOfDay(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isHourOfDay"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified number of hours in any day.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum number of hours (inclusive).</param>
        <param name="high">Maximum number of hours (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40110

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val hour: Int = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getHourOfDay
      (hour >= low) && (hour < high)
    }
  }
  provide(new IsHourOfDay)

  class IsDayOfWeek(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isDayOfWeek"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified day of week range, with 0 being Monday and 6 being Sunday.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum day of the week (inclusive).</param>
        <param name="high">Maximum day of the week (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40120

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val day = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getDayOfWeek - 1
      (day >= low) && (day < high)
    }
  }
  provide(new IsDayOfWeek)

  class IsDayOfMonth(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isDayOfMonth"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified day of month range, with 1 being the first of the month..</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum day of the month (inclusive).</param>
        <param name="high">Maximum day of the month (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40130

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val day = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getDayOfMonth
      (day >= low) && (day < high)
    }
  }
  provide(new IsDayOfMonth)

  class IsMonthOfYear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isMonthOfYear"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified month of year range, with 1 being January and 12 being December.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum month of the year (inclusive).</param>
        <param name="high">Maximum month of the year (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40140

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val month = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getMonthOfYear
      (month >= low) && (month < high)
    }
  }
  provide(new IsMonthOfYear)

  class IsDayOfYear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isDayOfYear"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String, "low" -> P.Double, "high" -> P.Double), P.Boolean)
    def doc =
      <doc>
        <desc>Determines if a timestamp falls within a specified day of year range, with 1 being the first of the year.</desc>
        {tsparam}{zoneparam}
        <param name="low">Minimum day of year (inclusive).</param>
        <param name="high">Maximum day of year (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises "bad time range" if low <m>{"\\mathrm{low} \\geq \\mathrm{high}"}</m>.</error>
        {tsdetails}{zoneerror(errcodeBase + 1)}{dateerror(errcodeBase + 2)}
      </doc>
    def errcodeBase = 40150

    def apply(ts: Double, zone: String, low: Double, high: Double): Boolean = { 
      if (low >= high) throw new PFARuntimeException("bad time range", errcodeBase + 0, name, pos)
      val day = new DateTime(tscheck(ts, errcodeBase + 2, name, pos)).withZone(tz(zone, errcodeBase + 1, name, pos)).getDayOfYear
      (day >= low) && (day < high)
    }
  }
  provide(new IsDayOfYear)

  class IsWeekend(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isWeekend"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Boolean)
    def doc =
      <doc>
        <desc>Returns <c>true</c> if the timestamp falls on a Saturday or Sunday, <c>false</c> otherwise.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40160

    def apply(ts: Double, zone: String): Boolean = { 
      val day: Int = new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos)).getDayOfWeek() - 1
      day >= 5
    }
  }
  provide(new IsWeekend)

  class IsWorkHours(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isWorkHours"
    def sig = Sig(List("ts" -> P.Double, "zone" -> P.String), P.Boolean)
    def doc =
      <doc>
        <desc>Returns <c>true</c> if the timestamp falls between 9 am (inclusive) and 5 pm (exclusive) on Monday through Friday, otherwise <c>false</c>.</desc>
        {tsparam}{zoneparam}{tsdetails}{zoneerror(errcodeBase + 0)}{dateerror(errcodeBase + 1)}
      </doc>
    def errcodeBase = 40170

    def apply(ts: Double, zone: String): Boolean = { 
      val dt: DateTime = new DateTime(tscheck(ts, errcodeBase + 1, name, pos)).withZone(tz(zone, errcodeBase + 0, name, pos))   
      val hour: Int = dt.getHourOfDay()
      val day: Int = dt.getDayOfWeek() - 1
      (day < 5) && ((hour >= 9) && (hour < 17))
    }
  }
  provide(new IsWorkHours)

}
