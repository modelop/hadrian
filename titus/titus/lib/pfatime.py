#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Hadrian.
#
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import datetime

import pytz

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, negativeIndex, startEnd
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn
prefix = "time."

class UTC(datetime.tzinfo):
    def utcoffset(setlf, dt):
        return datetime.timedelta(0)
    def tzname(self, dt):
        return "UTC"
    def dst(self, td):
        return datetime.timedelta(0)

def tz(dt, zone, code, name, pos):
    if zone == "":
        return dt
    else:
        try:
            return dt.astimezone(pytz.timezone(zone))
        except pytz.exceptions.UnknownTimeZoneError:
            raise PFARuntimeException("unrecognized timezone string", code, name, pos)

def tscheck(ts, code, name, pos):
    if math.isnan(ts) or ts < -62135596800 or ts > 253402300799:
        raise PFARuntimeException("timestamp out of range", code, name, pos)
    return ts

class Year(LibFcn):
    name = prefix + "year"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40000
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).year
provide(Year())

class MonthOfYear(LibFcn):
    name = prefix + "monthOfYear"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40010
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).month
provide(MonthOfYear())

class DayOfYear(LibFcn):
    name = prefix + "dayOfYear"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40020
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).timetuple().tm_yday
provide(DayOfYear())

class DayOfMonth(LibFcn):
    name = prefix + "dayOfMonth"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40030
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).day
provide(DayOfMonth())

class DayOfWeek(LibFcn):
    name = prefix + "dayOfWeek"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40040
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).weekday()
provide(DayOfWeek())

class HourOfDay(LibFcn):
    name = prefix + "hourOfDay"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40050
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).hour
provide(HourOfDay())

class MinuteOfHour(LibFcn):
    name = prefix + "minuteOfHour"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40060
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).minute
provide(MinuteOfHour())

class SecondOfMinute(LibFcn):
    name = prefix + "secondOfMinute"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Int())
    errcodeBase = 40070
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        return tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).second
provide(SecondOfMinute())

class MakeTimestamp(LibFcn):
    name = prefix + "makeTimestamp"
    sig = Sig([{"year": P.Int()}, {"month": P.Int()}, {"day": P.Int()}, {"hour": P.Int()}, {"minute": P.Int()}, {"second": P.Int()}, {"millisecond": P.Int()}, {"zone": P.String()}], P.Int())
    epoch = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, UTC())
    errcodeBase = 40080
    def __call__(self, state, scope, pos, paramTypes, year, month, day, hour, minute, second, millisecond, zone):
        try:
            microsecond = int(round(millisecond*1000.0))
            if zone == "":
                dt = datetime.datetime(year, month, day, hour, minute, second, microsecond, UTC())
            else:
                dt = pytz.timezone(zone).localize(datetime.datetime(year, month, day, hour, minute, second, microsecond))
        except pytz.exceptions.UnknownTimeZoneError:
            raise PFARuntimeException("unrecognized timezone string", self.errcodeBase + 0, self.name, pos)
        except ValueError:
            raise PFARuntimeException("timestamp undefined for given parameters", self.errcodeBase + 1, self.name, pos)
        else:
            return (dt.astimezone(UTC()) - self.epoch).total_seconds()
provide(MakeTimestamp())

class IsSecondOfMinute(LibFcn):
    name = prefix + "isSecondOfMinute"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40090
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        second = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).second
        return second >= low and second < high
provide(IsSecondOfMinute())

class IsMinuteOfHour(LibFcn):
    name = prefix + "isMinuteOfHour"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40100
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        minute = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).minute
        return minute >= low and minute < high
provide(IsMinuteOfHour())

class IsHourOfDay(LibFcn):
    name = prefix + "isHourOfDay"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40110
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        hour = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).hour
        return hour >= low and hour < high
provide(IsHourOfDay())

class IsDayOfWeek(LibFcn):
    name = prefix + "isDayOfWeek"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40120
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        day = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).weekday()
        return day >= low and day < high
provide(IsDayOfWeek())

class IsDayOfMonth(LibFcn):
    name = prefix + "isDayOfMonth"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40130
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        day = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).day
        return day >= low and day < high
provide(IsDayOfMonth())

class IsMonthOfYear(LibFcn):
    name = prefix + "isMonthOfYear"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40140
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        month = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).month
        return month >= low and month < high
provide(IsMonthOfYear())

class IsDayOfYear(LibFcn):
    name = prefix + "isDayOfYear"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}, {"low": P.Double()}, {"high": P.Double()}], P.Boolean())
    errcodeBase = 40150
    def __call__(self, state, scope, pos, paramTypes, ts, zone, low, high):
        if (low >= high):
            raise PFARuntimeException("bad time range", self.errcodeBase + 0, self.name, pos)
        day = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 2, self.name, pos), UTC()), zone, self.errcodeBase + 1, self.name, pos).timetuple().tm_yday
        return day >= low and day < high
provide(IsDayOfYear())

class IsWeekend(LibFcn):
    name = prefix + "isWeekend"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Boolean())
    errcodeBase = 40160
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        day = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos).weekday()
        return day == 5 or day == 6
provide(IsWeekend())

class IsWorkHours(LibFcn):
    name = prefix + "isWorkHours"
    sig = Sig([{"ts": P.Double()}, {"zone": P.String()}], P.Boolean())
    errcodeBase = 40170
    def __call__(self, state, scope, pos, paramTypes, ts, zone):
        dt = tz(datetime.datetime.fromtimestamp(tscheck(ts, self.errcodeBase + 1, self.name, pos), UTC()), zone, self.errcodeBase + 0, self.name, pos)
        day = dt.weekday()
        hour = dt.hour
        return (day != 5 or day != 6) and (hour >= 9 and hour < 17)
provide(IsWorkHours())
