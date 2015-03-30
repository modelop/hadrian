
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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, negativeIndex, checkRange, startEnd
import titus.P as P

import datetime
import time

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn
prefix = "time."

############################################################# Year ################
class Year(LibFcn):
    name = prefix + "year"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.year
provide(Year())


############################################################# MonthOfYear ################
class MonthOfYear(LibFcn):
    name = prefix + "monthOfYear"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.month
provide(MonthOfYear())


############################################################# DayOfYear ################
class DayOfYear(LibFcn):
    name = prefix + "dayOfYear"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.timetuple().tm_yday
provide(DayOfYear())


############################################################# DayOfMonth ################
class DayOfMonth(LibFcn):
    name = prefix + "dayOfMonth"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.day
provide(DayOfMonth())


############################################################# DayOfWeek ################
class DayOfWeek(LibFcn):
    name = prefix + "dayOfWeek"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.weekday()
provide(DayOfWeek())


############################################################# HourOfDay ################
class HourOfDay(LibFcn):
    name = prefix + "hourOfDay"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.hour
provide(HourOfDay())


############################################################# MinuteOfHour ################
class MinuteOfHour(LibFcn):
    name = prefix + "minuteOfHour"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.minute
provide(MinuteOfHour())


############################################################# SecondOfMinute ################
class SecondOfMinute(LibFcn):
    name = prefix + "secondOfMinute"
    sig = Sig([{"ts": P.Double()}],  P.Int())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        return dt.second
provide(SecondOfMinute())


############################################################# MakeTimestamp ################
class MakeTimestamp(LibFcn):
    name = prefix + "makeTimestamp"
    sig = Sig([{"year": P.Int()}, {"month": P.Int()}, {"day": P.Int()},
               {"hour": P.Int()}, {"minute": P.Int()}, {"second": P.Int()},
               {"millisecond": P.Int()}],  P.Int())
    def __call__(self, state, scope, paramTypes, year, month, day, hour, minute, second, millisecond):
        try:
            microsecond = int(round(millisecond*1000.0))
            dt = datetime.datetime(year, month, day, hour, minute, second, microsecond)
            return (dt - datetime.datetime(1970,1,1)).total_seconds()
        except:
            raise PFARuntimeException('timestamp undefined for given parameters')
provide(MakeTimestamp())


############################################################# IsSecondOfMinute ################
class IsSecondOfMinute(LibFcn):
    name = prefix + "isSecondOfMinute"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        second = dt.second
        if (second >= low and second < high):
            return True
        else:
            return False
provide(IsSecondOfMinute())


############################################################# IsMinuteOfHour ################
class IsMinuteOfHour(LibFcn):
    name = prefix + "isMinuteOfHour"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        minute = dt.minute
        if (minute >= low and minute < high):
            return True
        else:
            return False
provide(IsMinuteOfHour())


############################################################# IsHourOfDay ################
class IsHourOfDay(LibFcn):
    name = prefix + "isHourOfDay"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        hour = dt.hour
        if (hour >= low and hour < high):
            return True
        else:
            return False
provide(IsHourOfDay())


############################################################# IsDayOfWeek ################
class IsDayOfWeek(LibFcn):
    name = prefix + "isDayOfWeek"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        day = dt.weekday()
        if (day >= low and day < high):
            return True
        else:
            return False
provide(IsDayOfWeek())


############################################################# IsDayOfMonth ################
class IsDayOfMonth(LibFcn):
    name = prefix + "isDayOfMonth"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        day = dt.day
        if (day >= low and day < high):
            return True
        else:
            return False
provide(IsDayOfMonth())


############################################################# IsMonthOfYear ################
class IsMonthOfYear(LibFcn):
    name = prefix + "isMonthOfYear"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        month = dt.month
        if (month >= low and month < high):
            return True
        else:
            return False
provide(IsMonthOfYear())


############################################################# IsDayOfYear ################
class IsDayOfYear(LibFcn):
    name = prefix + "isDayOfYear"
    sig = Sig([{"ts": P.Double()}, {"low": P.Double()}, {"high": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts, low, high):
        if (low >= high):
            raise PFARuntimeException('bad time range')
        dt = datetime.datetime.utcfromtimestamp(ts)
        dt = dt.timetuple()
        day = dt.tm_yday
        if (day >= low and day < high):
            return True
        else:
            return False
provide(IsDayOfYear())


############################################################# IsWeekend ################
class IsWeekend(LibFcn):
    name = prefix + "isWeekend"
    sig = Sig([{"ts": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        day = dt.weekday()
        if (day == 5 and day == 6):
            return True
        else:
            return False
provide(IsWeekend())


############################################################# IsWorkHours ################
class IsWorkHours(LibFcn):
    name = prefix + "isWorkHours"
    sig = Sig([{"ts": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        day = dt.weekday()
        hour = dt.hour
        if (day != 5 and day != 6 and hour >= 9 and hour < 17):
            return True
        else:
            return False
provide(IsWorkHours())

############################################################# IsNonWorkHours ################
class IsNonWorkHours(LibFcn):
    name = prefix + "isNonWorkHours"
    sig = Sig([{"ts": P.Double()}],  P.Boolean())
    def __call__(self, state, scope, paramTypes, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        day = dt.weekday()
        hour = dt.hour
        if (day < 5 and (hour < 9 or hour >= 17)):
            return True
        else:
            return False
provide(IsNonWorkHours())

############################################################# FromUTCToLocal ################
class FromUTCToLocal(LibFcn):
    name = prefix + "fromUTCToLocal"
    sig = Sig([{"ts": P.Double()}, {"offset": P.Double()}],  P.Double())
    def __call__(self, state, scope, paramTypes, ts, offset):
        dt = datetime.datetime.fromtimestamp(ts) + datetime.timedelta(milliseconds=round(offset*3600000))
        return time.mktime(dt.timetuple())
provide(FromUTCToLocal())

############################################################# FromLocalToUTC ################
class FromLocalToUTC(LibFcn):
    name = prefix + "fromLocalToUTC"
    sig = Sig([{"ts": P.Double()}, {"offset": P.Double()}],  P.Double())
    def __call__(self, state, scope, paramTypes, ts, offset):
        dt = datetime.datetime.fromtimestamp(ts) - datetime.timedelta(milliseconds=round(offset*3600000))
        return time.mktime(dt.timetuple())
provide(FromLocalToUTC())


















