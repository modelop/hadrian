#!/usr/bin/env python

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

from collections import OrderedDict
import itertools

import titus.P as P
from titus.datatype import Type
from titus.datatype import FcnType
from titus.datatype import ExceptionType
from titus.datatype import AvroType
from titus.datatype import AvroCompiled
from titus.datatype import AvroNull
from titus.datatype import AvroBoolean
from titus.datatype import AvroInt
from titus.datatype import AvroLong
from titus.datatype import AvroFloat
from titus.datatype import AvroDouble
from titus.datatype import AvroBytes
from titus.datatype import AvroFixed
from titus.datatype import AvroString
from titus.datatype import AvroEnum
from titus.datatype import AvroArray
from titus.datatype import AvroMap
from titus.datatype import AvroRecord
from titus.datatype import AvroField
from titus.datatype import AvroUnion

class IncompatibleTypes(Exception):
    """An internal Exception raised when signature matching needs to give up and return False.

    This is an internal Exception and should always be caught, not propagated to the user.
    """
    pass

class PFAVersion(object):
    """Describes a PFA (the language) version number as a triple of major number, minor number, release number (all non-negative integers).

    The version has a strict ordering (numerical lexicographic).

    Used to specify a version in which to interpret a PFA document and to label the beginning and end of support of function signatures.

    It could, in the future, be used to label support ranges of special forms, too, but that would always be managed by hand within the special form's constructors.
    """

    def __init__(self, major, minor, release):
        """Create a ``PFAVersion`` from a major, minor, release number triple (all non-negative integers).

        :type major: non-negative integer
        :param major: major version number (major changes)
        :type minor: non-negative integer
        :param minor: minor version number (new features)
        :type release: non-negative integer
        :param release: release version number (bug-fixes)
        """
        if major < 0 or minor < 0 or release < 0 or not isinstance(major, (int, long)) or not isinstance(minor, (int, long)) or not isinstance(release, (int, long)):
            raise ValueError("PFA version major, minor, and release numbers must be non-negative integers")
        self.major = major
        self.minor = minor
        self.release = release
    def __repr__(self):
        """Represent a PFAVersion as a dotted triple on the screen."""
        return "{0}.{1}.{2}".format(self.major, self.minor, self.release)
    def __cmp__(self, other):
        """Strict ordering on PFAVersions: check major number first, then minor, then release."""
        if self.major == other.major and self.minor == other.minor:
            return cmp(self.release, other.release)
        elif self.major == other.major:
            return cmp(self.minor, other.minor)
        else:
            return cmp(self.major, other.major)
    @staticmethod
    def fromString(x):
        """Create a ``PFAVersion`` from a dotted string.

        :type x: string in the form "major.minor.release"
        :param x: string to convert into a ``PFAVersion``
        :rtype: titus.signature.PFAVersion
        :return: corresponding ``PFAVersion``
        """
        try:
            major, minor, release = map(int, x.split("."))
            return PFAVersion(major, minor, release)
        except ValueError:
            raise ValueError("PFA version numbers must have major.minor.release structure, where major, minor, and release are integers")

class Lifespan(object):
    """Describes the range of support of a function signature (or, in the future, special form) in terms of an optional beginning of life (birth), and optional deprecation and end of life (death).

    If a deprecation is specified, a death must be as well, and vice-versa. Whether or not a birth is specified is independent.

    At a given titus.signature.PFAVersion, the Lifespan has three possible states: current (method ``current`` returns ``True``), deprecated (method ``deprecated`` returns ``True``), and non-existent (both ``current`` and ``deprecated`` return ``False``). Method ``current`` and ``deprecated`` are mutually exclusive; for a given titus.signature.PFAVersion, they would never both return ``True``.
    """

    def __init__(self, birth, deprecation, death, contingency):
        """Create a Lifespan from a birth, deprecation, death, and contingency.

        :type birth: titus.signature.PFAVersion or ``None``
        :param birth: first PFA version number in which this signature (or special form) is valid; ``None`` means the beginning of time
        :type deprecation: titus.signature.PFAVersion or ``None``
        :param deprecation: first PFA version number in which this signature should raise a deprecation warning; ``None`` means never
        :type death: titus.signature.PFAVersion or ``None``
        :param death: first PFA version number in which this signature is skipped over as not existing; ``None`` means the end of time
        :type contingency: string or ``None``
        :param contingency: message to accompany the deprecation warning; usually tells the user which function to use instead
        """

        if deprecation is None and death is None:
            pass
        elif deprecation is not None and death is not None and deprecation < death:
            pass
        else:
            raise ValueError("deprecation and death must be specified together, and deprecation version must be strictly earlier than death version")

        if birth is not None and deprecation is not None and birth >= deprecation:
            raise ValueError("if birth and deprecation are specified together, birth version must be strictly earlier than deprecation version")

        self.birth = birth
        self.deprecation = deprecation
        self.death = death
        self.contingency = contingency

    def current(self, now):
        """:type now: titus.signature.PFAVersion
        :param now: the version number to query
        :rtype: bool
        :return: ``True`` if the feature exists and is not deprecated in version ``now``, ``False`` otherwise.
        """
        return (self.birth is None or now >= self.birth) and (self.deprecation is None or now < self.deprecation)

    def deprecated(self, now):
        """:type now: titus.signature.PFAVersion
        :param now: the version number to query
        :rtype: bool
        :return: ``True`` if the feature exists and is deprecated in version ``now``, ``False`` otherwise.
        """
        return self.deprecation is not None and self.deprecation <= now < self.death

class LabelData(object):
    """Used internally to carry information about a label in a wildcard and how well it matches a prospective argument type."""
    @staticmethod
    def distinctTypes(candidates, out = None):
        if out is None:
            out = []
        for candidate in candidates:
            if isinstance(candidate, AvroUnion):
                out = LabelData.distinctTypes(candidate.types, out)
            elif any(y.accepts(candidate) for y in out):
                pass
            else:
                i = 0
                for y in out:
                    if candidate.accepts(y): break
                    i += 1
                if i == len(out):
                    out.append(candidate)
                else:
                    out[i] = candidate
        return out

    @staticmethod
    def broadestType(candidates):
        """Compute the narrowest possible supertype of a set of types.

        :type candidates: list of titus.datatype.AvroType
        :param candidates: set of types for which to find the narrowest possible supertype
        :rtype: titus.datatype.AvroType
        :return: narrowest possible supertype, usually a union of the candidates
        """

        realCandidates = [x for x in candidates if not isinstance(x, ExceptionType)]

        if len(candidates) == 0:
            return ValueError("empty list of types")
        elif len(realCandidates) == 0:
            return ValueError("list of types consists only of exception type")

        elif all(isinstance(x, AvroNull) for x in realCandidates):
            return realCandidates[0]
        elif all(isinstance(x, AvroBoolean) for x in realCandidates):
            return realCandidates[0]

        elif all(isinstance(x, AvroInt) for x in realCandidates):
            return AvroInt()
        elif all(isinstance(x, AvroInt) or isinstance(x, AvroLong) for x in realCandidates):
            return AvroLong()
        elif all(isinstance(x, AvroInt) or isinstance(x, AvroLong) or isinstance(x, AvroFloat) for x in realCandidates):
            return AvroFloat()
        elif all(isinstance(x, AvroInt) or isinstance(x, AvroLong) or isinstance(x, AvroFloat) or isinstance(x, AvroDouble) for x in realCandidates):
            return AvroDouble()

        elif all(isinstance(x, AvroBytes) for x in realCandidates):
            return realCandidates[0]
        elif all(isinstance(x, AvroString) for x in realCandidates):
            return realCandidates[0]

        elif all(isinstance(x, AvroArray) for x in realCandidates):
            return AvroArray(P.mustBeAvro(LabelData.broadestType([x.items for x in realCandidates])))

        elif all(isinstance(x, AvroMap) for x in realCandidates):
            return AvroMap(P.mustBeAvro(LabelData.broadestType([x.values for x in realCandidates])))

        elif all(isinstance(x, AvroFixed) for x in realCandidates):
            fullName = realCandidates[0].fullName
            if all(x.fullName == fullName for x in realCandidates[1:]):
                return realCandidates[0]
            else:
                raise IncompatibleTypes("incompatible fixed types: " + " ".join(map(repr, realCandidates)))

        elif all(isinstance(x, AvroEnum) for x in realCandidates):
            fullName = realCandidates[0].fullName
            if all(x.fullName == fullName for x in realCandidates[1:]):
                return realCandidates[0]
            else:
                raise IncompatibleTypes("incompatible enum types: " + " ".join(map(repr, realCandidates)))

        elif all(isinstance(x, AvroRecord) for x in realCandidates):
            fullName = realCandidates[0].fullName
            if all(x.fullName == fullName for x in realCandidates[1:]):
                return realCandidates[0]
            else:
                raise IncompatibleTypes("incompatible record types: " + " ".join(map(repr, realCandidates)))

        elif all(isinstance(x, FcnType) for x in realCandidates):
            params = realCandidates[0].params
            ret = realCandidates[0].ret

            if all(x.params == params and x.ret == ret for x in realCandidates[1:]):
                return realCandidates[0]
            else:
                raise IncompatibleTypes("incompatible function types: " + " ".join(map(repr, realCandidates)))

        elif not any(isinstance(x, FcnType) for x in realCandidates):
            types = LabelData.distinctTypes(realCandidates)
            types = [P.mustBeAvro(x) for x in types]

            countFixed = 0
            countEnum = 0
            for t in types:
                if isinstance(t, AvroFixed):
                    countFixed += 1
                if isinstance(t, AvroEnum):
                    countEnum += 1

            if countFixed > 1:
                raise IncompatibleTypes("incompatible fixed types")
            if countEnum > 1:
                raise IncompatibleTypes("incompatible enum types")

            return AvroUnion(types)

        else:
            raise IncompatibleTypes("incompatible function/non-function types: " + " ".join(map(repr, realCandidates)))

    def __init__(self):
        self.members = []
        self.expectedFields = None

    def add(self, t):
        self.members.append(t)

    def determineAssignment(self):
        out = LabelData.broadestType(self.members)
        if self.expectedFields is not None:
            if isinstance(out, AvroRecord):
                if self.expectedFields != [x.name for x in out.fields]:
                    raise IncompatibleTypes("fields do not agree with EnumFields symbols (same names, same order)")
            else:
                raise IncompatibleTypes("EnumFields.wildRecord does not point to a record")
        return out

    def __repr__(self):
        return "<titus.signature.LabelData {0} at {1}>".format(self.members, "0x%x" % id(self))

class Signature(object):
    """Abstract trait for function signatures. Known subclasses: titus.signature.Sig and titus.signature.Sigs."""
    def accepts(self, args, version):
        raise NotImplementedError

class Sigs(Signature):
    """PFA function signature for ad-hoc polymorphism (list of different signatures supported by the function)."""

    def __init__(self, cases):
        """:type cases: list of titus.signature.Sig
        :param cases: signatures supported by this function; the order of this list is the order in which Titus will attempt to resolve the signatures (the first one that matches is used)
        """
        self.cases = cases

    def accepts(self, args, version):
        """Determine if this list of signatures accepts the given arguments for a given PFA version number.

        :type args: list of titus.datatype.AvroType
        :param args: arguments to match against each of the signature patterns
        :type version: titus.signature.PFAVersion
        :param version: PFA version number in which to interpret the patterns
        :rtype: (titus.signature.Sig, list of titus.datatype.AvroType, AvroType)
        :return: (matching signature, resolved argument types, resolved return type) if one of the signatures accepts the arguments; ``None`` otherwise
        """
        for case in self.cases:
            result = case.accepts(args, version)
            if result is not None:
                return result
        return None

class Sig(Signature):
    """PFA function signature for a single pattern.

    This class can either be used directly as a function's only signature or it may be contained within a titus.signature.Sigs.
    """

    def __init__(self, params, ret, lifespan = Lifespan(None, None, None, None)):
        """:type params: list of titus.P
        :param params: patterns for each argument (in order)
        :type ret: titus.P
        :param ret: pattern for the return type
        :type lifespan: titus.signature.Lifespan
        :param lifespan: validity range for this signature; default is eternal (existing from the beginning of time to the end of time)
        """
        self.params = params
        self.ret = ret
        self.lifespan = lifespan

    def __repr__(self):
        alreadyLabeled = set()
        return "(" + ", ".join(p.keys()[0] + ": " + toText(p.values()[0], alreadyLabeled) for p in self.params) + " -> " + toText(self.ret, alreadyLabeled) + ")"

    def accepts(self, args, version):
        """Determine if this signature accepts the given arguments for a given PFA version number.

        :type args: list of titus.datatype.AvroType
        :param args: arguments to match against the signature pattern
        :type version: titus.signature.PFAVersion
        :param version: PFA version number in which to interpret the pattern
        :rtype: (titus.signature.Sig, list of titus.datatype.AvroType, AvroType)
        :return: (self, resolved argument types, resolved return type) if this signature accepts the arguments; ``None`` otherwise
        """

        if self.lifespan.current(version) or self.lifespan.deprecated(version):
            labelData = {}
            if len(self.params) == len(args) and all(self.check(p.values()[0], a, labelData, False, False) for p, a in zip(self.params, args)):
                try:
                    assignments = dict((l, ld.determineAssignment()) for l, ld in labelData.items())
                    assignedParams = [self.assign(p.values()[0], a, assignments) for p, a in zip(self.params, args)]
                    assignedRet = self.assignRet(self.ret, assignments)
                    return (self, assignedParams, assignedRet)
                except IncompatibleTypes:
                    return None
            else:
                return None
        else:
            return None

    def check(self, pat, arg, labelData, strict, reversed):
        """Determine if a single slot in the parameter pattern matches a single argument's type.

        :type pat: titus.P
        :param pat: pattern for a type that can include wildcards, etc.
        :type arg: titus.datatype.AvroType
        :param arg: supplied argument type that may or may not fit the pattern
        :type labelData: dict from label letters to titus.signature.LabelData
        :param labelData: label associations made so far (so that repeated label letters are forced to resolve to the same types)
        :type strict: boolean
        :param strict: if ``True``, don't allow subtypes to match supertypes
        :type reversed: if ``True``, match structures contravariantly instead of covariantly (for instance, when matching arguments of functions passed as arguments)
        :rtype: bool
        :return: ``True`` if ``arg`` matches ``pat``; ``False`` otherwise
        """

        if isinstance(arg, ExceptionType):
            return False

        elif isinstance(pat, P.Null) and isinstance(arg, AvroNull):
            return True
        elif isinstance(pat, P.Boolean) and isinstance(arg, AvroBoolean):
            return True

        elif isinstance(pat, P.Int) and isinstance(arg, AvroInt):
            return True
        elif isinstance(pat, P.Long) and isinstance(arg, AvroLong):
            return True
        elif isinstance(pat, P.Float) and isinstance(arg, AvroFloat):
            return True
        elif isinstance(pat, P.Double) and isinstance(arg, AvroDouble):
            return True

        elif not strict and not reversed and \
                 isinstance(pat, P.Long) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong)):
            return True
        elif not strict and not reversed and \
                 isinstance(pat, P.Float) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong) or isinstance(arg, AvroFloat)):
            return True
        elif not strict and not reversed and \
                 isinstance(pat, P.Double) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong) or isinstance(arg, AvroFloat) or isinstance(arg, AvroDouble)):
            return True

        elif not strict and reversed and \
                 (isinstance(pat, P.Int) or isinstance(pat, P.Long)) and isinstance(arg, AvroLong):
            return True
        elif not strict and reversed and \
                 (isinstance(pat, P.Int) or isinstance(pat, P.Long) or isinstance(pat, P.Float)) and isinstance(arg, AvroFloat):
            return True
        elif not strict and reversed and \
                 (isinstance(pat, P.Int) or isinstance(pat, P.Long) or isinstance(pat, P.Float) or isinstance(pat, P.Double)) and isinstance(arg, AvroDouble):
            return True

        elif isinstance(pat, P.Bytes) and isinstance(arg, AvroBytes):
            return True
        elif isinstance(pat, P.String) and isinstance(arg, AvroString):
            return True

        elif isinstance(pat, P.Array) and isinstance(arg, AvroArray):
            return self.check(pat.items, arg.items, labelData, strict, reversed)
        elif isinstance(pat, P.Map) and isinstance(arg, AvroMap):
            return self.check(pat.values, arg.values, labelData, strict, reversed)
        elif isinstance(pat, P.Union) and isinstance(arg, AvroUnion):
            found = False
            originalLabelData = dict(labelData)
            labelData2 = dict(labelData)
            atypesPermutations = itertools.permutations(arg.types)

            atypesPermutations_isEmpty = False
            while not found and not atypesPermutations_isEmpty:
                available = OrderedDict((i, x) for i, x in enumerate(pat.types))
                try:
                    nextPermutation = atypesPermutations.next()
                except StopIteration:
                    atypesPermutations_isEmpty = True
                else:
                    anyfalse = False
                    for a in nextPermutation:
                        foundone = False
                        for i, p in available.items():
                            if self.check(p, a, labelData2, True, reversed):
                                del available[i]
                                foundone = True
                                break

                        if not foundone:
                            anyfalse = True
                            break

                    if not anyfalse:
                        found = True

                    if not found:
                        labelData2 = dict(originalLabelData)

            labelData.update(labelData2)
            return found
            
        elif isinstance(pat, P.Union) and isinstance(arg, AvroType):
            return any(self.check(p, arg, labelData, strict, reversed) for p in pat.types)

        elif isinstance(pat, P.Fixed) and isinstance(arg, AvroFixed):
            if pat.fullName is not None:
                return pat.fullName == arg.fullName
            else:
                return pat.size == arg.size

        elif isinstance(pat, P.Enum) and isinstance(arg, AvroEnum):
            if pat.fullName is not None:
                return pat.fullName == arg.fullName
            else:
                return pat.symbols == arg.symbols

        elif isinstance(pat, P.Record) and isinstance(arg, AvroRecord):
            if pat.fullName is not None:
                return pat.fullName == arg.fullName
            else:
                amap = dict((x.name, x) for x in arg.fields)

                if set(pat.fields.keys()) == set(amap.keys()):
                    return all(self.check(pat.fields[k], amap[k].avroType, labelData, True, reversed) for k in pat.fields.keys())
                else:
                    return False

        elif isinstance(pat, P.Fcn) and isinstance(arg, FcnType):
            return all(self.check(p, a, labelData, strict, True) for p, a in zip(pat.params, arg.params)) and \
                   self.check(pat.ret, arg.ret, labelData, strict, False)

        elif isinstance(pat, P.Wildcard):
            if pat.oneOf is None or arg in pat.oneOf:
                if not pat.label in labelData:
                    labelData[pat.label] = LabelData()
                labelData[pat.label].add(arg)
                return True
            else:
                return False

        elif isinstance(pat, P.WildRecord) and isinstance(arg, AvroRecord):
            if not pat.label in labelData:
                labelData[pat.label] = LabelData()
            labelData[pat.label].add(arg)

            amap = dict((x.name, x) for x in arg.fields)

            if set(pat.minimalFields.keys()).issubset(set(amap.keys())):
                return all(self.check(pt, amap[pn].avroType, labelData, True, reversed) for pn, pt in pat.minimalFields.items())
            else:
                return False

        elif isinstance(pat, P.WildEnum):
            if not pat.label in labelData:
                labelData[pat.label] = LabelData()
            labelData[pat.label].add(arg)
            return True

        elif isinstance(pat, P.WildFixed):
            if not pat.label in labelData:
                labelData[pat.label] = LabelData()
            labelData[pat.label].add(arg)
            return True

        elif isinstance(pat, P.EnumFields) and isinstance(arg, AvroEnum):
            if not pat.label in labelData:
                labelData[pat.label] = LabelData()
            labelData[pat.label].add(arg)
            labelData[pat.wildRecord].expectedFields = arg.symbols
            return True

        else:
            return False

    def assign(self, pat, arg, assignments):
        """Apply the label assignments (e.g. "A" matched to "int", "B" matched to "string", etc.) to each parameter of the signature.

        :type pat: titus.P
        :param pat: original parameter pattern
        :type arg: titus.datatype.AvroType
        :param arg: supplied argument type
        :type assignments: dict from label letters to titus.datatype.AvroType
        :param assignments: assigned types to apply
        :rtype: titus.datatype.AvroType
        :return: resolved type for one parameter of the signature
        """

        if isinstance(pat, P.Null) and isinstance(arg, AvroNull):
            return arg
        elif isinstance(pat, P.Boolean) and isinstance(arg, AvroBoolean):
            return arg

        elif isinstance(pat, P.Int) and isinstance(arg, AvroInt):
            return AvroInt()
        elif isinstance(pat, P.Long) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong)):
            return AvroLong()
        elif isinstance(pat, P.Float) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong) or isinstance(arg, AvroFloat)):
            return AvroFloat()
        elif isinstance(pat, P.Double) and (isinstance(arg, AvroInt) or isinstance(arg, AvroLong) or isinstance(arg, AvroFloat) or isinstance(arg, AvroDouble)):
            return AvroDouble()

        elif isinstance(pat, P.Bytes) and isinstance(arg, AvroBytes):
            return arg
        elif isinstance(pat, P.String) and isinstance(arg, AvroString):
            return arg

        elif isinstance(pat, P.Array) and isinstance(arg, AvroArray):
            return AvroArray(P.mustBeAvro(self.assign(pat.items, arg.items, assignments)))
        elif isinstance(pat, P.Map) and isinstance(arg, AvroMap):
            return AvroMap(P.mustBeAvro(self.assign(pat.values, arg.values, assignments)))
        elif isinstance(pat, P.Union) and isinstance(arg, AvroUnion):
            return arg
        elif isinstance(pat, P.Union) and isinstance(arg, AvroType):
            return arg

        elif isinstance(pat, P.Fixed) and isinstance(arg, AvroFixed):
            return arg
        elif isinstance(pat, P.Enum) and isinstance(arg, AvroEnum):
            return arg
        elif isinstance(pat, P.Record) and isinstance(arg, AvroRecord):
            return arg

        elif isinstance(pat, P.Fcn) and isinstance(arg, FcnType):
            return arg

        elif isinstance(pat, P.Wildcard):
            return assignments[pat.label]
        elif isinstance(pat, P.WildRecord):
            return assignments[pat.label]
        elif isinstance(pat, P.WildEnum):
            return assignments[pat.label]
        elif isinstance(pat, P.WildFixed):
            return assignments[pat.label]
        elif isinstance(pat, P.EnumFields):
            return assignments[pat.label]

        else:
            raise Exception

    def assignRet(self, pat, assignments):
        """Apply the label assignments (e.g. "A" matched to "int", "B" matched to "string" etc.) to the return pattern.

        :type pat: titus.P
        :param pat: original return pattern
        :type assignments: dict from label letters to titus.datatype.AvroType
        :param assignments: assigned types to apply
        :rtype: titus.datatype.AvroType
        :return: resolved type for the return value of the signature
        """

        if isinstance(pat, P.Null):
            return AvroNull()
        elif isinstance(pat, P.Boolean):
            return AvroBoolean()
        elif isinstance(pat, P.Int):
            return AvroInt()
        elif isinstance(pat, P.Long):
            return AvroLong()
        elif isinstance(pat, P.Float):
            return AvroFloat()
        elif isinstance(pat, P.Double):
            return AvroDouble()
        elif isinstance(pat, P.Bytes):
            return AvroBytes()
        elif isinstance(pat, P.String):
            return AvroString()

        elif isinstance(pat, P.Array):
            return AvroArray(self.assignRet(pat.items, assignments))
        elif isinstance(pat, P.Map):
            return AvroMap(self.assignRet(pat.values, assignments))
        elif isinstance(pat, P.Union):
            return AvroUnion([self.assignRet(x, assignments) for x in pat.types])

        elif isinstance(pat, P.Fixed):
            return P.toType(pat)
        elif isinstance(pat, P.Enum):
            return P.toType(pat)
        elif isinstance(pat, P.Record):
            return P.toType(pat)
        elif isinstance(pat, P.Fcn):
            return P.toType(pat)

        elif isinstance(pat, P.Wildcard):
            return assignments[pat.label]
        elif isinstance(pat, P.WildRecord):
            return assignments[pat.label]
        elif isinstance(pat, P.WildEnum):
            return assignments[pat.label]
        elif isinstance(pat, P.WildFixed):
            return assignments[pat.label]
        elif isinstance(pat, P.EnumFields):
            return assignments[pat.label]

        else:
            raise Exception(repr(pat))

def toText(p, alreadyLabeled=None):
    """Render a pattern as human-readable text.

    :type p: titus.P
    :param p: the pattern
    :type alreadyLabeled: set of strings or ``None``
    :param alreadyLabeled: used to avoid recursion in recursive types
    :rtype: string
    :return: text representation of the pattern
    """

    if alreadyLabeled is None:
        alreadyLabeled = set()
    if isinstance(p, P.Null):
        return "null"
    elif isinstance(p, P.Boolean):
        return "boolean"
    elif isinstance(p, P.Int):
        return "int"
    elif isinstance(p, P.Long):
        return "long"
    elif isinstance(p, P.Float):
        return "float"
    elif isinstance(p, P.Double):
        return "double"
    elif isinstance(p, P.Bytes):
        return "bytes"
    elif isinstance(p, P.Fixed):
        if p.fullName is None:
            return "fixed(size: {0})".format(p.size)
        else:
            return "fixed(size: {0}, name: {1})".format(p.size, p.fullName)
    elif isinstance(p, P.String):
        return "string"
    elif isinstance(p, P.Enum):
        if p.fullName is None:
            return "fixed(symbols: {0})".format(", ".join(p.symbols))
        else:
            return "fixed(symbols: {0}, name: {1})".format(", ".join(p.symbols), p.fullName)
    elif isinstance(p, P.Array):
        return "array of " + toText(p.items, alreadyLabeled)
    elif isinstance(p, P.Map):
        return "map of " + toText(p.values, alreadyLabeled)
    elif isinstance(p, P.Record):
        if p.fullName is None:
            return "record (fields: {" + ", ".join(n + ": " + toText(f, alreadyLabeled) for n, f in p.fields.items()) + "})"
        else:
            return "record (fields: {" + ", ".join(n + ": " + toText(f, alreadyLabeled) for n, f in p.fields.items()) + ", name: " + p.fullName + "})"
    elif isinstance(p, P.Union):
        return "union of {" + ", ".join(toText(x, alreadyLabeled) for x in p.types) + "}"
    elif isinstance(p, P.Fcn):
        return "function of (" + ", ".join(toText(x, alreadyLabeled) for x in p.params) + ") -> " + toText(p.ret, alreadyLabeled)
    elif isinstance(p, P.Wildcard) and p.label in alreadyLabeled:
        return p.label
    elif isinstance(p, P.WildEnum) and p.label in alreadyLabeled:
        return p.label
    elif isinstance(p, P.WildFixed) and p.label in alreadyLabeled:
        return p.label
    elif isinstance(p, P.WildRecord) and p.label in alreadyLabeled:
        return p.label
    elif isinstance(p, P.EnumFields) and p.label in alreadyLabeled:
        return p.label
    elif isinstance(p, P.Wildcard):
        alreadyLabeled.add(p.label)
        if len(p.oneOf) == 0:
            return "any " + p.label
        else:
            return "any " + p.label + " of {" + ", ".join(toTextType(x) for x in p.oneOf) + "}"
    elif isinstance(p, P.WildEnum):
        alreadyLabeled.add(p.label)
        return "any enum " + p.label
    elif isinstance(p, P.WildFixed):
        alreadyLabeled.add(p.label)
        return "any fixed " + p.label
    elif isinstance(p, P.WildRecord):
        alreadyLabeled.add(p.label)
        return "any record " + p.label + ("" if len(p.minimalFields) == 0 else " with fields {" + ", ".join(n + ": " + toText(f, alreadyLabeled) for n, f in p.minimalFields.items()) + "}")
    elif isinstance(p, P.EnumFields):
        alreadyLabeled.add(p.label)
        return "enum " + p.label + " of fields of " + p.wildRecord

def toTextType(x):
    """Render a type as human-readable text.

    Note that this function is incomplete, but it covers all the cases seen in any PFA functions.

    :type x: titus.datatype.AvroType
    :param x: the data type
    :rtype: string
    :return: text representation of the data type
    """

    if isinstance(x, AvroNull):
        return "null"
    elif isinstance(x, AvroBoolean):
        return "boolean"
    elif isinstance(x, AvroInt):
        return "int"
    elif isinstance(x, AvroLong):
        return "long"
    elif isinstance(x, AvroFloat):
        return "float"
    elif isinstance(x, AvroDouble):
        return "double"
    elif isinstance(x, AvroBytes):
        return "bytes"
    elif isinstance(x, AvroString):
        return "string"
    elif isinstance(x, AvroArray):
        return "array of " + toTextType(x.items)
    elif isinstance(x, AvroMap):
        return "map of " + toTextType(x.values)
