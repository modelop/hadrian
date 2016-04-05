#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import OrderedDict
import copy
import json
import re
import sys

############################################## quick access functions

def splitIndex(index):
    """Normalize a casual index like "one, two, 3,four" to canonical form like ["one", "two", 3, "four"]."""
    out = [x.strip() for x in index.split(",")]
    for i in xrange(len(out)):
        try:
            out[i] = int(out[i])
        except ValueError:
            pass
    return out

def get(expr, index):
    """Get a subexpression from the root expr and index."""
    if isinstance(index, basestring):
        index = splitIndex(index)
    out = expr
    for i in index:
        out = out[i]
    return out

def assign(expr, index, to):
    """Destructively (in-place) replace a subexpression at a given index with another expression."""
    if isinstance(index, basestring):
        index = splitIndex(index)
    out = expr
    for i in index[:-1]:
        out = out[i]
    out[index[-1]] = to

def assigned(expr, index, to):
    """Return a copy of expr with a replaced subexpression, leaving the original intact."""
    expr = copy.deepcopy(expr)
    assign(expr, index, to)
    return expr

def assignAt(pattern, expr, to):
    """Destructively (in-place) replace subexpressions that match a regular expression pattern."""
    for i in indexes(pattern, expr):
        assign(expr, i, to)

def assignedAt(pattern, expr, to):
    """Return a copy of expr with replaced subexpressions at points that match a regular expression pattern."""
    expr = copy.deepcopy(expr)
    for i in indexes(pattern, expr):
        assign(expr, i, to)
    return expr

def remove(expr, index):
    """Destructively (in-place) remove a subexpression at a given index."""
    if isinstance(index, basestring):
        index = splitIndex(index)
    out = expr
    for i in index[:-1]:
        out = out[i]
    del out[index[-1]]

def removed(expr, index):
    """Return a copy of expr with a subexpression removed at a given index."""
    expr = copy.deepcopy(expr)
    remove(expr, index)
    return expr

############################################## pattern-matching primitives

class Match(object):
    """Represents an expression matched by a regular expression.

    original is a copy of the expression, identical to the original subexpression.
    modified is a copy of the expression with RegEx substitutions.
    groups is a dictionary of modified subexpressions."""

    def __init__(self, original, modified):
        self.original = original
        self.modified = modified
        self.groups = {}
    def __repr__(self):
        return "Match(" + repr(self.modified) + ")"

def getmatch(pattern, haystack):
    """Generic function to match a regular expression pattern to a haystack expression."""

    if isinstance(pattern, dict):
        if not isinstance(haystack, dict):
            return None
        if set(pattern.keys()).difference(set(["@"])) != set(haystack.keys()).difference(set(["@"])):
            return None
        out = Match({}, {})
        for key in pattern:
            if key != "@":
                m = getmatch(pattern[key], haystack[key])
                if m is not None:
                    out.original[key] = m.original
                    out.modified[key] = m.modified
                    out.groups.update(m.groups)
                else:
                    return None
        return out

    elif isinstance(pattern, (list, tuple)):
        if not isinstance(haystack, (list, tuple)):
            return None
        if len(pattern) != len(haystack):
            return None
        out = Match([], [])
        for index, item in enumerate(pattern):
            m = getmatch(item, haystack[index])
            if m is not None:
                out.original.append(m.original)
                out.modified.append(m.modified)
                out.groups.update(m.groups)
            else:
                return None
        return out

    elif isinstance(pattern, (basestring, long, int, float)):
        if pattern == haystack:
            return Match(haystack, haystack)
        else:
            return None

    elif pattern in (True, False, None):
        if pattern is haystack:
            return Match(haystack, haystack)
        else:
            return None

    else:
        return pattern.getmatch(haystack)

class Matcher(object):
    """Base class of regular expression patterns (other than literal JSON expressions)."""
    def getmatch(self, haystack):
        raise NotImplementedError

class Group(Matcher):
    """Tag a subexpression with a name so that it can be found later."""
    def __init__(self, **kwds):
        (self.name, self.matcher), = kwds.items()
    def __repr__(self):
        return "Group(" + repr(self.name) + "=" + repr(self.matcher) + ")"
    def getmatch(self, haystack):
        m = self.matcher.getmatch(haystack)
        if m is not None:
            m.groups[self.name] = m.modified
            return m
        else:
            return None

class Min(Matcher):
    """Match a dictionary that has at least the given key-value pairs."""
    def __init__(self, **obj):
        self.obj = obj
    def __repr__(self):
        return "Min(" + ", ".join(k + "=" + repr(v) for k, v in self.obj.items()) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, dict):
            return None
        if len(set(self.obj.keys()).difference(set(["@"])).difference(set(haystack.keys()))) > 0:
            return None
        out = Match(dict((k, v) for k, v in haystack.items() if k != "@"),
                    dict((k, v) for k, v in haystack.items() if k != "@"))
        for key in self.obj:
            if key != "@":
                m = getmatch(self.obj[key], haystack[key])
                if m is not None:
                    out.original[key] = m.original
                    out.modified[key] = m.modified
                    out.groups.update(m.groups)
                else:
                    return None
        return out

class Start(Matcher):
    """Match a list that starts with the given elements."""
    def __init__(self, *array):
        self.array = array
    def __repr__(self):
        return "Start(" + ", ".join(map(repr, self.array)) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (list, tuple)):
            return None
        if len(self.array) > len(haystack):
            return None
        out = Match([], [])
        for index, item in enumerate(self.array):
            m = getmatch(item, haystack[index])
            if m is not None:
                out.original.append(m.original)
                out.modified.append(m.modified)
                out.groups.update(m.groups)
            else:
                return None
        out.original.extend(haystack[len(self.array):])
        out.modified.extend(haystack[len(self.array):])
        return out

class End(Matcher):
    """Match a list that ends with the given elements."""
    def __init__(self, *array):
        self.array = array
    def __repr__(self):
        return "End(" + ", ".join(map(repr, self.array)) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (list, tuple)):
            return None
        if len(self.array) > len(haystack):
            return None
        out = Match([], [])
        for index, item in enumerate(self.array):
            m = getmatch(item, haystack[len(haystack) - len(self.array) + index])
            if m is not None:
                out.original.append(m.original)
                out.modified.append(m.modified)
                out.groups.update(m.groups)
            else:
                return None
        out.original = haystack[:(len(haystack) - len(self.array))] + out.original
        out.modified = haystack[:(len(haystack) - len(self.array))] + out.modified
        return out

class Or(Matcher):
    """Match at least one of several alternatives.

    If any alternatives have substitutions, the first matching substitution is returned."""
    def __init__(self, *alternatives):
        self.alternatives = alternatives
    def __repr__(self):
        return "Or(" + ", ".join(repr(x) for x in self.alternatives) + ")"
    def getmatch(self, haystack):
        if len(self.alternatives) == 0:
            return None
        for alt in self.alternatives:
            m = getmatch(alt, haystack)
            if m is not None:
                return m
        return None

class And(Matcher):
    """Match every one of several alternatives.

    If any alternatives have substitutions, the last substitution is returned."""
    def __init__(self, *alternatives):
        self.alternatives = alternatives
    def __repr__(self):
        return "And(" + ", ".join(repr(x) for x in self.alternatives) + ")"
    def getmatch(self, haystack):
        if len(self.alternatives) == 0:
            return None
        for alt in self.alternatives:
            m = getmatch(alt, haystack)
            if m is None:
                return None
        return m

class Approx(Matcher):
    """Match a number within a given range."""
    def __init__(self, central, error):
        self.central = central
        self.error = error
    def __repr__(self):
        return "Approx(" + repr(self.central) + ", " + repr(self.error) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (long, int, float)) or abs(haystack - self.central) > self.error:
            return None
        else:
            return Match(haystack, haystack)

class LT(Matcher):
    """Match a number if it is less than some value."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return "LT(" + repr(self.value) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (long, int, float)) or haystack >= self.value:
            return None
        else:
            return Match(haystack, haystack)

class LE(Matcher):
    """Match a number if it is less than or equal to some value."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return "LE(" + repr(self.value) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (long, int, float)) or haystack > self.value:
            return None
        else:
            return Match(haystack, haystack)

class GT(Matcher):
    """Match a number if it is greater than some value."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return "GT(" + repr(self.value) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (long, int, float)) or haystack <= self.value:
            return None
        else:
            return Match(haystack, haystack)

class GE(Matcher):
    """Match a number if it is greater than or equal to some value."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return "GE(" + repr(self.value) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, (long, int, float)) or haystack < self.value:
            return None
        else:
            return Match(haystack, haystack)

class RegEx(Matcher):
    """Match a string with a regular expression.

    If 'to' is given, this provides a substitution (using \\1, \\2, etc. variables).
    The 'flags' are iLmsux, corresponding to Python regular expression flags."""
    def __init__(self, pattern, to=None, flags=None):
        self.pattern = pattern
        self.to = to
        self.flags = flags
    def __repr__(self):
        return "RegEx(" + repr(self.pattern) + ", " + repr(self.to) + ", " + repr(self.flags) + ")"
    def getmatch(self, haystack):
        if not isinstance(haystack, basestring):
            return None
        flags = 0
        if self.flags is not None:
            if "i" in self.flags or "I" in self.flags:
                flags |= re.I
            if "l" in self.flags or "L" in self.flags:
                flags |= re.L
            if "m" in self.flags or "M" in self.flags:
                flags |= re.M
            if "s" in self.flags or "S" in self.flags:
                flags |= re.S
            if "u" in self.flags or "U" in self.flags:
                flags |= re.U
            if "x" in self.flags or "X" in self.flags:
                flags |= re.X
        if re.match(self.pattern, haystack, flags=flags) is None:
            return None
        elif self.to is None:
            return Match(haystack, haystack)
        else:
            return Match(haystack, re.sub(self.pattern, self.to, haystack, flags=flags))

class Any(Matcher):
    """Match any expression or any expression with one of several given types."""
    def __init__(self, *types):
        self.types = types
    def __repr__(self):
        return "Any(" + ", ".join(map(repr, self.types)) + ")"
    def getmatch(self, haystack):
        if len(self.types) == 0 or isinstance(haystack, self.types):
            return Match(haystack, haystack)
        else:
            return None

############################################## pattern-matching functions

def search(pattern, haystack, result=()):
    """Create a generator to yield matches as (index, Match) pairs."""
    if isinstance(haystack, dict):
        for key in sorted(haystack.keys()):
            for x in search(pattern, haystack[key], result + (key,)):
                yield x

    elif isinstance(haystack, (list, tuple)):
        for i, item in enumerate(haystack):
            for x in search(pattern, item, result + (i,)):
                yield x

    m = getmatch(pattern, haystack)
    if m is not None:
        yield result, m

def searchFirst(pattern, haystack):
    """Return a (index, Match) pair or None if there is no match."""
    for x in search(pattern, haystack):
        return x
    return None

def index(pattern, haystack):
    """Return an index or None if there is no match."""
    x = searchFirst(pattern, haystack)
    if x is None:
        return None
    else:
        return x[0]

def indexes(pattern, haystack):
    """Create a generator to yield matching indexes."""
    return (x[0] for x in search(pattern, haystack))

def contains(pattern, haystack):
    """Return True if the pattern is found; False otherwise."""
    return searchFirst(pattern, haystack) is not None

def count(pattern, haystack):
    """Return the number of matches."""
    return len(list(search(pattern, haystack)))

def findRef(pattern, haystack):
    """Return a reference to the matching subexpression within the original structure (for in-place modifications) or None if there is no match."""
    x = searchFirst(pattern, haystack)
    if x is None:
        return None
    else:
        return get(haystack, x[0])

def findCopy(pattern, haystack):
    """Return a copy of the matching subexpression or None if there is no match."""
    x = searchFirst(pattern, haystack)
    if x is None:
        return None
    else:
        return x[1].original   # "original" is a copy that is identical to the original (see Match class)

def findAllRef(pattern, haystack):
    """Create a generator to yield references to matching subexpressions within the original structure (for in-place modifications)."""
    for x in search(pattern, haystack):
        yield get(haystack, x[0])

def findAllCopy(pattern, haystack):
    """Create a generator to yield copies of the matching subexpressions."""
    for x in search(pattern, haystack):
        yield x[1].original   # "original" is a copy that is identical to the original (see Match class)

############################################## the look function presents a JSON object in an analyzable way

def look(expr, maxDepth=8, inlineDepth=2, indexWidth=30, dropAt=True, stream=sys.stdout):
    """Print a JSON object on the screen in a readable way.

    maxDepth: maximum depth to show before printing ellipsis (...)
    inlineDepth: maximum depth to show on a single line
    indexWidth: width (in characters) of the index column on the left
    dropAt: don't show "@" keys
    stream: allows the output to be sent to a file or stream.
    """
    if dropAt:
        expr = _dropAt(expr, maxDepth + inlineDepth)
    stream.write(("%%-%ds %%s\n" % indexWidth) % ("index", "data"))
    stream.write("-" * (indexWidth * 2) + "\n")
    for reprindex, reprdata in _look(expr, maxDepth, inlineDepth, [], indexWidth, ""):
        stream.write(("%%-%ds %%s\n" % indexWidth) % (reprindex, reprdata))
    stream.flush()

def _dropAt(expr, depth):
    if isinstance(expr, dict):
        return expr.__class__([(k, _dropAt(v, depth - 1)) for k, v in expr.items() if k != "@"])
    elif isinstance(expr, (list, tuple)):
        return [_dropAt(x, depth - 1) for x in expr]
    else:
        return expr

def _acceptableDepth(expr, limit):
    if limit < 0:
        return False
    elif isinstance(expr, dict):
        return all(_acceptableDepth(x, limit - 1) for x in expr.values())
    elif isinstance(expr, (list, tuple)):
        return all(_acceptableDepth(x, limit - 1) for x in expr)
    else:
        return True

class _Pair(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

class _Item(object):
    def __init__(self, value):
        self.value = value

def _look(expr, maxDepth, inlineDepth, index, indexWidth, indent):
    reprindex = ",".join(map(str, index))
    if len(reprindex) > indexWidth:
        reprindex = reprindex[:(indexWidth - 3)] + "..."

    if isinstance(expr, _Pair):
        block = _look(expr.value, maxDepth, inlineDepth, index, indexWidth, indent)
        block[0][0] = reprindex
        block[0][1] = indent + json.dumps(expr.key) + ": " + block[0][1].lstrip()
        block[-1][1] = block[-1][1] + ","
        return block

    elif isinstance(expr, _Item):
        block = _look(expr.value, maxDepth, inlineDepth, index, indexWidth, indent)
        block[0][0] = reprindex
        block[0][1] = indent + block[0][1].lstrip()
        block[-1][1] = block[-1][1] + ","
        return block

    elif _acceptableDepth(expr, inlineDepth):
        return [[reprindex, indent + json.dumps(expr)]]

    elif isinstance(expr, dict):
        if maxDepth > 0:
            block = []
            for key, value in expr.items():
                block.extend(_look(_Pair(key, value), maxDepth - 1, inlineDepth, index + [key], indexWidth, indent + "  "))
            block[-1][1] = block[-1][1].rstrip(",")
            return [[reprindex, indent + "{"]] + block + [["", indent + "}"]]
        else:
            return [[reprindex, indent + "{...}"]]

    elif isinstance(expr, (list, tuple)):
        if maxDepth > 0:
            block = []
            for i, value in enumerate(expr):
                block.extend(_look(_Item(value), maxDepth - 1, inlineDepth, index + [i], indexWidth, indent + "  "))
            block[-1][1] = block[-1][1].rstrip(",")
            return [[reprindex, indent + "["]] + block + [["", indent + "]"]]
        else:
            return [[reprindex, indent + "[...]"]]
