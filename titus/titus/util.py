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

import inspect
import sys

TYPE_ERRORS_IN_PRETTYPFA = True
def ts(avroType):
    """Create a human-readable type string of a type.

    Note: if titus.util.TYPE_ERRORS_IN_PRETTYPFA is ``True``, the type will be printed in PrettyPFA notation; otherwise, as a raw Avro schema.

    :type avroType: titus.datatype.AvroType
    :param avroType: type to print out
    :rtype: string
    :return: string representation of the type.
    """

    if TYPE_ERRORS_IN_PRETTYPFA:
        pretty = avscToPretty(avroType.jsonNode(set()))
        if pretty.count("\n") > 0:
            pretty = "\n    " + pretty.replace("\n", "\n    ") + "\n"
        return pretty
    else:
        return repr(avroType)

uniqueEngineNameCounter = 0
def uniqueEngineName():
    """Provide an engine name, incrementing titus.util.uniqueEngineNameCounter to ensure uniqueness of values supplied by this function."""
    sys.modules["titus.util"].uniqueEngineNameCounter += 1
    return "Engine_{0}".format(sys.modules["titus.util"].uniqueEngineNameCounter)

uniqueRecordNameCounter = 0
def uniqueRecordName():
    """Provide a record type name, incrementing titus.util.uniqueRecordNameCounter to ensure uniqueness of values supplied by this function."""
    sys.modules["titus.util"].uniqueRecordNameCounter += 1
    return "Record_{0}".format(sys.modules["titus.util"].uniqueRecordNameCounter)

uniqueEnumNameCounter = 0
def uniqueEnumName():
    """Provide an enum type name, incrementing titus.util.uniqueEnumNameCounter to ensure uniqueness of values supplied by this function."""
    sys.modules["titus.util"].uniqueEnumNameCounter += 1
    return "Enum_{0}".format(sys.modules["titus.util"].uniqueEnumNameCounter)

uniqueFixedNameCounter = 0
def uniqueFixedName():
    """Provide a fixed type name, incrementing titus.util.uniqueFixedNameCounter to ensure uniqueness of values supplied by this function."""
    sys.modules["titus.util"].uniqueFixedNameCounter += 1
    return "Fixed_{0}".format(sys.modules["titus.util"].uniqueFixedNameCounter)

def pos(dot, at):
    """Create a human-readable position of a PFA element in a JSON file."""
    if at is None or at == "":
        return "at {0}".format("" if (dot == "") else "PFA field \"" + dot + "\"")
    else:
        return "at {0}{1}".format(at, "" if (dot == "") else " (PFA field \"" + dot + "\")")

def flatten(x):
    """General-purpose list-of-lists flattening.

    :type x: list of lists of something
    :param x: list of lists to flatten
    :rtype: list of that something
    :return: flattened list
    """
    return [item for sublist in x for item in sublist]

def div(numer, denom):
    """PFA division behavior: inf and -inf for a finite number divided by zero and nan for 0/0.

    Also ensures that return type of any two numbers (including integers) is floating point.

    :type numer: number
    :param numer: the numerator
    :type denom: number
    :param denom: the denominator
    :rtype: float
    :return: fraction, including non-numeric possibilities
    """

    try:
        return numer / float(denom)
    except ZeroDivisionError:
        if numer > 0.0:
            return float("inf")
        elif numer < 0.0:
            return float("-inf")
        else:
            return float("nan")

class DynamicScope(object):
    """Keep track of a variable scope dynamically, for the sake of simulating lexical scope in a PFA document (ironically)."""

    def __init__(self, parent):
        """:type parent: titus.util.DynamicScope or ``None``
        :param parent: scope to use for resolving variables not found in this scope
        """
        self.parent = parent
        self.symbols = dict()

    def get(self, symbol):
        """Look up a variable, returning its value.

        :type symbol: string
        :param symbol: variable name to look up
        :rtype: anything
        :return: variable value
        """
        if symbol in self.symbols:
            return self.symbols[symbol]
        elif self.parent is not None:
            return self.parent.get(symbol)
        else:
            raise RuntimeError()

    def let(self, nameExpr):
        """Create a new variable.

        :type nameExpr: dict from new variable names to their initial values
        :param nameExpr: variables and values to assign
        """
        for symbol, init in nameExpr.items():
            self.symbols[symbol] = init

    def set(self, nameExpr):
        """Reassign variables.

        :type nameExpr: dict from new variable names to their new values
        :param nameExpr: variables and values to assign
        """
        for symbol, value in nameExpr.items():
            if symbol in self.symbols:
                self.symbols[symbol] = value
            elif self.parent is not None:
                self.parent.set(nameExpr)
            else:
                raise RuntimeError()

def callfcn(state, scope, fcn, args):
    """Helper function for calling function callbacks.

    Used in library functions that have been given functions as arguments.

    :type state: titus.genpy.ExecutionState
    :param state: execution state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type fcn: callable
    :param fcn: function to call (must be a PFA user-defined function or library function)
    :type args: list of values
    :param args: arguments to pass to the function (other than the ``state`` and ``scope``)
    :rtype: anything
    :return: whatever ``fcn`` returns
    """

    if hasattr(fcn, "paramNames"):
        callScope = DynamicScope(scope)
        if isinstance(args, (tuple, list)):
            args = dict(zip(fcn.paramNames, args))
        callScope.let(args)
        return fcn(state, callScope)
    else:
        return fcn(state, scope, pos, None, *args)

def negativeIndex(length, index):
    """Helper function for interpreting negative array indexes as counting from the end of the array (just like Python).

    :type length: non-negative integer
    :param length: length of the array in question
    :type index: integer
    :param index: index to interpret
    :rtype: non-negative integer
    :return: if negative, count from the end of the array; otherwise, pass-through
    """

    if index >= 0:
        return index
    else:
        return length + index

def startEnd(length, start, end):
    """Helper function to normalize the start and end of an array by counting from the end of the array (just like Python).

    :type length: non-negative integer
    :param length: length of the array in question
    :type start: integer
    :param start: starting index to interpret
    :type end: integer
    :param end: ending index to interpret
    :rtype: (non-negative integer, non-negative integer)
    :return: (normalized starting index, normalized ending index)
    """

    if start >= 0:
        normStart = start
    else:
        normStart = length + start

    if normStart < 0:
        normStart = 0
    if normStart > length:
        normStart = length

    if end >= 0:
        normEnd = end
    else:
        normEnd = length + end

    if normEnd < 0:
        normEnd = 0
    if normEnd > length:
        normEnd = length
    if normEnd < normStart:
        normEnd = normStart

    return normStart, normEnd

def case(clazz):
    """Decoration to make a "case class" in Python.

    When applied to a class, read the parameters of the ``__init__`` and turn them into class fields and show them in the ``__repr__`` representation.
    """

    fields = [x for x in inspect.getargspec(clazz.__init__).args[1:] if x != "pos"]

    try:
        code = clazz.__init__.__func__
    except AttributeError:
        code = clazz.__init__.__code__

    context = dict(globals())
    context[clazz.__name__] = clazz

    if "pos" in inspect.getargspec(clazz.__init__).args:
        argFields = fields + ["pos=None"]
        assignFields = fields + ["pos"]
    else:
        argFields = assignFields = fields

    newMethods = {}
    exec("""
def __init__(self, {args}):
{assign}
    self._init({args})
""".format(args=", ".join(argFields),
           assign="\n".join(["    self.{0} = {0}".format(x) for x in (assignFields)])),
         context,
         newMethods)
    
    if len(fields) == 0:
        exec("""
def __repr__(self):
    return \"{0}()\"
""".format(clazz.__name__), context, newMethods)

        exec("""
def __eq__(self, other):
    return isinstance(other, {0})
""".format(clazz.__name__), context, newMethods)

    else:
        exec("""
def __repr__(self):
    return \"{0}(\" + {1} + \")\"
""".format(clazz.__name__, "+ \", \" + ".join(["repr(self." + x + ")" for x in fields])),
             context,
             newMethods)

        exec("""
def __eq__(self, other):
    if isinstance(other, {0}):
        return {1}
    else:
        return False
""".format(clazz.__name__, " and ".join(["self.{x} == other.{x}".format(x=x) for x in fields])),
             context,
             newMethods)

    clazz._init = clazz.__init__
    clazz.__init__ = newMethods["__init__"]

    if hasattr(clazz, "toString"):
        clazz.__repr__ = clazz.toString
    else:
        clazz.__repr__ = newMethods["__repr__"]

    if hasattr(clazz, "equals"):
        clazz.__eq__ = clazz.equals
    else:
        clazz.__eq__ = newMethods["__eq__"]

    return clazz

def untagUnion(expr, unionTypes):
    """Turn a tagged union datum into a bare Python object or pass-through if not a tagged union datum.

    :type expr: anything
    :param expr: tagged union, which may be of the form ``{"tag": value}`` or just ``value``
    :type unionTypes: list of Pythonized JSON
    :param unionTypes: types allowed by this union in Pythonized JSON schemas
    :rtype: anything
    :return: ``{"tag": value}`` collapsed to ``value`` if ``tag`` names one of the ``unionTypes``; otherwise, pass-through
    """

    if isinstance(expr, dict) and len(expr) == 1:
        tag, = expr.keys()
        value, = expr.values()
        for expectedTag in unionTypes:
            if isinstance(expectedTag, dict):
                if expectedTag["type"] in ("record", "enum", "fixed"):
                    if "namespace" in expectedTag and expectedTag["namespace"].strip() != "":
                        expectedTag = expectedTag["namespace"] + "." + expectedTag["name"]
                    else:
                        expectedTag = expectedTag["name"]
                else:
                    expectedTag = expectedTag["type"]
            if tag == expectedTag:
                return value
    return expr

def avscToPretty(avsc, indent=0):
    """Turn an Avro type from Pythonized JSON into a more human-readable PrettyPFA string.

    :type avsc: Pythonized JSON
    :param avsc: Avro type schema
    :type indent: non-negative integer
    :param indent: number of spaces to indent (default 0)
    :rtype: string
    :return: PrettyPFA representation
    """

    if isinstance(avsc, basestring):
        return " " * indent + avsc
    elif isinstance(avsc, dict) and "type" in avsc:
        tpe = avsc["type"]

        if tpe == "function":
            return " " * indent + "fcn(" + ", ".join("arg{}: {}".format(i + 1, avscToPretty(x)) for i, x in enumerate(avsc["params"])) + " -> " + avscToPretty(avsc["ret"]) + ")"

        elif tpe == "exception":
            return "exception"

        elif tpe in ("null", "boolean", "int", "long", "float", "double", "bytes", "string"):
            return " " * indent + tpe

        elif tpe == "array":
            return " " * indent + "array(" + avscToPretty(avsc["items"], indent + 6).lstrip() + ")"

        elif tpe == "map":
            return " " * indent + "map(" + avscToPretty(avsc["values"], indent + 4).lstrip() + ")"

        elif tpe == "record":
            name = avsc["name"]
            if "namespace" in avsc and len(avsc["namespace"]) > 0:
                name = avsc["namespace"] + "." + name
            fields = []
            for f in avsc["fields"]:
                fields.append(" " * (indent + 7) + f["name"] + ": " + avscToPretty(f["type"], indent + 7 + len(f["name"]) + 2).lstrip())
            return " " * indent + "record(" + name + ",\n" + ",\n".join(fields) + ")"

        elif tpe == "fixed":
            name = avsc["name"]
            if "namespace" in avsc and len(avsc["namespace"]) > 0:
                name = avsc["namespace"] + "." + name
            return " " * indent + "fixed(" + str(avsc["size"]) + ", " + name + ")"

        elif tpe == "enum":
            name = avsc["name"]
            if "namespace" in avsc and len(avsc["namespace"]) > 0:
                name = avsc["namespace"] + "." + name
            return " " * indent + "enum([" + ", ".join(x for x in avsc["symbols"]) + "], " + name + ")"

        else:
            raise TypeError("malformed Avro schema")

    elif isinstance(avsc, (list, tuple)):
        variants = []
        for x in avsc:
            variants.append(avscToPretty(x, indent + 6))
        return " " * indent + "union(" + ",\n".join(variants).lstrip() + ")"

    else:
        raise TypeError("malformed Avro schema")
