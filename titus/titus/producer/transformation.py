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

import math

import numpy

from titus.pfaast import Ast
from titus.pfaast import Call
from titus.pfaast import Ref
from titus.pfaast import LiteralNull
from titus.pfaast import LiteralBoolean
from titus.pfaast import LiteralInt
from titus.pfaast import LiteralLong
from titus.pfaast import LiteralFloat
from titus.pfaast import LiteralDouble
from titus.pfaast import LiteralString
from titus.datatype import AvroArray
from titus.datatype import AvroMap
from titus.datatype import AvroRecord
from titus.prettypfa import ppfa
from titus.prettypfa import pfa

class Transformation(object):
    """Represents an expression that can be applied to Numpy arrays in Python and included in a PFA scoring engine."""

    constants = {
        "m.pi": "math.pi",
        "m.e": "math.e",
        }

    functions = {
        "+": "numpy.add",
        "-": "numpy.subtract",
        "*": "numpy.multiply",
        "/": "numpy.true_divide",
        "u-": "numpy.negative",
        "**": "numpy.power",
        "%": "numpy.mod",
        "%%": "numpy.remainder",
        
        "==": "numpy.equal",
        "!=": "numpy.not_equal",
        ">": "numpy.greater",
        ">=": "numpy.greater_equal",
        "<": "numpy.less",
        "<=": "numpy.less_equal",
        "&&": "numpy.logical_and",
        "||": "numpy.logical_or",
        "^^": "numpy.logical_xor",
        "!": "numpy.logical_not",
        
        "m.abs": "numpy.absolute",
        "m.acos": "numpy.arccos",
        "m.asin": "numpy.arcsin",
        "m.atan": "numpy.arctan",
        "m.atan2": "numpy.arctan2",
        "m.ceil": "numpy.ceil",
        "m.copysign": "numpy.copysign",
        "m.cos": "numpy.cos",
        "m.cosh": "numpy.cosh",
        "m.exp": "numpy.exp",
        "m.expm1": "numpy.expm1",
        "m.floor": "numpy.floor",
        "m.hypot": "numpy.hypot",
        "m.ln": "numpy.log",
        "m.log10": "numpy.log10",
        "m.round": "numpy.round",
        "m.rint": "numpy.rint",
        "m.sin": "numpy.sin",
        "m.sinh": "numpy.sinh",
        "m.sqrt": "numpy.sqrt",
        "m.tan": "numpy.tan",
        "m.tanh": "numpy.tanh",
        }

    namespace = {"math": math, "numpy": numpy}

    @staticmethod
    def findFields(x):
        """Find the symbols (variables) referenced in a PFA expression.

        :type x: Pythonized JSON
        :param x: the PFA expression
        :rtype: list of strings
        :return: all symbols found
        """

        if isinstance(x, dict):
            return sum((Transformation.findFields(v) for k, v in x.items() if k != "@"), [])
        elif isinstance(x, (list, tuple)):
            return sum((Transformation.findFields(xi) for xi in x), [])
        elif isinstance(x, basestring):
            return [x]
        else:
            return []

    @staticmethod
    def replace(x, subs):
        """Apply replacements to a PFA expression.

        :type x: Pythonized JSON
        :param x: the PFA expression
        :type subs: dict from substitution identifier to its replacement value
        :param subs: identifiers to replace
        :rtype: Pythonized JSON
        :return: the PFA with replacements
        """

        if isinstance(x, dict):
            return dict((k, Transformation.replace(v, subs)) for k, v in x.items())
        elif isinstance(x, (list, tuple)):
            return [Transformation.replace(xi, subs) for xi in x]
        elif isinstance(x, basestring) and x in subs:
            return subs[x]
        else:
            return x

    @staticmethod
    def toNumpyExpr(ast):
        """Convert a PFA abstract syntax tree into a Numpy expression.

        :type ast: titus.pfaast.Ast
        :param ast: the PFA to convert
        :rtype: string
        :return: executable Python code in a string
        """

        if isinstance(ast, Call):
            if ast.name in Transformation.constants and len(ast.args) == 0:
                return Transformation.constants[ast.name]
            elif ast.name in Transformation.functions:
                return Transformation.functions[ast.name] + "(" + ", ".join(Transformation.toNumpyExpr(x) for x in ast.args) + ")"
            else:
                raise ValueError("No numpy equivalent defined for function {0}".format(ast.name))
        elif isinstance(ast, Ref):
            return ast.name
        elif isinstance(ast, LiteralNull):
            return float("nan")
        elif isinstance(ast, (LiteralBoolean, LiteralInt, LiteralLong, LiteralFloat, LiteralDouble, LiteralString)):
            return repr(ast.value)
        else:
            raise ValueError("No numpy equivalent defined for expression {0}".format(ast.toJson()))

    def __init__(self, *indexed, **named):
        """Create a Transformation either from an ordered list of Python expressions or by keywords.

        Although it's possible to mix the ordered list method and the keyword method, doing so could be confusing.

        Expressions loaded as positional arguments are given keyword names "_0", "_1", "_2", etc., and all methods proceed as though they were loaded as keywords.

        :type indexed: strings (as positional arguments)
        :param indexed: executable Python code in strings
        :type named: strings (as keyword arguments)
        :param named: executable Python code in strings
        """

        # indexed inputs come first, in order, and are labeled as _0, _1, _2, etc.
        self.exprs = named
        self.order = sorted(named.keys())
        for i, expr in enumerate(indexed):
            key = "_%d" % i
            self.exprs[key] = expr
            self.order.insert(i, key)

        # construct PFA from each expression
        self.pfas = dict((k, ppfa(v).jsonNode(False, set())) for k, v in self.exprs.items())

        # find all the fields referenced in all of the expressions
        self.fields = sorted(set(sum((Transformation.findFields(x) for x in self.pfas.values()), [])))

        # construct lambda functions for transforming Numpy
        self.lambdas = dict((k, eval("lambda " + ", ".join(self.fields) + ": " + Transformation.toNumpyExpr(ppfa(v)), self.namespace)) for k, v in self.exprs.items())

    def transform(self, dataset, fieldNames=None):
        """Return a transformed Numpy dataset (leaving the original intact).

        :type dataset: Numpy record array, dict of 1-D Numpy arrays, or Numpy 2-d table
        :param dataset: input dataset to be transformed; the Numpy record names or dict keys must correspond to the keywords of the arguments used to construct this ``Transformation``, or the column indexes of the 2-d table must correspond to the positions of the arguments used to construct this ``Transformation``.
        :rtype: same as ``dataset``
        :return: transformed dataset (operations are *not* performed in-place)
        """

        # option 1: dataset is a record array
        if isinstance(dataset, numpy.core.records.recarray):
            outType = "recarray"
            if fieldNames is None:
                fieldNames = dataset.dtype.names

        # option 2: dataset is a dictionary of 1-D arrays
        elif isinstance(dataset, dict) and all(isinstance(x, numpy.ndarray) and len(x.shape) == 1 for x in dataset.values()):
            outType = "dict"
            if fieldNames is None:
                fieldNames = dataset.keys()

        # option 3: dataset is a 2-D table
        elif isinstance(dataset, numpy.ndarray):
            outType = "array"
            if fieldNames is None:
                fieldNames = self.fields
            if len(dataset.shape) == 1:
                dataset = numpy.reshape(dataset, (dataset.shape[0], 1))
            if len(fieldNames) != dataset.shape[1]:
                raise TypeError("cannot interpret a {0}-column array as {1} fields".format(dataset.shape[1], len(fieldNames)))

        else:
            raise TypeError("expecting Numpy recarray, dictionary of 1-D arrays, or a 2-D array")

        # complain if any fields are not in this fieldNames
        cannotSupply = set(self.fields).difference(set(fieldNames))
        if len(cannotSupply) > 0:
            raise TypeError("expressions need [{0}], which are not supplied".format(", ".join(sorted(cannotSupply))))

        # evaluate the Numpy expressions
        if outType == "array":
            computed = dict((k, v(*[dataset[:, fieldNames.index(f)] for f in self.fields])) for k, v in self.lambdas.items())
        else:
            computed = dict((k, v(*[dataset[f] for f in self.fields])) for k, v in self.lambdas.items())

        # return the same type you received
        if outType == "recarray":
            return numpy.core.records.fromarrays([computed[x] for x in self.order], names=self.order)
        elif outType == "dict":
            return computed
        elif outType == "array":
            return numpy.vstack([computed[k] for k in self.order]).T

    @staticmethod
    def interpret(x):
        """Interpret expression from a PFA abstract syntax tree or PrettyPFA string.

        :type x: titus.datatype.Ast, PrettyPFA string
        :param x: input PFA
        :rtype: Pythonized JSON
        :return: the PFA as Pythonized JSON
        """

        if isinstance(x, Ast):
            return x.jsonNode(False, set())
        elif isinstance(x, basestring):
            return ppfa(x).jsonNode(False, set())
        else:
            return pfa(x).jsonNode(False, set())

    def new(self, avroType, subs={}, **subs2):
        """Construct a PFA "new" expression for this transformation.

        :type avroType: titus.datatype.AvroType
        :param avroType: type argument for the PFA "new" expression
        :type subs: dict of substitutions
        :param subs: subsititusions to apply
        :type subs2: dict of substitutions
        :param subs2: more subsititusions to apply
        :rtype: Pythonized JSON
        :return: PFA "new" expression
        """

        subs2.update(subs)
        for k, v in subs2.items():
            subs2[k] = Transformation.interpret(v)

        if isinstance(avroType, AvroArray):
            return {"type": avroType.jsonNode(set()),
                    "new": [Transformation.replace(self.pfas[k], subs2) for k in self.order]}
        elif isinstance(avroType, dict) and avroType["type"] == "array":
            return {"type": avroType,
                    "new": [Transformation.replace(self.pfas[k], subs2) for k in self.order]}

        elif isinstance(avroType, AvroMap):
            return {"type": avroType.jsonNode(set()),
                    "new": dict((k, Transformation.replace(v, subs2)) for k, v in self.pfas.items())}
        elif isinstance(avroType, dict) and avroType["type"] == "map":
            return {"type": avroType,
                    "new": dict((k, Transformation.replace(v, subs2)) for k, v in self.pfas.items())}

        elif isinstance(avroType, AvroRecord):
            return {"type": avroType.name,
                    "new": dict((k, Transformation.replace(v, subs2)) for k, v in self.pfas.items())}
        elif isinstance(avroType, dict) and avroType["type"] == "record" or isinstance(avroType, basestring):
            return {"type": avroType,
                    "new": dict((k, Transformation.replace(v, subs2)) for k, v in self.pfas.items())}

        else:
            raise TypeError("new can only be used to make an array, map, or record")

    def let(self, subs={}, **subs2):
        """Construct a PFA "let" expression for this transformation.

        :type subs: dict of substitutions
        :param subs: subsititusions to apply
        :type subs2: dict of substitutions
        :param subs2: more subsititusions to apply
        :rtype: Pythonized JSON
        :return: PFA "let" expression
        """

        subs2.update(subs)
        for k, v in subs2.items():
            subs2[k] = Transformation.interpret(v)

        return {"let": dict((k, Transformation.replace(v, subs2)) for k, v in self.pfas.items())}

    def expr(self, name="_0", subs={}, **subs2):
        """Construct a PFA expression for one of the expressions in this ``Transformation``.

        :type name: string
        :param name: expression to choose
        :type subs: dict of substitutions
        :param subs: subsititusions to apply
        :type subs2: dict of substitutions
        :param subs2: more subsititusions to apply
        :rtype: Pythonized JSON
        :return: PFA "let" expression
        """

        subs2.update(subs)
        for k, v in subs2.items():
            subs2[k] = Transformation.interpret(v)

        return Transformation.replace(self.pfas[name], subs2)

