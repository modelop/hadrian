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

import base64
import json
import re
from collections import OrderedDict

import six

import titus.lib.core
import titus.lib.pfamath
import titus.lib.spec
import titus.lib.link
import titus.lib.kernel
import titus.lib.la
import titus.lib.metric
import titus.lib.rand
import titus.lib.pfastring
import titus.lib.regex
import titus.lib.parse
import titus.lib.cast
import titus.lib.array
import titus.lib.map
import titus.lib.bytes
import titus.lib.fixed
import titus.lib.enum
import titus.lib.pfatime
import titus.lib.impute
import titus.lib.interp
import titus.lib.prob.dist
import titus.lib.stat.pfatest
import titus.lib.stat.sample
import titus.lib.stat.change
import titus.lib.model.reg
import titus.lib.model.tree
import titus.lib.model.cluster
import titus.lib.model.neighbor
import titus.lib.model.naive
import titus.lib.model.neural
import titus.lib.model.svm

import titus.P as P
import titus.util

from titus.errors import *
from titus.fcn import Fcn
from titus.signature import IncompatibleTypes
from titus.signature import LabelData
from titus.signature import Sig
from titus.signature import PFAVersion
from titus.datatype import *
import titus.options
from titus.util import ts

############################################################ utils

def inferType(expr, symbols=None, cells=None, pools=None, fcns=None, version=PFAVersion(0, 0, 0)):
    """Utility function to infer the type of a given expression.

    :type expr: titus.pfaast.Expression
    :param expr: expression to examine
    :type symbols: dict from symbol name to titus.datatype.AvroType
    :param symbols: data types of variables used in the expression
    :type cells: dict from cell name to titus.datatype.AvroType
    :param cells: data types of cells used in the expression
    :type pools: dict from pool name to titus.datatype.AvroType
    :param pools: data types of pools used in the expression
    :type fcns: dict from function name to titus.fcn.Fcn
    :param fcns: functions used in the expression
    :rtype: titus.datatype.AvroType
    :return: data type of the expression's return value
    """

    if symbols is None:
        symbols = {}
    if cells is None:
        cells = {}
    if pools is None:
        pools = {}
    if fcns is None:
        fcns = {}

    symbolTable = SymbolTable(None, symbols, cells, pools, True, False)
    functionTable = FunctionTable.blank()
    functionTable.functions.update(fcns)

    context, result = expr.walk(NoTask(), symbolTable, functionTable, titus.options.EngineOptions(None, None), version)
    return context.retType

############################################################ symbols

def validSymbolName(test):
    """Determine if a symbol name is valid.

    :type test: string
    :param test: symbol (variable) name to check
    :rtype: bool
    :return: ``True`` if valid; ``False`` otherwise
    """
    return re.match("^[A-Za-z_][A-Za-z0-9_]*$", test) is not None

class SymbolTable(object):
    """Represents the symbols (variables) and their data types in a lexical scope."""

    def __init__(self, parent, symbols, cells, pools, sealedAbove, sealedWithin):
        """:type parent: titus.pfaast.SymbolTable or ``None``
        :param parent: enclosing scope; symbol lookup defers to the parent scope if not found here
        :type symbols: dict from symbol name to titus.datatype.AvroType
        :param symbols: initial symbol names and their types
        :type cells: dict from cell name to titus.datatype.AvroType
        :param cells: initial cell names and their types
        :type pools: dict from pool name to titus.datatype.AvroType
        :param pools: initial pool names and their types
        :type sealedAbove: bool
        :param sealedAbove: if ``True``, symbols in the parent scope cannot be modified (but can be accessed); if ``False``, this scope does not restrict access (but a parent might)
        :type sealedWithin: bool
        :param sealedWithin: if ``True``, new symbols cannot be created in this scope
        """
        self.parent = parent
        self.symbols = symbols
        self.cells = cells
        self.pools = pools
        self.sealedAbove = sealedAbove
        self.sealedWithin = sealedWithin

    def getLocal(self, name):
        """Get a symbol's type specifically from *this* scope.

        :type name: string
        :param name: name of the symbol
        :rtype: titus.datatype.AvroType or ``None``
        :return: the symbol's type if defined in *this* scope, ``None`` otherwise
        """
        if name in self.symbols:
            return self.symbols[name]
        else:
            return None

    def getAbove(self, name):
        """Get a symbol's type specifically from a *parent's* scope, not this one.

        :type name: string
        :param name: name of the symbol
        :rtype: titus.datatype.AvroType or ``None``
        :return: the symbol's type if defined in a *parent's* scope, ``None`` otherwise
        """
        if self.parent is None:
            return None
        else:
            return self.parent.get(name)

    def get(self, name):
        """Get a symbol's type from this scope or a parent's.

        :type name: string
        :param name: name of the symbol
        :rtype: titus.datatype.AvroType or ``None``
        :return: the symbol's type if defined, ``None`` otherwise
        """
        out = self.getLocal(name)
        if out is None:
            return self.getAbove(name)
        else:
            return out

    def __call__(self, name):
        """Get a symbol's type from this scope or a parent's and raise a ``KeyError`` if not defined

        :type name: string
        :param name: name of the symbol
        :rtype: titus.datatype.AvroType
        :return: the symbol's type if defined, raise a ``KeyError`` otherwise
        """
        out = self.get(name)
        if out is None:
            raise KeyError("no symbol named \"{0}\"".format(name))
        else:
            return out

    def writable(self, name):
        """Determine if a symbol can be modified in this scope.

        :type name: string
        :param name: name of the symbol
        :rtype: bool
        :return: ``True`` if the symbol can be modified; ``False`` otherwise
        """
        if self.sealedWithin:
            return False
        else:
            if name in self.symbols:
                return True
            else:
                if self.sealedAbove:
                    return False
                else:
                    if self.parent is None:
                        raise ValueError("no symbol named \"{0}\"".format(name))
                    else:
                        return self.parent.writable(name)

    def put(self, name, avroType):
        """Create or overwrite a symbol's type in the table.

        :type name: string
        :param name: name of the symbol
        :type avroType: titus.datatype.AvroType
        :param avroType: the data type to associate with this symbol
        :rtype: ``None``
        :return: nothing; changes table in-place
        """
        self.symbols[name] = avroType

    def cell(self, name):
        """Get a cell's type from this scope or a parent's.

        :type name: string
        :param name: name of the cell
        :rtype: titus.datatype.AvroType or ``None``
        :return: the cell's type if defined, ``None`` otherwise
        """
        if name in self.cells:
            return self.cells[name]
        elif self.parent is not None:
            return self.parent.cell(name)
        else:
            return None

    def pool(self, name):
        """Get a pool's type from this scope or a parent's.

        :type name: string
        :param name: name of the pool
        :rtype: titus.datatype.AvroType or ``None``
        :return: the pool's type if defined, ``None`` otherwise
        """
        if name in self.pools:
            return self.pools[name]
        elif self.parent is not None:
            return self.parent.pool(name)
        else:
            return None
        
    def newScope(self, sealedAbove, sealedWithin):
        """Create a new scope with this as parent.

        :type sealedAbove: bool
        :param sealedAbove: if ``True``, symbols in the parent scope cannot be modified (but can be accessed); if ``False``, this scope does not restrict access (but a parent might)
        :type sealedWithin: bool
        :param sealedWithin: if ``True``, new symbols cannot be created in this scope
        :rtype: titus.pfaast.SymbolTable
        :return: a new scope, linked to this one
        """
        return SymbolTable(self, {}, {}, {}, sealedAbove, sealedWithin)

    @property
    def inThisScope(self):
        """All symbols (and their types) that are defined in this scope (*not* in any parents).

        :rtype: dict from symbol name to titus.datatype.AvroType
        :return: symbols and their types
        """
        return self.symbols

    @property
    def allInScope(self):
        """All symbols (and their types) that are defined in this scope and all parents.

        :rtype: dict from symbol name to titus.datatype.AvroType
        :return: symbols and their types
        """
        if self.parent is None:
            out = {}
        else:
            out = self.parent.allInScope(self)

        out.update(self.symbols)
        return out

    @staticmethod
    def blank():
        """Create a blank symbol table.

        :rtype: titus.pfaast.SymbolTable
        :return: a symbol table containing nothing
        """
        return SymbolTable(None, {}, {}, {}, True, False)

############################################################ functions

def validFunctionName(test):
    """Determine if a function name is valid.

    :type test: string
    :param test: function name to check
    :rtype: bool
    :return: ``True`` if valid; ``False`` otherwise
    """
    return re.match("^[A-Za-z_]([A-Za-z0-9_]|\\.[A-Za-z][A-Za-z0-9_]*)*$", test) is not None

class UserFcn(Fcn):
    """Represents a user-defined function."""
    def __init__(self, name, sig):
        """:type name: string
        :param name: name of the function
        :type sig: titus.signature.Sig
        :param sig: function signature (note that titus.signature.Sigs is not allowed for user-defined functions)
        """
        self.name = name
        self.sig = sig

    def genpy(self, paramTypes, args, pos=None):
        """Generate an executable Python string for this function; usually ```call(state, DynamicScope(None), self.f["function name"], {arguments...})```."""
        parNames = [list(x.keys())[0] for x in self.sig.params]
        return "call(state, DynamicScope(None), self.f[" + repr(self.name) + "], {" + ", ".join([repr(k) + ": " + v for k, v in zip(parNames, args)]) + "})"

    @staticmethod
    def fromFcnDef(n, fcnDef):
        """Create an executable function object from an abstract syntax tree of a function definition.

        :type n: string
        :param n: name of the new function
        :type fcnDef: titus.pfaast.FcnDef
        :param fcnDef: the abstract syntax tree function definition
        :rtype: titus.pfaast.UserFcn
        :return: the executable function
        """
        return UserFcn(n, Sig([{list(t.keys())[0]: P.fromType(list(t.values())[0])} for t in fcnDef.params], P.fromType(fcnDef.ret)))

class EmitFcn(Fcn):
    """The special ``emit`` function."""

    def __init__(self, outputType):
        """:type outputType: titus.datatype.AvroType
        :param outputType: output type of the PFA document, which is also the signature of the ``emit`` function
        """
        self.sig = Sig([{"output": P.fromType(outputType)}], P.Null())

    def genpy(self, paramTypes, args, pos=None):
        """Generate an executable Python string for this function; usually ``self.f["emit"].engine.emit(argument)``."""
        return "self.f[\"emit\"].engine.emit(" + args[0] + ")"

class FunctionTable(object):
    """Represents a table of all accessible PFA function names, such as library functions, user-defined functions, and possibly emit."""

    def __init__(self, functions):
        """:type functions: dict from function name to titus.fcn.Fcn
        :param functions: function lookup table
        """
        self.functions = functions

    @staticmethod
    def blank():
        """Create a function table containing nothing but library functions.

        This is where all the PFA library modules are enumerated.
        """

        functions = {}
        functions.update(titus.lib.core.provides)
        functions.update(titus.lib.pfamath.provides)
        functions.update(titus.lib.spec.provides)
        functions.update(titus.lib.link.provides)
        functions.update(titus.lib.kernel.provides)
        functions.update(titus.lib.la.provides)
        functions.update(titus.lib.metric.provides)
        functions.update(titus.lib.rand.provides)
        functions.update(titus.lib.pfastring.provides)
        functions.update(titus.lib.regex.provides)
        functions.update(titus.lib.parse.provides)
        functions.update(titus.lib.cast.provides)
        functions.update(titus.lib.array.provides)
        functions.update(titus.lib.map.provides)
        functions.update(titus.lib.bytes.provides)
        functions.update(titus.lib.fixed.provides)
        functions.update(titus.lib.enum.provides)
        functions.update(titus.lib.pfatime.provides)
        functions.update(titus.lib.impute.provides)
        functions.update(titus.lib.interp.provides)
        functions.update(titus.lib.prob.dist.provides)
        functions.update(titus.lib.stat.pfatest.provides)
        functions.update(titus.lib.stat.sample.provides)
        functions.update(titus.lib.stat.change.provides)
        functions.update(titus.lib.model.reg.provides)
        functions.update(titus.lib.model.tree.provides)
        functions.update(titus.lib.model.cluster.provides)
        functions.update(titus.lib.model.neighbor.provides)
        functions.update(titus.lib.model.naive.provides)
        functions.update(titus.lib.model.neural.provides)
        functions.update(titus.lib.model.svm.provides)

        # TODO: functions.update(titus.lib.other.provides)...

        return FunctionTable(functions)

############################################################ type-checking and transforming ASTs

class AstContext(object):
    """Trait for titus.pfaast.Ast context classes."""
    pass

class ArgumentContext(AstContext):
    """Subtrait for context classes of titus.pfaast.Argument."""
    def calls(self):
        raise NotImplementedError

class ExpressionContext(ArgumentContext):
    """Subtrait for context classes of titus.pfaast.Expression."""
    def retType(self):
        raise NotImplementedError

class FcnContext(ArgumentContext):
    """Subtrait for context classes of titus.pfaast.FcnDef."""
    def fcnType(self):
        raise NotImplementedError

class TaskResult(object):
    """Trait for result of a generic task, passed to titus.pfaast.Ast ``walk``."""
    pass

class Task(object):
    """Trait for a generic task, passed to titus.pfaast.Ast ``walk``."""
    def __call__(self, astContext, engineOptions, resoledType=None):
        """Perform a task from context generated by a titus.pfaast.Ast ``walk``.

        :type astContext: titus.pfaast.AstContext
        :param astContext: data about the titus.pfaast.Ast node after type-checking
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type resolvedType: titus.datatype.AvroType or ``None``
        :param resolvedType: ?
        :rtype: titus.pfaast.TaskResult
        :return: the result of this task
        """
        raise NotImplementedError

class NoTask(Task):
    """Concrete titus.pfaast.Task that does nothing, used for type-checking without producing an engine."""

    class EmptyResult(TaskResult):
        """Concrete titus.pfaast.TaskResult that contains no result."""
        pass

    def __call__(self, astContext, engineOptions, resolvedType=None):
        """Do nothing.

        :type astContext: titus.pfaast.AstContext
        :param astContext: data about the titus.pfaast.Ast node after type-checking
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type resolvedType: titus.datatype.AvroType or ``None``
        :param resolvedType: ?
        :rtype: titus.pfaast.NoTask.EmptyResult
        :return: empty result object
        """
        return self.EmptyResult()

def check(engineConfig, version):
    """Check a PFA abstract syntax tree for semantic errors using a titus.pfaast.NoTask to avoid unnecessary work.

    Raises an exception if the document is not valid.

    :type engineConfig: titus.pfaast.EngineConfig
    :param engineConfig: abstract syntax tree for a complete PFA document
    :type version: titus.signature.PFAVersion
    :param version: PFA version in which to interpret the document
    :rtype: ``None``
    :return: nothing if the PFA document is valid; raises an exception otherwise
    """
    engineConfig.walk(NoTask(),
                      SymbolTable.blank(),
                      FunctionTable.blank(),
                      titus.options.EngineOptions(engineConfig.options, None),
                      version)

def isValid(engineConfig, version):
    """Check a PFA abstract syntax tree for semantic errors using a titus.pfaast.NoTask to avoid unnecessary work.

    :type engineConfig: titus.pfaast.EngineConfig
    :param engineConfig: abstract syntax tree for a complete PFA document
    :type version: titus.signature.PFAVersion
    :param version: PFA version in which to interpret the document
    :rtype: bool
    :return: ``True`` if the PFA document is valid; ``False`` otherwise
    """
    try:
        check(engineConfig, version)
    except (PFASyntaxException, PFASemanticException, PFAInitializationException):
        return False
    else:
        return True

############################################################ AST nodes

class Ast(object):
    """Abstract base class for a PFA abstract syntax tree."""

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        if pf.isDefinedAt(self):
            return [pf(self)]
        else:
            return []

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return self

    def walk(self, task, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: task to perform
        :type version: titus.signature.PFAVersion
        :param version: PFA language version in which to interpret this PFA
        """
        self.walk(task, SymbolTable.blank(), FunctionTable.blank(), titus.options.EngineOptions(None, None), version)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        raise NotImplementedError

    def toJson(self, lineNumbers=True):
        """Serialize this abstract syntax tree as a JSON string.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks at the beginning of each JSON object
        :rtype: string
        :return: JSON string
        """
        return json.dumps(self.jsonNode(lineNumbers, set()))

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        raise NotImplementedError

    def startDict(self, lineNumbers):
        """Helper function to build a Pythonized JSON object.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include the locator mark
        :rtype: ``OrderedDict``
        :return: an empty Pythonized JSON object or one containing only the locator mark
        """
        if lineNumbers and self.pos is not None:
            return OrderedDict({"@": self.pos})
        else:
            return OrderedDict()

class Subs(Ast):
    """Represents an unresolved substitution in the abstract syntax tree.

    PrettyPFA can make substitutions with ``<<French quotes>>``.

    A PFA document is not valid if it contains any unresolved substitutions.
    """

    def __init__(self, name, lineno=None):
        """:type name: string
        :param name: key for substitution
        :type lineno: integer or ``None``
        :param lineno: line number
        """
        self.name = name
        self.pos = "Substitution at line " + str(lineno)
        self.lineno = lineno
        self.low = lineno
        self.high = lineno
        self.context = None

    def asExpr(self, state):
        """Mini-AST method for representing as an expression."""
        self.context = "expr"
        return self

    def asType(self, state):
        """Mini-AST method for representing as a type."""
        self.context = "type"
        return self

    def walk(self, task, version):
        """Raises a syntax exception because unresolved substitutions are not allowed in valid PFA."""
        raise PFASyntaxException("Unresolved substitution \"{0}\"".format(self.name), self.pos)

    def toJson(self, lineNumbers=True):
        """Raises a syntax exception because unresolved substitutions are not allowed in valid PFA."""
        raise PFASyntaxException("Unresolved substitution \"{0}\"".format(self.name), self.pos)

    def jsonNode(self, lineNumbers, memo):
        """Inserts ``self`` into the Pythonized JSON, which would make it unserializable."""
        return self

    def __repr__(self):
        return "<<" + self.name + ">>"
        
class Method(object):
    """PFA execution method may be "map", "emit", or "fold"."""
    MAP = "map"
    EMIT = "emit"
    FOLD = "fold"

@titus.util.case
class EngineConfig(Ast):
    """Abstract syntax tree for a whole PFA document."""
    def __init__(self,
                 name,
                 method,
                 inputPlaceholder,
                 outputPlaceholder,
                 begin,
                 action,
                 end,
                 fcns,
                 zero,
                 merge,
                 cells,
                 pools,
                 randseed,
                 doc,
                 version,
                 metadata,
                 options,
                 pos=None):
        """:type name: string
        :param name: name of the PFA engine (may be auto-generated)
        :type method: titus.pfaast.Method string
        :param method: execution method ("map", "emit", or "fold")
        :type inputPlaceholder: titus.datatype.AvroPlaceholder
        :param inputPlaceholder: input type as a placeholder (so it can exist before type resolution)
        :type outputPlaceholder: titus.datatype.AvroPlaceholder
        :param outputPlaceholder: output type as a placeholder (so it can exist before type resolution)
        :type begin: list of titus.pfaast.Expression
        :param begin: ``begin`` algorithm
        :type action: list of titus.pfaast.Expression
        :param action: ``action`` algorithm
        :type end: list of titus.pfaast.Expression
        :param end: ``end`` algorithm
        :type fcns: dict of titus.pfaast.FcnDef
        :param fcns: user-defined functions
        :type zero: string or ``None``
        :param zero: initial value for "fold" ``tally``
        :type merge: list of titus.pfaast.Expression or ``None``
        :param merge: ``merge`` algorithm for "fold"
        :type cells: dict of titus.pfaast.Cell
        :param cells: ``cell`` definitions
        :type pools: dict of titus.pfaast.Pool
        :param pools: ``pool`` definitions
        :type randseed: integer or ``None``
        :param randseed: random number seed
        :type doc: string or ``None``
        :param doc: optional documentation string
        :type version: integer or ``None``
        :param version: optional version number
        :type metadata: dict of string
        :param metadata: computer-readable documentation
        :type options: dict of Pythonized JSON
        :param options: implementation options
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

        if method not in (Method.MAP, Method.EMIT, Method.FOLD):
            raise PFASyntaxException("\"method\" must be \"map\", \"emit\", or \"fold\"", pos)

        if not isinstance(inputPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"inputPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(outputPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"outputPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(begin, (list, tuple)) or not all(isinstance(x, Expression) for x in begin):
            raise PFASyntaxException("\"begin\" must be a list of Expressions", pos)

        if not isinstance(action, (list, tuple)) or not all(isinstance(x, Expression) for x in action):
            raise PFASyntaxException("\"action\" must be a list of Expressions", pos)

        if not isinstance(end, (list, tuple)) or not all(isinstance(x, Expression) for x in end):
            raise PFASyntaxException("\"end\" must be a list of Expressions", pos)

        if not isinstance(fcns, dict) or not all(isinstance(x, FcnDef) for x in fcns.values()):
            raise PFASyntaxException("\"fcns\" must be a dictionary of FcnDefs", pos)

        if not isinstance(zero, six.string_types) and not zero is None:
            raise PFASyntaxException("\"zero\" must be a string or None", pos)

        if (not isinstance(merge, (list, tuple)) or not all(isinstance(x, Expression) for x in merge)) and not merge is None:
            raise PFASyntaxException("\"merge\" must be list of Expressions or None", pos)

        if not isinstance(cells, dict) or not all(isinstance(x, Cell) for x in cells.values()):
            raise PFASyntaxException("\"cells\" must be a dictionary of Cells", pos)

        if not isinstance(pools, dict) or not all(isinstance(x, Pool) for x in pools.values()):
            raise PFASyntaxException("\"pools\" must be a dictionary of Pools", pos)

        if not isinstance(randseed, int) and not randseed is None:
            raise PFASyntaxException("\"randseed\" must be an int or None", pos)

        if not isinstance(doc, six.string_types) and not doc is None:
            raise PFASyntaxException("\"doc\" must be a string or None", pos)

        if not isinstance(version, int) and not version is None:
            raise PFASyntaxException("\"version\" must be an int or None", pos)

        if not isinstance(metadata, dict) or not all(isinstance(x, six.string_types) for x in metadata.values()):
            raise PFASyntaxException("\"metadata\" must be a dictionary of strings", pos)

        if not isinstance(options, dict):
            raise PFASyntaxException("\"options\" must be a dictionary", pos)

        if len(self.action) < 1:
            raise PFASyntaxException("\"action\" must contain least one expression", pos)

        if method == Method.FOLD and (zero is None or merge is None):
            raise PFASyntaxException("folding engines must include \"zero\" and \"merge\" top-level fields", pos)

        if method != Method.FOLD and (zero is not None or merge is not None):
            raise PFASyntaxException("non-folding engines must not include \"zero\" and \"merge\" top-level fields", pos)

        if merge is not None and len(merge) < 1:
            raise PFASyntaxException("\"merge\" must contain least one expression", pos)

    def toString(self):
        return '''EngineConfig(name={name},
    method={method},
    inputPlaceholder={inputPlaceholder},
    outputPlaceholder={outputPlaceholder},
    begin={begin},
    action={action},
    end={end},
    fcns={fcns},
    zero={zero},
    merge={merge},
    cells={cells},
    pools={pools},
    randseed={randseed},
    doc={doc},
    version={version},
    metadata={metadata},
    options={options})'''.format(name=self.name,
                         method=self.method,
                         inputPlaceholder=self.inputPlaceholder,
                         outputPlaceholder=self.outputPlaceholder,
                         begin=self.begin,
                         action=self.action,
                         end=self.end,
                         fcns=self.fcns,
                         zero=self.zero,
                         merge=self.merge,
                         cells=self.cells,
                         pools=self.pools,
                         randseed=self.randseed,
                         doc=self.doc,
                         version=self.version,
                         metadata=self.metadata,
                         options=self.options)

    @property
    def input(self):
        """Input type after type resolution (titus.datatype.AvroType)."""
        return self.inputPlaceholder.avroType

    @property
    def output(self):
        """Output type after type resolution (titus.datatype.AvroType)."""
        return self.outputPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(EngineConfig, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.begin) + \
               titus.util.flatten(x.collect(pf) for x in self.action) + \
               titus.util.flatten(x.collect(pf) for x in self.end) + \
               titus.util.flatten(x.collect(pf) for x in self.fcns.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.cells.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.pools.values())

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return EngineConfig(self.name,
                                self.method,
                                self.inputPlaceholder,
                                self.outputPlaceholder,
                                [x.replace(pf) for x in self.begin],
                                [x.replace(pf) for x in self.action],
                                [x.replace(pf) for x in self.end],
                                dict((k, v.replace(pf)) for k, v in self.fcns.items()),
                                self.zero,
                                self.merge,
                                dict((k, v.replace(pf)) for k, v in self.cells.items()),
                                dict((k, v.replace(pf)) for k, v in self.pools.items()),
                                self.randseed,
                                self.doc,
                                self.version,
                                self.metadata,
                                self.options,
                                self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, pfaVersion):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        topWrapper = SymbolTable(symbolTable, {}, self.cells, self.pools, True, False)

        userFunctions = {}
        for fname, fcnDef in self.fcns.items():
            ufname = "u." + fname
            if not validFunctionName(ufname):
                raise PFASemanticException("\"{0}\" is not a valid function name".format(fname), self.pos)
            userFunctions[ufname] = UserFcn.fromFcnDef(ufname, fcnDef)

        if self.method == Method.EMIT:
            emitFcn = {"emit": EmitFcn(self.output)}
        else:
            emitFcn = {}

        withUserFunctions = FunctionTable(dict(list(functionTable.functions.items()) + list(userFunctions.items()) + list(emitFcn.items())))

        userFcnContexts = []
        for fname, fcnDef in self.fcns.items():
            ufname = "u." + fname
            scope = topWrapper.newScope(True, False)
            fcnContext, fcnResult = fcnDef.walk(task, scope, withUserFunctions, engineOptions, pfaVersion)
            userFcnContexts.append((ufname, fcnContext))

        beginScopeWrapper = topWrapper.newScope(True, False)
        beginScopeWrapper.put("name", AvroString())
        beginScopeWrapper.put("instance", AvroInt())
        if self.version is not None:
            beginScopeWrapper.put("version", AvroInt())
        beginScopeWrapper.put("metadata", AvroMap(AvroString()))
        beginScope = beginScopeWrapper.newScope(True, False)

        beginContextResults = [x.walk(task, beginScope, withUserFunctions, engineOptions, pfaVersion) for x in self.begin]
        beginResults = [x[1] for x in beginContextResults]
        beginCalls = set(titus.util.flatten([x[0].calls for x in beginContextResults]))

        if self.merge is not None:
            mergeScopeWrapper = topWrapper.newScope(True, False)
            mergeScopeWrapper.put("tallyOne", self.output)
            mergeScopeWrapper.put("tallyTwo", self.output)
            mergeScopeWrapper.put("name", AvroString())
            mergeScopeWrapper.put("instance", AvroInt())
            if self.version is not None:
                mergeScopeWrapper.put("version", AvroInt())
            mergeScopeWrapper.put("metadata", AvroMap(AvroString()))
            mergeScope = mergeScopeWrapper.newScope(True, False)

            mergeContextResults = [x.walk(task, mergeScope, withUserFunctions, engineOptions, pfaVersion) for x in self.merge]
            mergeCalls = set(titus.util.flatten([x[0].calls for x in mergeContextResults]))

            if not self.output.accepts(mergeContextResults[-1][0].retType):
                raise PFASemanticException("merge's inferred output type is {0} but the declared output type is {1}".format(ts(mergeContextResults[-1][0].retType), ts(self.output)), self.pos)

            mergeOption = ([x[1] for x in mergeContextResults],
                           dict(list(mergeScopeWrapper.inThisScope.items()) + list(mergeScope.inThisScope.items())),
                           mergeCalls)
        else:
            mergeOption = None

        # this will go into end and action, but not begin or merge
        if self.method == Method.FOLD:
            topWrapper.put("tally", self.output)

        endScopeWrapper = topWrapper.newScope(True, False)
        endScopeWrapper.put("name", AvroString())
        endScopeWrapper.put("instance", AvroInt())
        if self.version is not None:
            endScopeWrapper.put("version", AvroInt())
        endScopeWrapper.put("metadata", AvroMap(AvroString()))
        endScopeWrapper.put("actionsStarted", AvroLong())
        endScopeWrapper.put("actionsFinished", AvroLong())
        endScope = endScopeWrapper.newScope(True, False)

        endContextResults = [x.walk(task, endScope, withUserFunctions, engineOptions, pfaVersion) for x in self.end]
        endResults = [x[1] for x in endContextResults]
        endCalls = set(titus.util.flatten([x[0].calls for x in endContextResults]))

        actionScopeWrapper = topWrapper.newScope(True, False)
        actionScopeWrapper.put("input", self.input)
        actionScopeWrapper.put("name", AvroString())
        actionScopeWrapper.put("instance", AvroInt())
        if self.version is not None:
            actionScopeWrapper.put("version", AvroInt())
        actionScopeWrapper.put("metadata", AvroMap(AvroString()))
        actionScopeWrapper.put("actionsStarted", AvroLong())
        actionScopeWrapper.put("actionsFinished", AvroLong())
        actionScope = actionScopeWrapper.newScope(True, False)

        actionContextResults = [x.walk(task, actionScope, withUserFunctions, engineOptions, pfaVersion) for x in self.action]
        actionCalls = set(titus.util.flatten([x[0].calls for x in actionContextResults]))

        if self.method == Method.MAP or self.method == Method.FOLD:
            if not self.output.accepts(actionContextResults[-1][0].retType):
                raise PFASemanticException("action's inferred output type is {0} but the declared output type is {1}".format(ts(actionContextResults[-1][0].retType), ts(self.output)), self.pos)

        context = self.Context(
            self.name,
            self.method,
            self.input,
            self.output,
            self.inputPlaceholder.parser.compiledTypes,
            (beginResults,
             dict(list(beginScopeWrapper.inThisScope.items()) + list(beginScope.inThisScope.items())),
             beginCalls),
            ([x[1] for x in actionContextResults],
             dict(list(actionScopeWrapper.inThisScope.items()) + list(actionScope.inThisScope.items())),
             actionCalls),
            (endResults,
             dict(list(endScopeWrapper.inThisScope.items()) + list(endScope.inThisScope.items())),
             endCalls),
            userFcnContexts,
            self.zero,
            mergeOption,
            self.cells,
            self.pools,
            self.randseed,
            self.doc,
            self.version,
            self.metadata,
            self.options,
            self.inputPlaceholder.parser)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["name"] = self.name
        out["input"] = self.inputPlaceholder.jsonNode(memo)
        out["output"] = self.outputPlaceholder.jsonNode(memo)
        out["method"] = self.method

        if len(self.begin) > 0:
            out["begin"] = [x.jsonNode(lineNumbers, memo) for x in self.begin]

        out["action"] = [x.jsonNode(lineNumbers, memo) for x in self.action]

        if len(self.end) > 0:
            out["end"] = [x.jsonNode(lineNumbers, memo) for x in self.end]

        if len(self.fcns) > 0:
            out["fcns"] = dict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.fcns.items())

        if len(self.cells) > 0:
            out["cells"] = dict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.cells.items())

        if len(self.pools) > 0:
            out["pools"] = dict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.pools.items())

        if self.zero is not None:
            out["zero"] = json.loads(self.zero)

        if self.merge is not None:
            out["merge"] = [x.jsonNode(lineNumbers, memo) for x in self.merge]

        if self.randseed is not None:
            out["randseed"] = self.randseed

        if self.doc is not None:
            out["doc"] = self.doc

        if self.version is not None:
            out["version"] = self.version

        if len(self.metadata) > 0:
            out["metadata"] = dict(self.metadata)

        if len(self.options) > 0:
            out["options"] = dict(self.options.items())

        return out

    @titus.util.case
    class Context(AstContext):
        def __init__(self,
                     name,
                     method,
                     input,
                     output,
                     compiledTypes,
                     begin,
                     action,
                     end,
                     fcns,
                     zero,
                     merge,
                     cells,
                     pools,
                     randseed,
                     doc,
                     version,
                     metadata,
                     options,
                     parser): pass

class CellPoolSource(object):
    """Source methods for cells and pools."""
    EMBEDDED = "embedded"
    JSON = "json"
    AVRO = "avro"

@titus.util.case
class Cell(Ast):
    """Abstract syntax tree for a ``cell`` definition."""

    def __init__(self, avroPlaceholder, init, shared, rollback, source, pos=None):
        """:type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: cell type as a placeholder (so it can exist before type resolution)
        :type init: string or callable
        :param init: serialized JSON string containing initial data or a function that produces it (from an external file, usually)
        :type shared: bool
        :param shared: if ``True``, this cell shares data with all others in the same titus.genpy.SharedState
        :type rollback: bool
        :param rollback: if ``True``, this cell rolls back its value when it encounters an exception
        :type source: titus.pfaast.CellPoolSource string
        :param source: value of cell's ``source`` field
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(init, six.string_types) and not callable(init):
            raise PFASyntaxException("\"init\" must be a string or callable", pos)

        if not isinstance(shared, bool):
            raise PFASyntaxException("\"shared\" must be boolean", pos)

        if not isinstance(rollback, bool):
            raise PFASyntaxException("\"rollback\" must be boolean", pos)

        if not isinstance(source, six.string_types) and not source is None:
            raise PFASyntaxException("\"source\" must be a string or None", pos)
        
        if shared and rollback:
            raise PFASyntaxException("shared and rollback are mutually incompatible flags for a Cell", pos)

    def equals(self, other):
        if isinstance(other, Cell):
            return self.avroPlaceholder == other.avroPlaceholder and self.initJsonNode == other.initJsonNode and self.shared == other.shared and self.rollback == other.rollback and self.source == other.source
        else:
            return False

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context()
        return context, task(context, engineOptions)

    @property
    def initJsonNode(self):
        if callable(self.init):
            return json.loads(self.init(self.avroType))
        else:
            return json.loads(self.init)
        
    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["init"] = self.initJsonNode
        out["shared"] = self.shared
        out["rollback"] = self.rollback
        if self.source != CellPoolSource.EMBEDDED:
            out["source"] = self.source
        return out

    @titus.util.case
    class Context(AstContext):
        def __init__(self): pass

@titus.util.case
class Pool(Ast):
    """Abstract syntax tree for a ``pool`` definition."""

    def __init__(self, avroPlaceholder, init, shared, rollback, source, pos=None):
        """:type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: pool type as a placeholder (so it can exist before type resolution)
        :type init: string or callable
        :param init: serialized JSON string containing initial data or a function that produces it (from an external file, usually)
        :type shared: bool
        :param shared: if ``True``, this pool shares data with all others in the same titus.genpy.SharedState
        :type rollback: bool
        :param rollback: if ``True``, this pool rolls back its value when it encounters an exception
        :type source: titus.pfaast.CellPoolSource string
        :param source: value of pool's ``source`` field
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(init, dict) or not all(isinstance(x, six.string_types) or x is None for x in init.values()):
            raise PFASyntaxException("\"init\" must be a string or callable", pos)

        if not isinstance(shared, bool):
            raise PFASyntaxException("\"shared\" must be boolean", pos)

        if not isinstance(rollback, bool):
            raise PFASyntaxException("\"rollback\" must be boolean", pos)

        if not isinstance(source, six.string_types) and not source is None:
            raise PFASyntaxException("\"source\" must be a string or None", pos)

        if shared and rollback:
            raise PFASyntaxException("shared and rollback are mutually incompatible flags for a Pool", pos)

    def equals(self, other):
        if isinstance(other, Pool):
            return self.avroPlaceholder == other.avroPlaceholder and self.initJsonNode == other.initJsonNode and self.shared == other.shared and self.rollback == other.rollback and self.source == other.source
        else:
            return False

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context()
        return context, task(context, engineOptions)

    @property
    def initJsonNode(self):
        if callable(self.init):
            return json.loads(self.init(AvroMap(self.avroType)))
        else:
            return OrderedDict((k, json.loads(v)) for k, v in self.init.items())

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["init"] = self.initJsonNode
        out["shared"] = self.shared
        out["rollback"] = self.rollback
        if self.source != CellPoolSource.EMBEDDED:
            out["source"] = self.source
        return out
            
    @titus.util.case
    class Context(AstContext):
        def __init__(self): pass

class Argument(Ast):
    """Trait for all function arguments, which can be expressions or function references."""
    pass

class Expression(Argument):
    """Trait for all PFA expressions, which resolve to Avro-typed values."""
    pass

class LiteralValue(Expression):
    """Trait for all PFA literal values, which are known constants at compile-time."""
    pass

class PathIndex(object):
    """Trait for path index elements, which can be used in ``attr``, ``cell``, and ``pool`` ``path`` arrays."""
    pass

@titus.util.case
class ArrayIndex(PathIndex):
    """Array indexes, which are concrete titus.pfaast.PathIndex elements that dereference arrays (expressions of ``int`` type)."""
    def __init__(self, i, t): pass

@titus.util.case
class MapIndex(PathIndex):
    """Map indexes, which are concrete titus.pfaast.PathIndex elements that dereference maps (expressions of ``string`` type)."""
    def __init__(self, k, t): pass

@titus.util.case
class RecordIndex(PathIndex):
    """Record indexes, which are concrete titus.pfaast.PathIndex elements that dereference records (literal ``string`` expressions)."""
    def __init__(self, f, t): pass

class HasPath(object):
    """Mixin for titus.pfaast.Ast classes that have paths (``attr``, ``cell``, ``pool``)."""

    def walkPath(self, avroType, task, symbolTable, functionTable, engineOptions, version):
        """Dereference a ``path``, checking all types along the way.

        :type avroType: titus.datatype.AvroType
        :param avroType: data type of the base expression or cell/pool
        :type task: titus.pfaast.Task
        :param task: generic task to perform on each expression in the path
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.datatype.AvroType, set of strings, list of titus.pfaast.PathIndex)
        :return: (type of dereferenced object, functions called, path indexes)
        """

        calls = set()
        scope = symbolTable.newScope(True, True)
        walkingType = avroType

        pathIndexes = []
        for indexIndex, expr in enumerate(self.path):
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)
            
            if isinstance(walkingType, AvroArray):
                if isinstance(exprContext.retType, AvroLong) or isinstance(exprContext.retType, AvroInt):
                    walkingType = walkingType.items
                    pathIndexes.append(ArrayIndex(exprResult, walkingType))
                else:
                    raise PFASemanticException("path index for an array must resolve to a long or int; item {0} is a {1}".format(indexIndex, ts(exprContext.retType)), self.pos)

            elif isinstance(walkingType, AvroMap):
                if isinstance(exprContext.retType, AvroString):
                    walkingType = walkingType.values
                    pathIndexes.append(MapIndex(exprResult, walkingType))
                else:
                    raise PFASemanticException("path index for a map must resolve to a string; item {0} is a {1}".format(indexIndex, ts(exprContext.retType)), self.pos)

            elif isinstance(walkingType, AvroRecord):
                if isinstance(exprContext.retType, AvroString):
                    if isinstance(exprContext, LiteralString.Context):
                        name = exprContext.value
                    elif isinstance(exprContext, Literal.Context) and isinstance(exprContext.retType, AvroString):
                        name = json.loads(exprContext.value)
                    else:
                        raise PFASemanticException("path index for record {0} must be a literal string; item {1} is an object of type {2}".format(ts(walkingType), indexIndex, ts(exprContext.retType)), self.pos)

                    if name in walkingType.fieldsDict.keys():
                        walkingType = walkingType.field(name).avroType
                        pathIndexes.append(RecordIndex(name, walkingType))
                    else:
                        raise PFASemanticException("record {0} has no field named \"{1}\" (path index {2})".format(ts(walkingType), name, indexIndex), self.pos)

                else:
                    raise PFASemanticException("path index for a record must be a string; item {0} is a {1}".format(indexIndex, ts(exprContext.retType)), self.pos)

            else:
                raise PFASemanticException("path item {0} is a {1}, which cannot be indexed".format(indexIndex, ts(walkingType)), self.pos)

        return walkingType, calls, pathIndexes

@titus.util.case
class FcnDef(Argument):
    """Abstract syntax tree for a function definition."""

    def __init__(self, paramsPlaceholder, retPlaceholder, body, pos=None):
        """:type paramsPlaceholder: list of {string: titus.datatype.AvroPlaceholder} singletons
        :param paramsPlaceholder: function parameter types as placeholders (so they can exist before type resolution)
        :type retPlaceholder: titus.datatype.AvroPlaceholder
        :param retPlaceholder: return type as a placeholder (so it can exist before type resolution)
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(paramsPlaceholder, (list, tuple)) or not \
                all(isinstance(x, dict) and len(x) == 1 and \
                isinstance(list(x.values())[0], (AvroPlaceholder, AvroType)) \
                for x in paramsPlaceholder):
            raise PFASyntaxException("\"paramsPlaceholder\" must be a list of single-key dictionaries of AvroPlaceholders or AvroTypes", pos)

        if not isinstance(retPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"retPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expressions", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("function's \"do\" list must contain least one expression", pos)

    @property
    def paramNames(self):
        """Names of the parameters (list of strings)."""
        return [list(t.keys())[0] for t in self.paramsPlaceholder]

    @property
    def params(self):
        """Resolved parameter types (list of {string: titus.datatype.AvroType} singletons)."""
        return [{list(t.keys())[0]: list(t.values())[0].avroType} for t in self.paramsPlaceholder]

    @property
    def paramsDict(self):
        """Resolved parameter types as an unordered dictionary (dict of titus.datatype.AvroType)."""
        return dict((list(t.keys())[0], list(t.values())[0].avroType) for t in self.paramsPlaceholder)

    @property
    def ret(self):
        """Resolved return type (titus.datatype.AvroType)."""
        return self.retPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(FcnDef, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return FcnDef(self.paramsPlaceholder,
                          self.retPlaceholder,
                          [x.replace(pf) for x in self.body],
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        if len(self.paramsPlaceholder) > 22:
            raise PFASemanticException("function can have at most 22 parameters", self.pos)

        scope = symbolTable.newScope(True, False)
        for name, avroType in self.paramsDict.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid parameter name".format(name), self.pos)
            scope.put(name, avroType)

        results = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.body]

        inferredRetType = results[-1][0].retType
        if not isinstance(inferredRetType, ExceptionType) and not self.ret.accepts(inferredRetType):
            raise PFASemanticException("function's inferred return type is {0} but its declared return type is {1}".format(ts(results[-1][0].retType), ts(self.ret)), self.pos)

        context = self.Context(FcnType([list(t.values())[0] for t in self.params], self.ret), set(titus.util.flatten([x[0].calls for x in results])), self.paramNames, self.paramsDict, self.ret, scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["params"] = [{list(x.keys())[0]: list(x.values())[0].jsonNode(memo)} for x in self.paramsPlaceholder]
        out["ret"] = self.retPlaceholder.jsonNode(memo)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, paramNames, params, ret, symbols, exprs): pass

@titus.util.case
class FcnRef(Argument):
    """Abstract syntax tree for a function reference."""

    def __init__(self, name, pos=None):
        """:type name: string
        :param name: name of the function to reference
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        fcnsig = None
        if isinstance(fcn.sig, Sig) and (fcn.sig.lifespan.current(version) or fcn.sig.lifespan.deprecated(version)):
            fcnsig = fcn.sig
        elif isinstance(fcn.sig, Sigs):
            fcnsigs = [x for x in fcn.sig.cases if x.lifespan.current(version) or x.lifespan.deprecated(version)]
            if len(fcnsigs) == 1:
                fcnsig = fcnsigs[0]
        if fcnsig is None:
            raise PFASemanticException("only one-signature functions without generics can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)
        fcn.deprecationWarning(fcnsig, version)

        try:
            params, ret = fcnsig.params, fcnsig.ret
            fcnType = FcnType([P.toType(list(p.values())[0]) for p in params], P.mustBeAvro(P.toType(ret)))
        except IncompatibleTypes:
            raise PFASemanticException("only one-signature functions without generics can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)

        context = FcnRef.Context(fcnType, set([self.name]), fcn)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["fcn"] = self.name
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, fcn): pass

@titus.util.case
class FcnRefFill(Argument):
    """Abstract syntax tree for a function reference with partial application."""

    def __init__(self, name, fill, pos=None):
        """:type name: string
        :param name: name of function to reference
        :type fill: dict from parameter names to titus.pfaast.Argument
        :param fill: parameters to partially apply
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

        if not isinstance(fill, dict) or not all(isinstance(x, Argument) for x in fill.values()):
            raise PFASyntaxException("\"fill\" must be a dictionary of Arguments", pos)

        if len(self.fill) < 1:
            raise PFASyntaxException("\"fill\" must contain at least one parameter name-argument mapping", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(FcnRefFill, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.fill.values())

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return FcnRefFill(self.name,
                              dict((k, v.replace(pf)) for k, v in self.fill.items()),
                              self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set([self.name])

        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        fillScope = symbolTable.newScope(True, True)
        argTypeResult = {}
        for name, arg in self.fill.items():
            argCtx, argRes = arg.walk(task, fillScope, functionTable, engineOptions, version)

            calls = calls.union(argCtx.calls)

            if isinstance(argCtx, FcnContext):
                argTypeResult[name] = (argCtx.fcnType, argRes)
            elif isinstance(argCtx, ExpressionContext):
                argTypeResult[name] = (argCtx.retType, argRes)
            else:
                raise Exception

        fcnsig = None
        if isinstance(fcn.sig, Sig) and (fcn.sig.lifespan.current(version) or fcn.sig.lifespan.deprecated(version)):
            fcnsig = fcn.sig
        elif isinstance(fcn.sig, Sigs):
            fcnsigs = [x for x in fcn.sig.cases if x.lifespan.current(version) or x.lifespan.deprecated(version)]
            if len(fcnsigs) == 1:
                fcnsig = fcnsigs[0]
        if fcnsig is None:
            raise PFASemanticException("only one-signature functions without generics can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)
        fcn.deprecationWarning(fcnsig, version)

        try:
            params, ret = fcnsig.params, fcnsig.ret

            originalParamNames = [list(x.keys())[0] for x in params]
            fillNames = set(argTypeResult.keys())

            if not fillNames.issubset(set(originalParamNames)):
                raise PFASemanticException("fill argument names (\"{0}\") are not a subset of function \"{1}\" parameter names (\"{2}\")".format("\", \"".join(sorted(fillNames)), self.name, "\", \"".join(originalParamNames)), self.pos)

            fcnType = FcnType([P.mustBeAvro(P.toType(list(p.values())[0])) for p in params if list(p.keys())[0] not in fillNames], P.mustBeAvro(P.toType(ret)))
        except IncompatibleTypes:
            raise PFASemanticException("only one-signature functions without constraints can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)

        context = self.Context(fcnType, calls, fcn, originalParamNames, argTypeResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["fcn"] = self.name
        out["fill"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.fill.items())
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, fcn, originalParamNames, argTypeResult): pass

@titus.util.case
class CallUserFcn(Expression):
    """Abstract syntax tree for calling a user-defined function; choice of function determined at runtime."""

    def __init__(self, name, args, pos=None):
        """:type name: titus.pfaast.Expression
        :param name: resolves to an enum type with each enum specifying a user-defined function
        :type args: list of titus.pfaast.Expression
        :param args: arguments to pass to the chosen function
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, Expression):
            raise PFASyntaxException("\"name\" must be an Expression", pos)

        if not isinstance(args, (list, tuple)) or not all(isinstance(x, Expression) for x in args):
            raise PFASyntaxException("\"args\" must be a list of Expressions", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(CallUserFcn, self).collect(pf) + \
               self.name.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.args)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CallUserFcn(self.name.replace(pf),
                               [x.replace(pf) for x in self.args],
                               self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        nameScope = symbolTable.newScope(True, True)
        nameContext, nameResult = self.name.walk(task, nameScope, functionTable, engineOptions, version)
        if isinstance(nameContext.retType, AvroEnum):
            fcnNames = nameContext.retType.symbols
        else:
            raise PFASemanticException("\"call\" name should be an enum, but is " + ts(nameContext.retType), self.pos)
        nameToNum = dict((x, i) for i, x in enumerate(fcnNames))

        scope = symbolTable.newScope(True, True)
        argResults = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.args]

        calls = set("u." + x for x in fcnNames)
        argTypes = []
        for ctx, res in argResults:
            if isinstance(ctx, ExpressionContext):
                calls = calls.union(ctx.calls)
                argTypes.append(ctx.retType)
            else:
                raise Exception

        nameToFcn = {}
        nameToParamTypes = {}
        nameToRetTypes = {}
        retTypes = []
        for n in fcnNames:
            fcn = functionTable.functions.get("u." + n, None)
            if fcn is None:
                raise PFASemanticException("unknown function \"{0}\" in enumeration type".format(n), self.pos)
            if not isinstance(fcn, UserFcn):
                raise PFASemanticException("function \"{0}\" is not a user function".format(n), self.pos)
            sigres = fcn.sig.accepts(argTypes, version)
            if sigres is not None:
                sig, paramTypes, retType = sigres
                fcn.deprecationWarning(sig, version)

                nameToFcn[n] = fcn
                nameToParamTypes[n] = paramTypes
                nameToRetTypes[n] = retType
                retTypes.append(retType)
            else:
                raise PFASemanticException("parameters of function \"{0}\" (in enumeration type) do not accept [{1}]".format(self.name, ",".join(map(ts, argTypes))), self.pos)

        try:
            retType = LabelData.broadestType(retTypes)
        except IncompatibleTypes as err:
            raise PFASemanticException(str(err), self.pos)

        context = self.Context(retType, calls, nameResult, nameToNum, nameToFcn, [x[1] for x in argResults], [x[0] for x in argResults], nameToParamTypes, nameToRetTypes)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["call"] = self.name.jsonNode(lineNumbers, memo)
        out["args"] = [x.jsonNode(lineNumbers, memo) for x in self.args]
        return out

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, name, nameToNum, nameToFcn, args, argContext, nameToParamTypes, nameToRetTypes): pass

@titus.util.case
class Call(Expression):
    """Abstract syntax tree for a function call."""

    def __init__(self, name, args, pos=None):
        """:type name: string
        :param name: name of the function to call
        :type args: list of titus.pfaast.Expression
        :param args: arguments to pass to the function
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

        if not isinstance(args, (list, tuple)) or not all(isinstance(x, Argument) for x in args):
            raise PFASyntaxException("\"args\" must be a list of Arguments", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Call, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.args)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Call(self.name,
                        [x.replace(pf) for x in self.args],
                        self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        scope = symbolTable.newScope(True, True)
        argResults = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.args]

        calls = set([self.name])
        argTypes = []
        for ctx, res in argResults:
            if isinstance(ctx, ExpressionContext):
                calls = calls.union(ctx.calls)
                argTypes.append(ctx.retType)
            elif isinstance(ctx, FcnDef.Context):
                calls = calls.union(ctx.calls)
                argTypes.append(ctx.fcnType)
            elif isinstance(ctx, FcnRef.Context):
                calls = calls.union(ctx.calls)
                argTypes.append(ctx.fcnType)
            elif isinstance(ctx, FcnRefFill.Context):
                calls = calls.union(ctx.calls)
                argTypes.append(ctx.fcnType)

        sigres = fcn.sig.accepts(argTypes, version)
        if sigres is not None:
            sig, paramTypes, retType = sigres
            fcn.deprecationWarning(sig, version)

            argContexts = [x[0] for x in argResults]
            argTaskResults = [x[1] for x in argResults]

            # Two-parameter task?
            # for i, a in enumerate(self.args):
            #     if isinstance(a, FcnRef):
            #         argTaskResults[i] = task(argContexts[i], engineOptions, paramTypes[i])

            context = self.Context(retType, calls, fcn, argTaskResults, argContexts, paramTypes, self.pos)

        else:
            raise PFASemanticException("parameters of function \"{0}\" do not accept [{1}]".format(self.name, ",".join(map(ts, argTypes))), self.pos)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out[self.name] = [x.jsonNode(lineNumbers, memo) for x in self.args]
        return out

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, fcn, args, argContexts, paramTypes, pos): pass

@titus.util.case
class Ref(Expression):
    """Abstract syntax tree for a variable (symbol) reference."""

    def __init__(self, name, pos=None):
        """:type name: string
        :param name: variable (symbol) to reference
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        if symbolTable.get(self.name) is None:
            raise PFASemanticException("unknown symbol \"{0}\"".format(self.name), self.pos)
        context = self.Context(symbolTable(self.name), set(), self.name)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        return self.name

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, name): pass

@titus.util.case
class LiteralNull(LiteralValue):
    """Abstract syntax tree for a literal ``null``."""

    def __init__(self, pos=None):
        """:type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        return None

    desc = "(null)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class LiteralBoolean(LiteralValue):
    """Abstract syntax tree for a literal ``true`` or ``false``."""

    def __init__(self, value, pos=None):
        """:type value: bool
        :param value: ``True`` or ``False``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, bool):
            raise PFASyntaxException("\"value\" must be boolean", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroBoolean(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        return self.value

    desc = "(boolean)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralInt(LiteralValue):
    """Abstract syntax tree for a literal integer."""

    def __init__(self, value, pos=None):
        """:type value: integer
        :param value: value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, int):
            raise PFASyntaxException("\"value\" must be an int", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroInt(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        return self.value

    desc = "(int)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralLong(LiteralValue):
    """Abstract syntax tree for a literal long."""

    def __init__(self, value, pos=None):
        """:type value: integer
        :param value: value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, int):
            raise PFASyntaxException("\"value\" must be an int", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroLong(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["long"] = self.value
        return out

    desc = "(long)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralFloat(LiteralValue):
    """Abstract syntax tree for a literal float."""

    def __init__(self, value, pos=None):
        """:type value: number
        :param value: value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, (int, float)):
            raise PFASyntaxException("\"value\" must be a number", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroFloat(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["float"] = self.value
        return out

    desc = "(float)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralDouble(LiteralValue):
    """Abstract syntax tree for a literal double."""

    def __init__(self, value, pos=None):
        """:type value: number
        :param value: value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, (int, float)):
            raise PFASyntaxException("\"value\" must be a number", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroDouble(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        return self.value

    desc = "(double)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralString(LiteralValue):
    """Abstract syntax tree for a literal string."""

    def __init__(self, value, pos=None):
        """:type value: string
        :param value: value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, six.string_types):
            raise PFASyntaxException("\"value\" must be a string", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroString(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["string"] = self.value
        return out

    desc = "(string)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralBase64(LiteralValue):
    """Abstract syntax tree for a literal base-64 encoded binary."""

    def __init__(self, value, pos=None):
        """:type value: raw binary string
        :param value: already base-64 decoded value
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(value, six.string_types):
            raise PFASyntaxException("\"value\" must be a string", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroBytes(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["base64"] = base64.b64encode(self.value)
        return out

    desc = "(bytes)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class Literal(LiteralValue):
    """Abstract syntax tree for an arbitrary literal value."""

    def __init__(self, avroPlaceholder, value, pos=None):
        """:type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: data type as a placeholder (so it can exist before type resolution)
        :type value: JSON string
        :param value: literal value encoded as a JSON string
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(value, six.string_types):
            raise PFASyntaxException("\"value\" must be a string", pos)

    def equals(self, other):
        if isinstance(other, Literal):
            return self.avroPlaceholder == other.avroPlaceholder and json.loads(self.value) == json.loads(other.value)
        else:
            return False

    @property
    def avroType(self):
        """Resolved data type (titus.datatype.AvroType)."""
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(self.avroType, set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["value"] = json.loads(self.value)
        return out

    desc = "(literal)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class NewObject(Expression):
    """Abstract syntax tree for a new map or record expression."""

    def __init__(self, fields, avroPlaceholder, pos=None):
        """:type fields: dict from field names to titus.pfaast.Expression
        :param fields: values to fill
        :type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: map or record type as a placeholder (so it can exist before type resolution)
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(fields, dict) or not all(isinstance(x, Expression) for x in fields.values()):
            raise PFASyntaxException("\"fields\" must be a dictionary of Expressions", pos)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

    def equals(self, other):
        if isinstance(other, NewObject):
            return self.fields == other.fields and self.avroPlaceholder == other.avroPlaceholder
        else:
            return False

    @property
    def avroType(self):
        """Resolved map or record type (titus.datatype.AvroType)."""
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(NewObject, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.fields.values())

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return NewObject(dict((k, v.replace(pf)) for k, v in self.fields.items()),
                             self.avroPlaceholder,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        fieldNameTypeExpr = []
        scope = symbolTable.newScope(True, True)
        for name, expr in self.fields.items():
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)
            fieldNameTypeExpr.append((name, exprContext.retType, exprResult))

        if isinstance(self.avroType, AvroRecord):
            fldsMap = dict((n, f.avroType) for n, f in self.avroType.fieldsDict.items())
            for n, t, xr in fieldNameTypeExpr:
                fieldType = fldsMap.get(n)
                if fieldType is None:
                    raise PFASemanticException("record constructed with \"new\" has unexpected field named \"{0}\"".format(n), self.pos)
                if not fieldType.accepts(t):
                    raise PFASemanticException("record constructed with \"new\" is has wrong field type for \"{0}\": {1} rather than {2}".format(n, ts(t), ts(fieldType)), self.pos)
            if set(fldsMap.keys()) != set(n for n, t, xr in fieldNameTypeExpr):
                raise PFASemanticException("record constructed with \"new\" is missing fields: {0} rather than {1}".format(sorted(n for n, t, xr in fieldNameTypeExpr), sorted(fldsMap.keys())), self.pos)

        elif isinstance(self.avroType, AvroMap):
            for n, t, xr in fieldNameTypeExpr:
                if not self.avroType.values.accepts(t):
                    raise PFASemanticException("map constructed with \"new\" has wrong type for value associated with key \"{0}\": {1} rather than {2}".format(n, ts(t), ts(self.avroType.values)), self.pos)

        else:
            raise PFASemanticException("object constructed with \"new\" must have record or map type, not {0}".format(ts(self.avroType)), self.pos)

        context = self.Context(self.avroType, calls.union(set([self.desc])), dict((x[0], x[2]) for x in fieldNameTypeExpr))
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["new"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.fields.items())
        return out

    desc = "new (object)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, fields): pass

@titus.util.case
class NewArray(Expression):
    """Abstract syntax tree for a new array expression."""

    def __init__(self, items, avroPlaceholder, pos=None):
        """:type items: list of titus.pfaast.Expression
        :param items: items to fill
        :type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: array type as a placeholder (so it can exist before type resolution)
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(items, (list, tuple)) or not all(isinstance(x, Expression) for x in items):
            raise PFASyntaxException("\"items\" must be a list of Expressions", pos)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

    def equals(self, other):
        if isinstance(other, NewArray):
            return self.items == other.items and self.avroPlaceholder == other.avroPlaceholder
        else:
            return False

    @property
    def avroType(self):
        """Resolved array type (titus.datatype.AvroType)."""
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(NewArray, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.items)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return NewArray([x.replace(pf) for x in self.items],
                            self.avroPlaceholder,
                            self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        scope = symbolTable.newScope(True, True)
        itemTypeExpr = []
        for expr in self.items:
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)
            itemTypeExpr.append((exprContext.retType, exprResult))

        if isinstance(self.avroType, AvroArray):
            for i, (t, xr) in enumerate(itemTypeExpr):
                if not self.avroType.items.accepts(t):
                    raise PFASemanticException("array constructed with \"new\" has wrong type for item {0}: {1} rather than {2}".format(i, ts(t), ts(self.avroType.items)), self.pos)

        else:
            raise PFASemanticException("array constructed with \"new\" must have array type, not {0}".format(ts(self.avroType)), self.pos)

        context = self.Context(self.avroType, calls.union(set([self.desc])), [x[1] for x in itemTypeExpr])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["new"] = [x.jsonNode(lineNumbers, memo) for x in self.items]
        return out

    desc = "new (array)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, items): pass

@titus.util.case
class Do(Expression):
    """Abstract syntax tree for a ``do`` block."""

    def __init__(self, body, pos=None):
        """:type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expressions", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" block must contain at least one expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Do, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Do([x.replace(pf) for x in self.body],
                      self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        scope = symbolTable.newScope(False, False)
        results = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.body]

        inferredType = results[-1][0].retType
        if isinstance(inferredType, ExceptionType):
            inferredType = AvroNull()

        context = self.Context(inferredType, set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    desc = "do"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprs): pass

@titus.util.case
class Let(Expression):
    """Abstract syntax tree for a ``let`` variable declaration."""

    def __init__(self, values, pos=None):
        """:type values: dict from variable (symbol) name to titus.pfaast.Expression
        :param values: new variables and their initial values
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(values, dict) or not all(isinstance(x, Expression) for x in values.values()):
            raise PFASyntaxException("\"values\" must be a dictionary of Expressions", pos)

        if len(self.values) < 1:
            raise PFASyntaxException("\"let\" must contain at least one declaration", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Let, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.values.values())

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Let(dict((k, v.replace(pf)) for k, v in self.values.items()),
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        if symbolTable.sealedWithin:
            raise PFASemanticException("new variable bindings are forbidden in this scope, but you can wrap your expression with \"do\" to make temporary variables", self.pos)

        calls = set()

        newSymbols = {}

        nameTypeExpr = []
        for name, expr in self.values.items():
            if symbolTable.get(name) is not None:
                raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(name), self.pos)

            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid symbol name".format(name), self.pos)

            scope = symbolTable.newScope(True, True)
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)

            if isinstance(exprContext.retType, ExceptionType):
                raise PFASemanticException("cannot declare a variable with exception type", self.pos)

            newSymbols[name] = exprContext.retType

            nameTypeExpr.append((name, exprContext.retType, exprResult))

        for name, avroType in newSymbols.items():
            symbolTable.put(name, avroType)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), nameTypeExpr)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["let"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.values.items())
        return out

    desc = "let"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, nameTypeExpr): pass

@titus.util.case
class SetVar(Expression):
    """Abstract syntax tree for a ``set`` variable update."""

    def __init__(self, values, pos=None):
        """:type values: dict from variable (symbol) name to titus.pfaast.Expression
        :param values: variables and their updated values
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(values, dict) or not all(isinstance(x, Expression) for x in values.values()):
            raise PFASyntaxException("\"values\" must be a dictionary of Expressions", pos)

        if len(self.values) < 1:
            raise PFASyntaxException("\"set\" must contain at least one assignment", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(SetVar, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.values.values())

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return SetVar(dict((k, v.replace(pf)) for k, v in self.values.items()),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        nameTypeExpr = []
        for name, expr in self.values.items():
            if symbolTable.get(name) is None:
                raise PFASemanticException("unknown symbol \"{0}\" cannot be assigned with \"set\" (use \"let\" to declare a new symbol)".format(name), self.pos)
            elif not symbolTable.writable(name):
                raise PFASemanticException("symbol \"{0}\" belongs to a sealed enclosing scope; it cannot be modified within this block)".format(name), self.pos)

            scope = symbolTable.newScope(True, True)
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)

            if not symbolTable(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{0}\" was declared as {1}; it cannot be re-assigned as {2}".format(name, ts(symbolTable(name)), ts(exprContext.retType)), self.pos)

            nameTypeExpr.append((name, symbolTable(name), exprResult))

        context = self.Context(AvroNull(), calls.union(set([self.desc])), nameTypeExpr)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["set"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.values.items())
        return out

    desc = "set"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, nameTypeExpr): pass

@titus.util.case
class AttrGet(Expression, HasPath):
    """Abstract syntax tree for an ``attr`` array, map, record extraction."""

    def __init__(self, expr, path, pos=None):
        """:type expr: titus.pfaast.Expression
        :param expr: base object to extract from
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the base object
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(expr, Expression):
            raise PFASyntaxException("\"expr\" must be an Expression", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

        if len(self.path) < 1:
            raise PFASyntaxException("attr path must have at least one key", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(AttrGet, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return AttrGet(self.expr.replace(pf),
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions, version)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        retType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions, version)
        context = self.Context(retType, calls.union(set([self.desc])), exprResult, exprContext.retType, pathResult, self.pos)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["attr"] = self.expr.jsonNode(lineNumbers, memo)
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "attr"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr, exprType, path, pos): pass

@titus.util.case
class AttrTo(Expression, HasPath):
    """Abstract syntax tree for an ``attr-to`` update."""

    def __init__(self, expr, path, to, pos=None):
        """:type expr: titus.pfaast.Expression
        :param expr: base object to extract from
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the base object
        :type to: titus.pfaast.Argument
        :param to: expression for a replacement object or function reference to use as an updator
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(expr, Expression):
            raise PFASyntaxException("\"expr\" must be an Expression", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

        if not isinstance(to, Argument):
            raise PFASyntaxException("\"to\" must be an Argument", pos)

        if len(self.path) < 1:
            raise PFASyntaxException("attr path must have at least one key", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(AttrTo, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return AttrTo(self.expr.replace(pf),
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions, version)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        setType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions, version)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions, version)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.retType, self.pos)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.fcnType, self.pos)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, self.pos)   # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, self.pos)   # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)
        
    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["attr"] = self.expr.jsonNode(lineNumbers, memo)
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        out["to"] = self.to.jsonNode(lineNumbers, memo)
        return out

    desc = "attr-to"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr, exprType, setType, path, to, toType, pos): pass

@titus.util.case
class CellGet(Expression, HasPath):
    """Abstract syntax tree for a ``cell`` reference or extraction."""

    def __init__(self, cell, path, pos=None):
        """:type cell: string
        :param cell: cell name
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the cell
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(cell, six.string_types):
            raise PFASyntaxException("\"cell\" must be a string", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(CellGet, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CellGet(self.cell,
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{0}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        retType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable, engineOptions, version)
        context = self.Context(retType, calls.union(set([self.desc])), self.cell, cellType, pathResult, shared, self.pos)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["cell"] = self.cell
        if len(self.path) > 0:
            out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "cell"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, cell, cellType, path, shared, pos): pass

@titus.util.case
class CellTo(Expression, HasPath):
    """Abstract syntax tree for a ``cell-to`` update."""

    def __init__(self, cell, path, to, pos=None):
        """:type cell: string
        :param cell: cell name
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the cell
        :type to: titus.pfaast.Argument
        :param to: expression for a replacement object or function reference to use as an updator
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(cell, six.string_types):
            raise PFASyntaxException("\"cell\" must be a string", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

        if not isinstance(to, Argument):
            raise PFASyntaxException("\"to\" must be an Argument", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(CellTo, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CellTo(self.cell,
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{0}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        setType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable, engineOptions, version)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions, version)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.retType, shared, self.pos)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.fcnType, shared, self.pos)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, shared, self.pos)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, shared, self.pos)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["cell"] = self.cell
        if len(self.path) > 0:
            out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        out["to"] = self.to.jsonNode(lineNumbers, memo)
        return out

    desc = "cell-to"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, cell, cellType, setType, path, to, toType, shared, pos): pass

@titus.util.case
class PoolGet(Expression, HasPath):
    """Abstract syntax tree for a ``pool`` reference or extraction."""

    def __init__(self, pool, path, pos=None):
        """:type pool: string
        :param pool: pool name
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the pool
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(pool, six.string_types):
            raise PFASyntaxException("\"pool\" must be a string", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

        if len(self.path) < 1:
            raise PFASyntaxException("pool path must have at least one key", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(PoolGet, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return PoolGet(self.pool,
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{0}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        retType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions, version)
        context = self.Context(retType, calls.union(set([self.desc])), self.pool, pathResult, shared, self.pos)
        return context, task(context, engineOptions)
        
    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["pool"] = self.pool
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "pool"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, pool, path, shared, pos): pass

@titus.util.case
class PoolTo(Expression, HasPath):
    """Abstract syntax tree for a ``pool-to`` update."""

    def __init__(self, pool, path, to, init, pos=None):
        """:type pool: string
        :param pool: pool name
        :type path: list of titus.pfaast.Expression
        :param path: array, map, and record indexes to extract from the pool
        :type to: titus.pfaast.Argument
        :param to: expression for a replacement object or function reference to use as an updator
        :type init: titus.pfaast.Expression
        :param init: initial value provided in case the desired pool item is empty
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(pool, six.string_types):
            raise PFASyntaxException("\"pool\" must be a string", pos)

        if not isinstance(path, (list, tuple)) or not all(isinstance(x, Expression) for x in path):
            raise PFASyntaxException("\"path\" must be a list of Expressions", pos)

        if not isinstance(to, Argument):
            raise PFASyntaxException("\"to\" must be an Argument", pos)

        if not isinstance(init, Expression):
            raise PFASyntaxException("\"init\" must be an Expression", pos)

        if len(self.path) < 1:
            raise PFASyntaxException("pool path must have at least one key", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(PoolTo, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf) + \
               self.init.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return PoolTo(self.pool,
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.init.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{0}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        setType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions, version)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions, version)

        initContext, initResult = self.init.walk(task, symbolTable, functionTable, engineOptions, version)

        if not poolType.accepts(initContext.retType):
            raise PFASemanticException("pool has type {0} but attempting to init with type {1}".format(ts(poolType), ts(initContext.retType)), self.pos)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.retType, initResult, shared, self.pos)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.fcnType, initResult, shared, self.pos)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, initResult, shared, self.pos)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, initResult, shared, self.pos)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["pool"] = self.pool
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        if self.init is not None:
            out["init"] = self.init.jsonNode(lineNumbers, memo)
        out["to"] = self.to.jsonNode(lineNumbers, memo)
        return out

    desc = "pool-to"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, pool, poolType, setType, path, to, toType, init, shared, pos): pass

@titus.util.case
class PoolDel(Expression):
    """Abstract syntax tree for a ``pool-del`` removal."""

    def __init__(self, pool, dell, pos=None):
        """:type pool: string
        :param pool: pool name
        :type dell: titus.pfaast.Expression
        :param dell: item to delete
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(pool, six.string_types):
            raise PFASyntaxException("\"pool\" must be a string", pos)

        if not isinstance(dell, Expression):
            raise PFASyntaxException("\"dell\" must be an Expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(PoolDel, self).collect(pf) + \
               self.dell.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return PoolDel(self.pool, self.dell.replace(pf), self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{0}\"".format(self.pool), self.pos)
        shared = p.shared

        delContext, delResult = self.dell.walk(task, symbolTable, functionTable, engineOptions, version)
        if not AvroString().accepts(delContext.retType):
            raise PFASemanticException("\"pool-del\" expression should evaluate to a string, but is {0}".format(delContext.retType), self.pos)

        calls = delContext.calls.union(set([self.desc]))

        context = PoolDel.Context(AvroNull(), calls, self.pool, delResult, shared)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["pool"] = self.pool
        out["del"] = self.dell.jsonNode(lineNumbers, memo)
        return out

    desc = "pool-del"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, pool, dell, shared): pass

@titus.util.case
class If(Expression):
    """Abstract syntax tree for an ``if`` branch."""

    def __init__(self, predicate, thenClause, elseClause, pos=None):
        """:type predicate: titus.pfaast.Expression
        :param predicate: test expression that evaluates to ``boolean``
        :type thenClause: list of titus.pfaast.Expression
        :param thenClause: expressions to evaluate if the ``predicate`` evaluates to ``true``
        :type elseClause: list of titus.pfaast.Expression or ``None``
        :param elseClause: expressions to evaluate if present and the ``predicate`` evaluates to ``false``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(predicate, Expression):
            raise PFASyntaxException("\"predicate\" must be an Expression", pos)

        if not isinstance(thenClause, (list, tuple)) or not all(isinstance(x, Expression) for x in thenClause):
            raise PFASyntaxException("\"thenClause\" must be a list of Expressions", pos)

        if (not isinstance(elseClause, (list, tuple)) or not all(isinstance(x, Expression) for x in elseClause)) and not elseClause is None:
            raise PFASyntaxException("\"elseClause\" must be a list of Expressions or None", pos)
        
        if len(self.thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(If, self).collect(pf) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return If(self.predicate.replace(pf),
                      [x.replace(pf) for x in self.thenClause],
                      [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                      self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        predScope = symbolTable.newScope(True, True)
        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions, version)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"if\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        thenScope = symbolTable.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable, engineOptions, version) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions, version) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            thenType = thenResults[-1][0].retType
            elseType = elseResults[-1][0].retType
            if isinstance(thenType, ExceptionType) and isinstance(elseType, ExceptionType):
                retType = AvroNull()
            else:
                try:
                    retType = P.mustBeAvro(LabelData.broadestType([thenType, elseType]))
                except IncompatibleTypes as err:
                    raise PFASemanticException(str(err), self.pos)

            retType, elseTaskResults, elseSymbols = retType, [x[1] for x in elseResults], elseScope.inThisScope
        else:
            retType, elseTaskResults, elseSymbols = AvroNull(), None, None

        context = self.Context(retType, calls.union(set([self.desc])), thenScope.inThisScope, predResult, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["if"] = self.predicate.jsonNode(lineNumbers, memo)
        out["then"] = [x.jsonNode(lineNumbers, memo) for x in self.thenClause]
        if self.elseClause is not None:
            out["else"] = [x.jsonNode(lineNumbers, memo) for x in self.elseClause]
        return out

    desc = "if"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, thenSymbols, predicate, thenClause, elseSymbols, elseClause): pass

@titus.util.case
class Cond(Expression):
    """Abstract syntax tree for a ``cond`` branch."""

    def __init__(self, ifthens, elseClause, pos=None):
        """:type ifthens: list of titus.pfaast.If
        :param ifthens: if-then pairs without else clauses
        :type elseClause: list of titus.pfaast.Expression or ``None``
        :param elseClause: expressions to evaluate if present and all if-then predicates evaluate to ``false``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """
        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(ifthens, (list, tuple)) or not all(isinstance(x, If) for x in ifthens):
            raise PFASyntaxException("\"ifthens\" must be a list of Ifs", pos)

        if (not isinstance(elseClause, (list, tuple)) or not all(isinstance(x, Expression) for x in elseClause)) and not elseClause is None:
            raise PFASyntaxException("\"elseClause\" must be a list of Expressions or None", pos)

        if len(self.ifthens) < 1:
            raise PFASyntaxException("\"cond\" must contain at least one predicate-block pair", pos)

        for it in ifthens:
            if len(it.thenClause) < 1:
                raise PFASyntaxException("\"then\" clause must contain at least one expression", pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Cond, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.ifthens) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Cond([x.replace(pf) for x in self.ifthens],
                        [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                        self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        walkBlocks = []
        for ifthen in self.ifthens:
            predicate = ifthen.predicate
            thenClause = ifthen.thenClause
            ifpos = ifthen.pos

            predScope = symbolTable.newScope(True, True)
            predContext, predResult = predicate.walk(task, predScope, functionTable, engineOptions, version)
            if not AvroBoolean().accepts(predContext.retType):
                raise PFASemanticException("\"if\" predicate should be boolean, but is " + ts(predContext.retType), ifpos)
            calls = calls.union(predContext.calls)

            thenScope = symbolTable.newScope(False, False)
            thenResults = [x.walk(task, thenScope, functionTable, engineOptions, version) for x in thenClause]
            for exprCtx, exprRes in thenResults:
                calls = calls.union(exprCtx.calls)

            walkBlocks.append(self.WalkBlock(thenResults[-1][0].retType, thenScope.inThisScope, predResult, [x[1] for x in thenResults]))

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions, version) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            walkBlocks.append(Cond.WalkBlock(elseResults[-1][0].retType, elseScope.inThisScope, None, [x[1] for x in elseResults]))
        
        if self.elseClause is None:
            retType = AvroNull()
        else:
            walkTypes = [x.retType for x in walkBlocks]
            if all(isinstance(x, ExceptionType) for x in walkTypes):
                retType = AvroNull()
            else:
                try:
                    retType = LabelData.broadestType(walkTypes)
                except IncompatibleTypes as err:
                    raise PFASemanticException(str(err), self.pos)

        context = self.Context(retType, calls.union(set([self.desc])), (self.elseClause is not None), walkBlocks)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["cond"] = [x.jsonNode(lineNumbers, memo) for x in self.ifthens]
        if self.elseClause is not None:
            out["else"] = [x.jsonNode(lineNumbers, memo) for x in self.elseClause]
        return out

    desc = "cond"

    @titus.util.case
    class WalkBlock(object):
        def __init__(self, retType, symbols, pred, exprs): pass

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, complete, walkBlocks): pass

@titus.util.case
class While(Expression):
    """Abstract syntax tree for a ``while`` loop."""

    def __init__(self, predicate, body, pos=None):
        """:type predicate: titus.pfaast.Expression
        :param predicate: test expression that evaluates to ``boolean``
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate while ``predicate`` evaluates to ``true``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(predicate, Expression):
            raise PFASyntaxException("\"predicate\" must be an Expression", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be XXX", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(While, self).collect(pf) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return While(self.predicate.replace(pf),
                         [x.replace(pf) for x in self.body],
                         self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions, version)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"while\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        loopResults = [x.walk(task, loopScope, functionTable, engineOptions, version) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, predResult, [x[1] for x in loopResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["while"] = self.predicate.jsonNode(lineNumbers, memo)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    desc = "while"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, predicate, loopBody): pass

@titus.util.case
class DoUntil(Expression):
    """Abstract syntax tree for a ``do-until`` post-test loop."""

    def __init__(self, body, predicate, pos=None):
        """:type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate until ``predicate`` evaluates to ``true``
        :type predicate: titus.pfaast.Expression
        :param predicate: test expression that evaluates to ``boolean``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expressions", pos)

        if not isinstance(predicate, Expression):
            raise PFASyntaxException("\"predicate\" must be an Expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(DoUntil, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body) + \
               self.predicate.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return DoUntil([x.replace(pf) for x in self.body],
                           self.predicate.replace(pf),
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        loopResults = [x.walk(task, loopScope, functionTable, engineOptions, version) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions, version)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"until\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, [x[1] for x in loopResults], predResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        out["until"] = self.predicate.jsonNode(lineNumbers, memo)
        return out

    desc = "do-until"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, loopBody, predicate): pass

@titus.util.case
class For(Expression):
    """Abstract syntax tree for a ``for`` loop."""

    def __init__(self, init, predicate, step, body, pos=None):
        """:type init: dict from new variable names to titus.pfaast.Expression
        :param init: initial values for the dummy variables
        :type predicate: titus.pfaast.Expression
        :param predicate: test expression that evaluates to ``boolean``
        :type step: dict from variable names to titus.pfaast.Expression
        :param step: update expressions for the dummy variables
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate while ``predicate`` evaluates to ``true``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(init, dict) or not all(isinstance(x, Expression) for x in init.values()):
            raise PFASyntaxException("\"init\" must be a dictionary of Expression", pos)

        if not isinstance(predicate, Expression):
            raise PFASyntaxException("\"predicate\" must be an Expression", pos)

        if not isinstance(step, dict) or not all(isinstance(x, Expression) for x in step.values()):
            raise PFASyntaxException("\"step\" must be a dictionary of Expressions", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be XXX", pos)

        if len(self.init) < 1:
            raise PFASyntaxException("\"for\" must contain at least one declaration", pos)

        if len(self.step) < 1:
            raise PFASyntaxException("\"step\" must contain at least one assignment", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(For, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.init.values()) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.step.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return For(dict((k, v.replace(pf)) for k, v in self.init.items()),
                       self.predicate.replace(pf),
                       dict((k, v.replace(pf)) for k, v in self.step.items()),
                       [x.replace(pf) for x in self.body],
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        loopScope = symbolTable.newScope(False, False)

        newSymbols = {}

        initNameTypeExpr = []
        for name, expr in self.init.items():
            if loopScope.get(name) is not None:
                raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(name), self.pos)

            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid symbol name".format(name), self.pos)

            initScope = loopScope.newScope(True, True)
            exprContext, exprResult = expr.walk(task, initScope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)

            newSymbols[name] = exprContext.retType
            
            initNameTypeExpr.append((name, exprContext.retType, exprResult))

        for name, avroType in newSymbols.items():
            loopScope.put(name, avroType)

        predicateScope = loopScope.newScope(True, True)
        predicateContext, predicateResult = self.predicate.walk(task, predicateScope, functionTable, engineOptions, version)
        if not AvroBoolean().accepts(predicateContext.retType):
            raise PFASemanticException("predicate should be boolean, but is " + ts(predicateContext.retType), self.pos)
        calls = calls.union(predicateContext.calls)

        stepNameTypeExpr = []
        for name, expr in self.step.items():
            if loopScope.get(name) is None:
                raise PFASemanticException("unknown symbol \"{0}\" cannot be assigned with \"step\"".format(name), self.pos)
            elif not loopScope.writable(name):
                raise PFASemanticException("symbol \"{0}\" belongs to a sealed enclosing scope; it cannot be modified within this block".format(name), self.pos)

            stepScope = loopScope.newScope(True, True)
            exprContext, exprResult = expr.walk(task, stepScope, functionTable, engineOptions, version)
            calls = calls.union(exprContext.calls)

            if not loopScope(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{0}\" was declared as {1}; it cannot be re-assigned as {2}".format(name, ts(loopScope(name)), ts(exprContext.retType)), self.pos)

            stepNameTypeExpr.append((name, loopScope(name), exprResult))

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions, version) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), initNameTypeExpr, predicateResult, [x[1] for x in bodyResults], stepNameTypeExpr)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["for"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.init.items())
        out["while"] = self.predicate.jsonNode(lineNumbers, memo)
        out["step"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.step.items())
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    desc = "for"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, initNameTypeExpr, predicate, loopBody, stepNameTypeExpr): pass

@titus.util.case
class Foreach(Expression):
    """Abstract syntax tree for a ``foreach`` loop."""

    def __init__(self, name, array, body, seq, pos=None):
        """:type name: string
        :param name: new variable name for array items
        :type array: titus.pfaast.Expression
        :param array: expression that evaluates to an array
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate for each item of the array
        :type seq: bool
        :param seq: if ``False``, seal the scope from above and allow implementations to parallelize (Titus doesn't)
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(name, six.string_types):
            raise PFASyntaxException("\"name\" must be a string", pos)

        if not isinstance(array, Expression):
            raise PFASyntaxException("\"array\" must be an Expression", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expressions", pos)

        if not isinstance(seq, bool):
            raise PFASyntaxException("\"seq\" must be boolean", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Foreach, self).collect(pf) + \
               self.array.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Foreach(self.name,
                           self.array.replace(pf),
                           [x.replace(pf) for x in self.body],
                           self.seq,
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        loopScope = symbolTable.newScope(not self.seq, False)

        if symbolTable.get(self.name) is not None:
            raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(self.name), self.pos)

        if not validSymbolName(self.name):
            raise PFASemanticException("\"{0}\" is not a valid symbol name".format(self.name), self.pos)

        objScope = loopScope.newScope(True, True)
        objContext, objResult = self.array.walk(task, objScope, functionTable, engineOptions, version)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroArray):
            raise PFASemanticException("expression referred to by \"in\" should be an array, but is " + ts(objContext.retType), self.pos)
        elementType = objContext.retType.items

        loopScope.put(self.name, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions, version) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.name, [x[1] for x in bodyResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["foreach"] = self.name
        out["in"] = self.array.jsonNode(lineNumbers, memo)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        out["seq"] = self.seq
        return out

    desc = "foreach"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, objType, objExpr, itemType, name, loopBody): pass

@titus.util.case
class Forkeyval(Expression):
    """Abstract syntax tree for ``forkey-forval`` loops."""

    def __init__(self, forkey, forval, map, body, pos=None):
        """:type forkey: string
        :param forkey: new variable name for map keys
        :type forval: string
        :param forval: new variable name for map values
        :type map: titus.pfaast.Expression
        :param map: expression that evaluates to a map
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate for each item of the map
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(forkey, six.string_types):
            raise PFASyntaxException("\"forkey\" must be a string", pos)

        if not isinstance(forval, six.string_types):
            raise PFASyntaxException("\"forval\" must be a string", pos)

        if not isinstance(map, Expression):
            raise PFASyntaxException("\"map\" must be an Expression", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expression", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Forkeyval, self).collect(pf) + \
               self.map.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Forkeyval(self.forkey,
                             self.forval,
                             self.map.replace(pf),
                             [x.replace(pf) for x in self.body],
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        loopScope = symbolTable.newScope(False, False)

        if symbolTable.get(self.forkey) is not None:
            raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(self.forkey), self.pos)
        if symbolTable.get(self.forval) is not None:
            raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(self.forval), self.pos)

        if not validSymbolName(self.forkey):
            raise PFASemanticException("\"{0}\" is not a valid symbol name".format(self.forkey), self.pos)
        if not validSymbolName(self.forval):
            raise PFASemanticException("\"{0}\" is not a valid symbol name".format(self.forval), self.pos)

        objScope = loopScope.newScope(True, True)
        objContext, objResult = self.map.walk(task, objScope, functionTable, engineOptions, version)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroMap):
            raise PFASemanticException("expression referred to by \"in\" should be a map, but is " + ts(objContext.retType), self.pos)
        elementType = objContext.retType.values

        loopScope.put(self.forkey, AvroString())
        loopScope.put(self.forval, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions, version) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union([self.desc]), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.forkey, self.forval, [x[1] for x in bodyResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["forkey"] = self.forkey
        out["forval"] = self.forval
        out["in"] = self.map.jsonNode(lineNumbers, memo)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    desc = "forkey-forval"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, objType, objExpr, valueType, forkey, forval, loopBody): pass

@titus.util.case
class CastCase(Ast):
    """Abstract syntax tree for one ``case`` of a ``cast-case`` block."""

    def __init__(self, avroPlaceholder, named, body, pos=None):
        """:type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: subtype cast as a placeholder (so it can exist before type resolution)
        :type named: string
        :param named: new variable name to hold the casted value
        :type body: list of titus.pfaast.Expression
        :param body: expressions to evaluate if casting to this subtype is possible
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

        if not isinstance(named, six.string_types):
            raise PFASyntaxException("\"named\" must be a string", pos)

        if not isinstance(body, (list, tuple)) or not all(isinstance(x, Expression) for x in body):
            raise PFASyntaxException("\"body\" must be a list of Expression", pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", pos)

        if not validSymbolName(self.named):
            raise PFASyntaxException("\"{0}\" is not a valid symbol name".format(self.named), pos)

    @property
    def avroType(self):
        """Resolved subtype (titus.datatype.AvroType)."""
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(CastCase, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CastCase(self.avroPlaceholder,
                            self.named,
                            [x.replace(pf) for x in self.body],
                            self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        scope = symbolTable.newScope(False, False)
        scope.put(self.named, self.avroType)

        results = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.body]
        context = self.Context(results[-1][0].retType, self.named, self.avroType, set(titus.util.flatten([x[0].calls for x in results])), scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["as"] = self.avroPlaceholder.jsonNode(memo)
        out["named"] = self.named
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    @titus.util.case
    class Context(AstContext):
        def __init__(self, retType, name, toType, calls, symbols, clause): pass

@titus.util.case
class CastBlock(Expression):
    """Abstract syntax tree for a ``cast-case`` block."""

    def __init__(self, expr, castCases, partial, pos=None):
        """:type expr: titus.pfaast.Expression
        :param expr: expression to cast
        :type castCases: list of titus.pfaast.CastCase
        :param castCases: subtype cases to attempt to cast
        :type partial: bool
        :param partial: if ``True``, allow subtype cases to not fully cover the expression type
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(expr, Expression):
            raise PFASyntaxException("\"expr\" must be an Expression", pos)

        if not isinstance(castCases, (list, tuple)) or not all(isinstance(x, CastCase) for x in castCases):
            raise PFASyntaxException("\"castCases\" must be a list of CastCases", pos)

        if not isinstance(partial, bool):
            raise PFASyntaxException("\"partial\" must be boolean", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(CastBlock, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.castCases)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CastBlock(self.expr.replace(pf),
                             [x.replace(pf) for x in self.castCases],
                             self.partial,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions, version)
        calls = calls.union(exprContext.calls)

        exprType = exprContext.retType
        types = [x.avroType for x in self.castCases]

        # did you have anything extraneous?
        for t in types:
            if not exprType.accepts(t):
                raise PFASemanticException("\"cast\" of expression with type {0} can never satisfy case {1}".format(ts(exprType), ts(t)), self.pos)

        cases = [x.walk(task, symbolTable, functionTable, engineOptions, version) for x in self.castCases]
        for castCtx, castRes in cases:
            calls = calls.union(castCtx.calls)

        if self.partial:
            retType = AvroNull()
        else:
            # are you missing anything necessary?
            if isinstance(exprType, AvroUnion):
                mustFindThese = exprType.types
            else:
                mustFindThese = [exprType]

            for mustFind in mustFindThese:
                if not any(t.accepts(mustFind) and mustFind.accepts(t) for t in types):
                    raise PFASemanticException("\"cast\" of expression with type {0} does not contain a case for {1}".format(ts(exprType), ts(mustFind)), self.pos)

            try:
                retType = LabelData.broadestType([x[0].retType for x in cases])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err), self.pos)

        context = self.Context(retType, calls.union(set([self.desc])), exprType, exprResult, cases, self.partial)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["cast"] = self.expr.jsonNode(lineNumbers, memo)
        out["cases"] = [x.jsonNode(lineNumbers, memo) for x in self.castCases]
        out["partial"] = self.partial
        return out

    desc = "cast-cases"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, exprType, expr, cases, partial): pass

@titus.util.case
class Upcast(Expression):
    """Abstract syntax tree for an ``upcast``."""

    def __init__(self, expr, avroPlaceholder, pos=None):
        """:type expr: titus.pfaast.Expression
        :param expr: expression to up-cast
        :type avroPlaceholder: titus.datatype.AvroPlaceholder
        :param avroPlaceholder: supertype as a placeholder (so it can exist before type resolution)
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(expr, Expression):
            raise PFASyntaxException("\"expr\" must be an Expression", pos)

        if not isinstance(avroPlaceholder, (AvroPlaceholder, AvroType)):
            raise PFASyntaxException("\"avroPlaceholder\" must be an AvroPlaceholder or AvroType", pos)

    @property
    def avroType(self):
        """Resolved supertype (titus.datatype.AvroType)."""
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Upcast, self).collect(pf) + \
               self.expr.collect(pf)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Upcast(self.expr.replace(pf),
                          self.avroPlaceholder,
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        scope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, scope, functionTable, engineOptions, version)

        if not self.avroType.accepts(exprContext.retType):
            raise PFASemanticException("expression results in {0}; cannot expand (\"upcast\") to {1}".format(ts(exprContext.retType), ts(self.avroType)), self.pos)

        context = self.Context(self.avroType, exprContext.calls.union(set([self.desc])), exprResult, exprContext.retType)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["upcast"] = self.expr.jsonNode(lineNumbers, memo)
        out["as"] = self.avroPlaceholder.jsonNode(memo)
        return out

    desc = "upcast"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr, originalType): pass

@titus.util.case
class IfNotNull(Expression):
    """Abstract syntax tree for an ``ifnotnull`` block."""

    def __init__(self, exprs, thenClause, elseClause, pos=None):
        """:type exprs: dict from new variable names to titus.pfaast.Expression
        :param exprs: variables to check for ``null``
        :type thenClause: list of titus.pfaast.Expression
        :param thenClause: expressions to evaluate if all variables are not ``null``
        :type elseClause: list of titus.pfaast.Expression or ``None``
        :param elseClause: expressions to evaluate if present and any variable is ``null``
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(exprs, dict) or not all(isinstance(x, Expression) for x in exprs.values()):
            raise PFASyntaxException("\"exprs\" must be a dictionary of Expressions", pos)

        if not isinstance(thenClause, (list, tuple)) or not all(isinstance(x, Expression) for x in thenClause):
            raise PFASyntaxException("\"thenClause\" must be a list of Expressions", pos)

        if (not isinstance(elseClause, (list, tuple)) or not all(isinstance(x, Expression) for x in elseClause)) and not elseClause is None:
            raise PFASyntaxException("\"elseClause\" must be a list of Expressions or None", pos)

        if len(self.exprs) < 1:
            raise PFASyntaxException("\"ifnotnull\" must contain at least one symbol-expression mapping", pos)

        if len(self.thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(IfNotNull, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return IfNotNull(dict((k, v.replace(pf)) for k, v in self.exprs.items()),
                             [x.replace(pf) for x in self.thenClause],
                             [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        exprArgsScope = symbolTable.newScope(True, True)
        assignmentScope = symbolTable.newScope(False, False)

        symbolTypeResult = []
        for name, expr in self.exprs.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid symbol name".format(name), self.pos)

            exprCtx, exprRes = expr.walk(task, exprArgsScope, functionTable, engineOptions, version)

            avroType = None
            if isinstance(exprCtx.retType, AvroUnion) and any(isinstance(x, AvroNull) for x in exprCtx.retType.types):
                if len(exprCtx.retType.types) > 2:
                    avroType = AvroUnion([x for x in exprCtx.retType.types if not isinstance(x, AvroNull)])
                elif len(exprCtx.retType.types) > 1:
                    avroType = [x for x in exprCtx.retType.types if not isinstance(x, AvroNull)][0]

            if avroType is None:
                raise PFASemanticException("\"ifnotnull\" expressions must all be unions of something and null; case \"{0}\" has type {1}".format(name, ts(exprCtx.retType)), self.pos)

            assignmentScope.put(name, avroType)

            calls = calls.union(exprCtx.calls)

            symbolTypeResult.append((name, avroType, exprRes))

        thenScope = assignmentScope.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable, engineOptions, version) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions, version) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            thenType = thenResults[-1][0].retType
            elseType = elseResults[-1][0].retType
            try:
                retType = LabelData.broadestType([thenType, elseType])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err), self.pos)

            retType, elseTaskResults, elseSymbols = retType, [x[1] for x in elseResults], elseScope.inThisScope
        else:
            retType, elseTaskResults, elseSymbols = AvroNull(), None, None

        context = self.Context(retType, calls.union(set([self.desc])), symbolTypeResult, thenScope.inThisScope, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        jsonExprs = {}
        for name, expr in self.exprs.items():
            jsonExprs[name] = expr.jsonNode(lineNumbers, memo)
        out["ifnotnull"] = jsonExprs
        out["then"] = [x.jsonNode(lineNumbers, memo) for x in self.thenClause]
        if self.elseClause is not None:
            out["else"] = [x.jsonNode(lineNumbers, memo) for x in self.elseClause]
        return out

    desc = "ifnotnull"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbolTypeResult, thenSymbols, thenClause, elseSymbols, elseClause): pass

class BinaryFormatter(object):
    """Helper class for ``pack`` and ``unpack`` that checks format strings."""

    formatPad = re.compile("""\s*(pad)\s*""")
    formatBoolean = re.compile("""\s*(boolean)\s*""")
    formatByte = re.compile("""\s*(byte|int8)\s*""")
    formatUnsignedByte = re.compile("""\s*unsigned\s*(byte|int8)\s*""")
    formatShort = re.compile("""\s*(<|>|!|little|big|network)?\s*(short|int16)\s*""")
    formatUnsignedShort = re.compile("""\s*(<|>|!|little|big|network)?\s*(unsigned\s*short|unsigned\s*int16)\s*""")
    formatInt = re.compile("""\s*(<|>|!|little|big|network)?\s*(int|int32)\s*""")
    formatUnsignedInt = re.compile("""\s*(<|>|!|little|big|network)?\s*(unsigned\s*int|unsigned\s*int32)\s*""")
    formatLong = re.compile("""\s*(<|>|!|little|big|network)?\s*(long|long\s+long|int64)\s*""")
    formatUnsignedLong = re.compile("""\s*(<|>|!|little|big|network)?\s*(unsigned\s*long|unsigned\s*long\s+long|unsigned\s*int64)\s*""")
    formatFloat = re.compile("""\s*(<|>|!|little|big|network)?\s*(float|float32)\s*""")
    formatDouble = re.compile("""\s*(<|>|!|little|big|network)?\s*(double|float64)\s*""")
    formatRaw = re.compile("""\s*raw\s*""")
    formatRawSize = re.compile("""\s*raw\s*([0-9]+)\s*""")
    formatToNull = re.compile("""\s*(null\s*)?terminated\s*""")
    formatPrefixed = re.compile("""\s*(length\s*)?prefixed\s*""")
    
    class Declare(object):
        """Trait for binary format declaration."""
        pass

    @titus.util.case
    class DeclarePad(Declare):
        """Binary format declaration for a padded byte."""
        def __init__(self, value): pass
        avroType = AvroNull()
        def __str__(self):
            return "(" + repr(self.value) + ", \"x\", 1)"

    @titus.util.case
    class DeclareBoolean(Declare):
        """Binary format declaration for a boolean byte."""
        def __init__(self, value): pass
        avroType = AvroBoolean()
        def __str__(self):
            return "(" + repr(self.value) + ", \"?\", 1)"

    @titus.util.case
    class DeclareByte(Declare):
        """Binary format declaration for an integer byte."""
        def __init__(self, value, unsigned): pass
        avroType = AvroInt()
        def __str__(self):
            if self.unsigned:
                return "(" + repr(self.value) + ", \"B\", 1)"
            else:
                return "(" + repr(self.value) + ", \"b\", 1)"

    @titus.util.case
    class DeclareShort(Declare):
        """Binary format declaration for a short (16-bit) integer."""
        def __init__(self, value, littleEndian, unsigned): pass
        avroType = AvroInt()
        def __str__(self):
            if self.unsigned:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<H\", 2)"
                else:
                    return "(" + repr(self.value) + ", \">H\", 2)"
            else:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<h\", 2)"
                else:
                    return "(" + repr(self.value) + ", \">h\", 2)"

    @titus.util.case
    class DeclareInt(Declare):
        """Binary format declaration for a regular (32-bit) integer."""
        def __init__(self, value, littleEndian, unsigned): pass
        @property
        def avroType(self):
            if self.unsigned:
                return AvroLong()
            else:
                return AvroInt()
        def __str__(self):
            if self.unsigned:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<I\", 4)"
                else:
                    return "(" + repr(self.value) + ", \">I\", 4)"
            else:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<i\", 4)"
                else:
                    return "(" + repr(self.value) + ", \">i\", 4)"

    @titus.util.case
    class DeclareLong(Declare):
        """Binary format declaration for a long (64-bit) integer."""
        def __init__(self, value, littleEndian, unsigned): pass
        @property
        def avroType(self):
            if self.unsigned:
                return AvroDouble()
            else:
                return AvroLong()
        def __str__(self):
            if self.unsigned:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<Q\", 8)"
                else:
                    return "(" + repr(self.value) + ", \">Q\", 8)"
            else:
                if self.littleEndian:
                    return "(" + repr(self.value) + ", \"<q\", 8)"
                else:
                    return "(" + repr(self.value) + ", \">q\", 8)"

    @titus.util.case
    class DeclareFloat(Declare):
        """Binary format declaration for a single-precision (32-bit) floating point number."""
        def __init__(self, value, littleEndian): pass
        avroType = AvroFloat()
        def __str__(self):
            if self.littleEndian:
                return "(" + repr(self.value) + ", \"<f\", 4)"
            else:
                return "(" + repr(self.value) + ", \">f\", 4)"

    @titus.util.case
    class DeclareDouble(Declare):
        """Binary format declaration for a double-precision (64-bit) floating point number."""
        def __init__(self, value, littleEndian): pass
        avroType = AvroDouble()
        def __str__(self):
            if self.littleEndian:
                return "(" + repr(self.value) + ", \"<d\", 8)"
            else:
                return "(" + repr(self.value) + ", \">d\", 8)"

    @titus.util.case
    class DeclareRaw(Declare):
        """Binary format declaration for arbitrary-width raw data."""
        def __init__(self, value): pass
        avroType = AvroBytes()
        def __str__(self):
            return "(" + repr(self.value) + ", \"raw\", None)"

    @titus.util.case
    class DeclareRawSize(Declare):
        """Binary format declaration for fixed-width raw data."""
        def __init__(self, value, size): pass
        avroType = AvroBytes()
        def __str__(self):
            return "(" + repr(self.value) + ", \"raw\", " + str(self.size) + ")"

    @titus.util.case
    class DeclareToNull(Declare):
        """Binary format declaration for null-terminated raw data."""
        def __init__(self, value): pass
        avroType = AvroBytes()
        def __str__(self):
            return "(" + repr(self.value) + ", \"tonull\", None)"

    @titus.util.case
    class DeclarePrefixed(Declare):
        """Binary format declaration for length-prefixed raw data."""
        def __init__(self, value): pass
        avroType = AvroBytes()
        def __str__(self):
            return "(" + repr(self.value) + ", \"prefixed\", None)"

    @staticmethod
    def formatToDeclare(value, f, pos, output):
        """Convert a format string to a titus.pfaast.BinaryFormatter.Declare object.

        :type value: titus.pfaast.TaskResult
        :param value: task result, usually Python code in a string, passed to the format declarer
        :type f: string
        :param f: format string
        :type pos: string or ``None``
        :param pos: source file location from the locator mark for error messages
        :type output: bool
        :param output: ``True`` for ``pack``, ``False`` for ``unpack``
        :rtype: titus.pfaast.BinaryFormatter.Declare subclass
        :return: format declarer
        """

        if re.match(BinaryFormatter.formatPad, f):             return BinaryFormatter.DeclarePad(value)
        elif re.match(BinaryFormatter.formatBoolean, f):       return BinaryFormatter.DeclareBoolean(value)
        elif re.match(BinaryFormatter.formatByte, f):          return BinaryFormatter.DeclareByte(value, False)
        elif re.match(BinaryFormatter.formatUnsignedByte, f):  return BinaryFormatter.DeclareByte(value, True)
        elif re.match(BinaryFormatter.formatShort, f):         return BinaryFormatter.DeclareShort(value, "<" in f or "little" in f, False)
        elif re.match(BinaryFormatter.formatUnsignedShort, f): return BinaryFormatter.DeclareShort(value, "<" in f or "little" in f, True)
        elif re.match(BinaryFormatter.formatInt, f):           return BinaryFormatter.DeclareInt(value, "<" in f or "little" in f, False)
        elif re.match(BinaryFormatter.formatUnsignedInt, f):   return BinaryFormatter.DeclareInt(value, "<" in f or "little" in f, True)
        elif re.match(BinaryFormatter.formatLong, f):          return BinaryFormatter.DeclareLong(value, "<" in f or "little" in f, False)
        elif re.match(BinaryFormatter.formatUnsignedLong, f):  return BinaryFormatter.DeclareLong(value, "<" in f or "little" in f, True)
        elif re.match(BinaryFormatter.formatFloat, f):         return BinaryFormatter.DeclareFloat(value, "<" in f or "little" in f)
        elif re.match(BinaryFormatter.formatDouble, f):        return BinaryFormatter.DeclareDouble(value, "<" in f or "little" in f)
        elif re.match(BinaryFormatter.formatRawSize, f):       return BinaryFormatter.DeclareRawSize(value, int(re.match(BinaryFormatter.formatRawSize, f).group(1)))
        elif re.match(BinaryFormatter.formatToNull, f):        return BinaryFormatter.DeclareToNull(value)
        elif re.match(BinaryFormatter.formatPrefixed, f):      return BinaryFormatter.DeclarePrefixed(value)
        elif re.match(BinaryFormatter.formatRaw, f):
            if output:
                return BinaryFormatter.DeclareRaw(value)
            else:
                raise PFASemanticException("cannot read from unsized \"raw\" in binary formatter", self.pos)
        else:
            raise PFASemanticException("unrecognized formatter \"{0}\" found in unpack's \"format\"".format(f), self.pos)

@titus.util.case
class Pack(Expression):
    """Abstract syntax tree for a ``pack`` construct."""

    def __init__(self, exprs, pos=None):
        """:type exprs: list of (string, titus.pfaast.Expression)
        :param exprs: (format, expression) pairs
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(exprs, (list, tuple)) or not all(isinstance(x, (list, tuple)) and len(x) == 2 and isinstance(x[0], six.string_types) and isinstance(x[1], Expression) for x in exprs):
            raise PFASyntaxException("\"exprs\" must be a list of (string, Expression) tuples", pos)

        if len(self.exprs) < 1:
            raise PFASyntaxException("\"pack\" must contain at least one format-expression mapping", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Pack, self).collect(pf) + \
               titus.util.flatten(v.collect(pf) for k, v in self.exprs)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Pack([(k, v.replace(pf)) for k, v in self.exprs], self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()

        exprsDeclareRes = []
        for f, expr in self.exprs:
            exprCtx, exprRes = expr.walk(task, symbolTable.newScope(True, True), functionTable, engineOptions, version)

            declare = BinaryFormatter.formatToDeclare(exprRes, f, self.pos, True)

            if not declare.avroType.accepts(exprCtx.retType):
                raise PFASemanticException("\"pack\" expression with type {0} cannot be cast to {1}".format(exprCtx.retType, f), self.pos)
            calls = calls.union(exprCtx.calls)

            exprsDeclareRes.append(declare)

        context = self.Context(AvroBytes(), calls.union(set([self.desc])), exprsDeclareRes, self.pos)
        return (context, task(context, engineOptions))

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["pack"] = []
        for f, expr in self.exprs:
            out["pack"].append({f: expr.jsonNode(lineNumbers, memo)})
        return out

    desc = "pack"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, exprsDeclareRes, pos): pass

@titus.util.case
class Unpack(Expression):
    """Abstract syntax tree for the ``unpack`` construct."""

    def __init__(self, bytes, format, thenClause, elseClause, pos=None):
        """:type bytes: titus.pfaast.Expression
        :param bytes: expression that evaluates to a ``bytes`` object
        :type format: list of (string, string)
        :param format: (new variable name, formatter) pairs
        :type thenClause: list of titus.pfaast.Expression
        :param thenClause: expressions to evaluate if the ``bytes`` matches the format
        :type elseClause: list of titus.pfaast.Expression or ``None``
        :param elseClause: expressions to evaluate if present and the ``bytes`` does not match the format
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(bytes, Expression):
            raise PFASyntaxException("\"bytes\" must be an Expression", pos)

        if not isinstance(format, (list, tuple)) or not all(isinstance(x, (list, tuple)) and len(x) == 2 and isinstance(x[0], six.string_types) and isinstance(x[1], six.string_types) for x in format):
            raise PFASyntaxException("\"format\" must be a list of (string, string) tuples", pos)

        if not isinstance(thenClause, (list, tuple)) or not all(isinstance(x, Expression) for x in thenClause):
            raise PFASyntaxException("\"thenClause\" must be a list of Expressions or None", pos)

        if (not isinstance(elseClause, (list, tuple)) or not all(isinstance(x, Expression) for x in elseClause)) and not elseClause is None:
            raise PFASyntaxException("\"elseClause\" must be a list of Expressions or None", pos)

        if len(format) < 1:
            raise PFASyntaxException("unpack's \"format\" must contain at least one symbol-format mapping", pos)

        if len(thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", pos)

        if elseClause is not None and len(elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Unpack, self).collect(pf) + \
               self.bytes.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Unpack(self.bytes.replace(pf),
                          self.format,
                          [x.replace(pf) for x in self.thenClause],
                          [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        calls = set()
        
        bytesScope = symbolTable.newScope(True, True)
        assignmentScope = symbolTable.newScope(False, False)

        bytesCtx, bytesRes = self.bytes.walk(task, bytesScope, functionTable, engineOptions, version)
        if not isinstance(bytesCtx.retType, AvroBytes):
            raise PFASemanticException("\"unpack\" expression must be a bytes object", self.pos)
        calls = calls.union(bytesCtx.calls)

        formatter = []
        for s, f in self.format:
            if assignmentScope.get(s) is not None:
                raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(s), self.pos)

            if not validSymbolName(s):
                raise PFASemanticException("\"{0}\" is not a valid symbol name", self.pos)

            formatter.append(BinaryFormatter.formatToDeclare(s, f, self.pos, False))
            assignmentScope.put(s, formatter[-1].avroType)

        thenScope = assignmentScope.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable, engineOptions, version) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions, version) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            thenType = thenResults[-1][0].retType
            elseType = elseResults[-1][0].retType
            try:
                retType = LabelData.broadestType([thenType, elseType])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err), self.pos)

            retType, elseTaskResults, elseSymbols = retType, [x[1] for x in elseResults], elseScope.inThisScope
        else:
            retType, elseTaskResults, elseSymbols = AvroNull(), None, None

        context = self.Context(retType, calls.union(set([self.desc])), bytesRes, formatter, thenScope.inThisScope, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["unpack"] = self.bytes.jsonNode(lineNumbers, memo)
        out["format"] = []
        for s, f in self.format:
            out["format"].append({s: f})
        out["then"] = [x.jsonNode(lineNumbers, memo) for x in self.thenClause]
        if self.elseClause is not None:
            out["else"] = [x.jsonNode(lineNumbers, memo) for x in self.elseClause]
        return out

    desc = "unpack"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, bytes, formatter, thenSymbols, thenClause, elseSymbols, elseClause): pass
        
@titus.util.case
class Doc(Expression):
    """Abstract syntax tree for inline documentation."""

    def __init__(self, comment, pos=None):
        """:type comment: string
        :param comment: inline documentation
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(comment, six.string_types):
            raise PFASyntaxException("\"comment\" must be a string", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["doc"] = self.comment
        return out

    desc = "doc"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class Error(Expression):
    """Abstract syntax tree for a user-defined error."""

    def __init__(self, message, code, pos=None):
        """:type message: string
        :param message: error message
        :type code: integer or ``None``
        :param code: optional error code
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(message, six.string_types):
            raise PFASyntaxException("\"message\" must be a string", pos)

        if not (isinstance(code, int) and code < 0) and code is not None:
            raise PFASyntaxException("\"code\" must be a negative int or None", pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        context = self.Context(ExceptionType(), set([self.desc]), self.message, self.code, self.pos)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["error"] = self.message
        if self.code is not None:
            out["code"] = self.code
        return out

    desc = "error"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, message, code, pos): pass

@titus.util.case
class Try(Expression):
    """Abstract syntax tree for a ``try`` form."""

    def __init__(self, exprs, filter, pos=None):
        """:type exprs: list of titus.pfaast.Expression
        :param exprs: expressions to evaluate that might raise an exception
        :type filter: ``None`` or a list of strings and integers
        :param filter: optional filter for error messages or error codes
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(exprs, (list, tuple)) or not all(isinstance(x, Expression) for x in exprs):
            raise PFASyntaxException("\"exprs\" must be a list of Expressions", pos)

        if (not isinstance(filter, (list, tuple)) or not all(isinstance(x, (six.string_types, int, long)) for x in filter)) and not filter is None:
            raise PFASyntaxException("\"filter\" must be a list of strings and integers or None", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Try, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Try([x.replace(pf) for x in self.exprs],
                       self.filter,
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        scope = symbolTable.newScope(True, True)
        results = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.exprs]

        exprType = results[-1][0].retType
        if isinstance(exprType, ExceptionType):
            exprType = AvroNull()
            inferredType = AvroNull()
        elif isinstance(exprType, AvroUnion):
            if AvroNull() in exprType.types:
                inferredType = exprType
            else:
                inferredType = AvroUnion(exprType.types + [AvroNull()])
        else:
            inferredType = AvroUnion([exprType, AvroNull()])

        context = self.Context(inferredType, set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, [x[1] for x in results], exprType, self.filter)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["try"] = [x.jsonNode(lineNumbers, memo) for x in self.exprs]
        if self.filter is not None:
            out["filter"] = self.filter
        return out

    desc = "try"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprs, exprType, filter): pass

@titus.util.case
class Log(Expression):
    """Abstract syntax tree for log messages."""

    def __init__(self, exprs, namespace, pos=None):
        """:type exprs: list of titus.pfaast.Expression
        :param exprs: expressions to print to the log
        :type namespace: string or ``None``
        :param namespace: optional namespace string
        :type pos: string or ``None``
        :param pos: source file location from the locator mark
        """

        if not isinstance(pos, six.string_types) and not pos is None:
            raise PFASyntaxException("\"pos\" must be a string or None", None)

        if not isinstance(exprs, (list, tuple)) or not all(isinstance(x, Expression) for x in exprs):
            raise PFASyntaxException("\"exprs\" must be a list of Expressions", pos)

        if not isinstance(namespace, six.string_types) and not namespace is None:
            raise PFASyntaxException("\"namespace\" must be a string or None", pos)

    def collect(self, pf):
        """Walk over tree applying a partial function, returning a list of results in its domain.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning anything
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: list of function results
        :return: a result for each abstract syntax tree node in the ``pf`` function's domain
        """
        return super(Log, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs)

    def replace(self, pf):
        """Walk over tree applying a partial function, returning a transformed copy of the tree.

        :type pf: callable with ``isDefinedAt`` method
        :param pf: partial function that takes any titus.pfaast.Ast as an argument, returning a replacement titus.pfaast.Ast
        :type pf.isDefinedAt: callable
        :param pf.isDefinedAt: domain that takes any titus.pfaast.Ast as an argument, returning ``True`` if this item is in the domain, ``False`` otherwise
        :rtype: new titus.pfaast.Ast tree
        :return: tree with nodes in the ``pf`` function's domain transformed; everything else left as-is
        """
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Log([x.replace(pf) for x in self.exprs],
                       self.namespace,
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions, version):
        """Walk over tree applying a titus.pfaast.Task while checking for semantic errors.

        This is how Python is generated from an abstract syntax tree: the titus.pfaast.Task in that case is titus.genpy.GeneratePython.

        :type task: titus.pfaast.Task
        :param task: generic task to perform on this abstract syntax tree node's context
        :type symbolTable: titus.pfaast.SymbolTable
        :param symbolTable: used to look up symbols, cells, and pools
        :type functionTable: titus.pfaast.FunctionTable
        :param functionTable: used to look up functions
        :type engineOptions: titus.options.EngineOptions
        :param engineOptions: implementation options
        :type version: titus.signature.PFAVersion
        :param version: version of the PFA language in which to interpret this PFA
        :rtype: (titus.pfaast.AstContext, titus.pfaast.TaskResult)
        :return: (information about this abstract syntax tree node after type-checking, result of the generic task)
        """
        scope = symbolTable.newScope(True, True)
        results = [x.walk(task, scope, functionTable, engineOptions, version) for x in self.exprs]
        context = self.Context(AvroNull(), set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, results, self.namespace)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        """Convert this abstract syntax tree to Pythonized JSON.

        :type lineNumbers: bool
        :param lineNumbers: if ``True``, include locator marks in each JSON object
        :type memo: set of string
        :param memo: used to avoid recursion; provide an empty set if unsure
        :rtype: Pythonized JSON
        :return: JSON representation
        """
        out = self.startDict(lineNumbers)
        out["log"] = [x.jsonNode(lineNumbers, memo) for x in self.exprs]
        if self.namespace is not None:
            out["namespace"] = self.namespace
        return out

    desc = "log"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprTypes, namespace): pass

