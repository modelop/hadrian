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
import math
import threading
import time
import random
import struct

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from titus.errors import *
import titus.pfaast
import titus.datatype
import titus.fcn
import titus.options
import titus.P as P
import titus.reader
import titus.signature
import titus.util
from titus.util import DynamicScope
import titus.version

from titus.pfaast import EngineConfig
from titus.pfaast import Cell as AstCell
from titus.pfaast import Pool as AstPool
from titus.pfaast import FcnDef
from titus.pfaast import FcnRef
from titus.pfaast import FcnRefFill
from titus.pfaast import CallUserFcn
from titus.pfaast import Call
from titus.pfaast import Ref
from titus.pfaast import LiteralNull
from titus.pfaast import LiteralBoolean
from titus.pfaast import LiteralInt
from titus.pfaast import LiteralLong
from titus.pfaast import LiteralFloat
from titus.pfaast import LiteralDouble
from titus.pfaast import LiteralString
from titus.pfaast import LiteralBase64
from titus.pfaast import Literal
from titus.pfaast import NewObject
from titus.pfaast import NewArray
from titus.pfaast import Do
from titus.pfaast import Let
from titus.pfaast import SetVar
from titus.pfaast import AttrGet
from titus.pfaast import AttrTo
from titus.pfaast import CellGet
from titus.pfaast import CellTo
from titus.pfaast import PoolGet
from titus.pfaast import PoolTo
from titus.pfaast import PoolDel
from titus.pfaast import If
from titus.pfaast import Cond
from titus.pfaast import While
from titus.pfaast import DoUntil
from titus.pfaast import For
from titus.pfaast import Foreach
from titus.pfaast import Forkeyval
from titus.pfaast import CastCase
from titus.pfaast import CastBlock
from titus.pfaast import Upcast
from titus.pfaast import IfNotNull
from titus.pfaast import BinaryFormatter
from titus.pfaast import Pack
from titus.pfaast import Unpack
from titus.pfaast import Doc
from titus.pfaast import Error
from titus.pfaast import Try
from titus.pfaast import Log

from titus.pfaast import Method
from titus.pfaast import ArrayIndex
from titus.pfaast import MapIndex
from titus.pfaast import RecordIndex

from titus.pmml.reader import pmmlToAst

class GeneratePython(titus.pfaast.Task):
    """A ``titus.pfaast.Task`` for turning PFA into executable Python."""

    @staticmethod
    def makeTask(style):
        """Make a ``titus.genpy.GeneratePython`` Task with a particular style.

        Currently, the only style is "pure" (``titus.genpy.GeneratePythonPure``).
        """

        if style == "pure":
            return GeneratePythonPure()
        else:
            raise NotImplementedError("unrecognized style " + style)

    def commandsMap(self, codes, indent):
        """Concatenate commands for a map-type engine."""

        suffix = indent + "self.actionsFinished += 1\n" + \
                 indent + "return last\n"
        return "".join(indent + x + "\n" for x in codes[:-1]) + indent + "last = " + codes[-1] + "\n" + suffix

    def commandsEmit(self, codes, indent):
        """Concatenate commands for an emit-type engine."""

        suffix = indent + "self.actionsFinished += 1\n"
        return "".join(indent + x + "\n" for x in codes) + suffix

    def commandsFold(self, codes, indent):
        """Concatenate commands for a fold-type engine."""

        prefix = indent + "scope.let({'tally': self.tally})\n"
        suffix = indent + "self.tally = last\n" + \
                 indent + "self.actionsFinished += 1\n" + \
                 indent + "return self.tally\n"
        return prefix + "".join(indent + x + "\n" for x in codes[:-1]) + indent + "last = " + codes[-1] + "\n" + suffix

    def commandsFoldMerge(self, codes, indent):
        """Concatenate commands for the merge section of a fold-type engine."""

        suffix = indent + "self.tally = last\n" + \
                 indent + "return self.tally\n"
        return "".join(indent + x + "\n" for x in codes[:-1]) + indent + "last = " + codes[-1] + "\n" + suffix

    def commandsBeginEnd(self, codes, indent):
        """Concatenate commands for the begin or end method."""

        return "".join(indent + x + "\n" for x in codes)

    def reprPath(self, path):
        """Build a path for "attr", "cell", or "pool" special forms."""

        out = []
        for p in path:
            if isinstance(p, ArrayIndex):
                out.append(p.i)
            elif isinstance(p, MapIndex):
                out.append(p.k)
            elif isinstance(p, RecordIndex):
                out.append(repr(p.f))
            else:
                raise Exception
        return ", ".join(out)

    def __call__(self, context, engineOptions):
        """Turn a PFA Context into Python."""

        if isinstance(context, EngineConfig.Context):
            if context.name is None:
                name = titus.util.uniqueEngineName()
            else:
                name = context.name

            begin, beginSymbols, beginCalls = context.begin
            action, actionSymbols, actionCalls = context.action
            end, endSymbols, endCalls = context.end

            callGraph = {"(begin)": beginCalls, "(action)": actionCalls, "(end)": endCalls}
            if context.merge is not None:
                mergeTasks, mergeSymbols, mergeCalls = context.merge
                callGraph["(merge)"] = mergeCalls
            for fname, fctx in context.fcns:
                callGraph[fname] = fctx.calls

            out = ["class PFA_" + name + """(PFAEngine):
    def __init__(self, cells, pools, config, options, log, emit, zero, instance, rand):
        self.actionsStarted = 0
        self.actionsFinished = 0
        self.cells = cells
        self.pools = pools
        self.config = config
        self.inputType = config.input
        self.outputType = config.output
        self.options = options
        self.log = log
        self.emit = emit
        self.instance = instance
        self.rand = rand
        self.callGraph = """ + repr(callGraph) + "\n"]

            if context.method == Method.FOLD:
                out.append("        self.tally = zero\n")

            out.append("""    def initialize(self):
        self
""")

            for ufname, fcnContext in context.fcns:
                out.append("        self.f[" + repr(ufname) + "] = " + self(fcnContext, engineOptions) + "\n")

            if len(begin) > 0:
                out.append("""
    def begin(self):
        state = ExecutionState(self.options, self.rand, 'action', self.parser)
        scope = DynamicScope(None)
        scope.let({'name': self.config.name, 'instance': self.instance, 'metadata': self.config.metadata})
        if self.config.version is not None:
            scope.let({'version': self.config.version})
""" + self.commandsBeginEnd(begin, "        "))
            else:
                out.append("""
    def begin(self):
        pass
""")

            if context.method == Method.MAP:
                commands = self.commandsMap(action, "            ")
            elif context.method == Method.EMIT:
                commands = self.commandsEmit(action, "            ")
            elif context.method == Method.FOLD:
                commands = self.commandsFold(action, "            ")

            out.append("""
    def action(self, input, check=True):
        if check:
            input = checkData(input, self.inputType)
        state = ExecutionState(self.options, self.rand, 'action', self.parser)
        scope = DynamicScope(None)
        for cell in self.cells.values():
            cell.maybeSaveBackup()
        for pool in self.pools.values():
            pool.maybeSaveBackup()
        self.actionsStarted += 1
        try:
            scope.let({'input': input, 'name': self.config.name, 'instance': self.instance, 'metadata': self.config.metadata, 'actionsStarted': self.actionsStarted, 'actionsFinished': self.actionsFinished})
            if self.config.version is not None:
                scope.let({'version': self.config.version})
""" + commands)

            out.append("""        except Exception:
            for cell in self.cells.values():
                cell.maybeRestoreBackup()
            for pool in self.pools.values():
                pool.maybeRestoreBackup()
            raise
""")

            if context.merge is not None:
                out.append("""
    def merge(self, tallyOne, tallyTwo):
        state = ExecutionState(self.options, self.rand, 'merge', self.parser)
        scope = DynamicScope(None)
        for cell in self.cells.values():
            cell.maybeSaveBackup()
        for pool in self.pools.values():
            pool.maybeSaveBackup()
        try:
            scope.let({'tallyOne': tallyOne, 'tallyTwo': tallyTwo, 'name': self.config.name, 'instance': self.instance, 'metadata': self.config.metadata})
            if self.config.version is not None:
                scope.let({'version': self.config.version})
""" + self.commandsFoldMerge(mergeTasks, "            "))

                out.append("""        except Exception:
            for cell in self.cells.values():
                cell.maybeRestoreBackup()
            for pool in self.pools.values():
                pool.maybeRestoreBackup()
            raise
""")

            if len(end) > 0:
                tallyLine = ""
                if context.method == Method.FOLD:
                    tallyLine = """        scope.let({'tally': self.tally})\n"""
                
                out.append("""
    def end(self):
        state = ExecutionState(self.options, self.rand, 'action', self.parser)
        scope = DynamicScope(None)
        scope.let({'name': self.config.name, 'instance': self.instance, 'metadata': self.config.metadata, 'actionsStarted': self.actionsStarted, 'actionsFinished': self.actionsFinished})
        if self.config.version is not None:
            scope.let({'version': self.config.version})
""" + tallyLine + self.commandsBeginEnd(end, "        "))
            else:
                out.append("""
    def end(self):
        pass
""")

            out.append("""
    def pooldel(self, name, item):
        p = self.pools[name]
        try:
            del p.value[item]
        except KeyError:
            pass
        return None
""")

            return "".join(out)

        elif isinstance(context, FcnDef.Context):
            return "labeledFcn(lambda state, scope: do(" + ", ".join(context.exprs) + "), [" + ", ".join(map(repr, context.paramNames)) + "])"

        elif isinstance(context, FcnRef.Context):
            return "self.f[" + repr(context.fcn.name) + "]"

        elif isinstance(context, FcnRefFill.Context):
            reducedArgs = ["\"$" + str(x) + "\"" for x in xrange(len(context.fcnType.params))]
            j = 0
            args = []
            for name in context.originalParamNames:
                if name in context.argTypeResult:
                    args.append(context.argTypeResult[name][1])
                else:
                    args.append("scope.get(\"$" + str(j) + "\")")
                    j += 1

            return "labeledFcn(lambda state, scope: call(state, DynamicScope(scope), self.f[" + repr(context.fcn.name) + "], [" + ", ".join(args) + "]), [" + ", ".join(reducedArgs) + "])"

        elif isinstance(context, CallUserFcn.Context):
            return "call(state, DynamicScope(None), self.f['u.' + " + context.name + "], [" + ", ".join(context.args) + "])"

        elif isinstance(context, Call.Context):
            return context.fcn.genpy(context.paramTypes + [context.retType], context.args, context.pos)

        elif isinstance(context, Ref.Context):
            return "scope.get({0})".format(repr(context.name))

        elif isinstance(context, LiteralNull.Context):
            return "None"

        elif isinstance(context, LiteralBoolean.Context):
            return str(context.value)

        elif isinstance(context, LiteralInt.Context):
            return str(context.value)

        elif isinstance(context, LiteralLong.Context):
            return str(context.value)

        elif isinstance(context, LiteralFloat.Context):
            return str(float(context.value))

        elif isinstance(context, LiteralDouble.Context):
            return str(float(context.value))

        elif isinstance(context, LiteralString.Context):
            return repr(context.value)

        elif isinstance(context, LiteralBase64.Context):
            return repr(context.value)

        elif isinstance(context, Literal.Context):
            return repr(titus.datatype.jsonDecoder(context.retType, json.loads(context.value)))

        elif isinstance(context, NewObject.Context):
            return "{" + ", ".join(repr(k) + ": " + v for k, v in context.fields.items()) + "}"

        elif isinstance(context, NewArray.Context):
            return "[" + ", ".join(context.items) + "]"

        elif isinstance(context, Do.Context):
            return "do(" + ", ".join(context.exprs) + ")"

        elif isinstance(context, Let.Context):
            return "scope.let({" + ", ".join(repr(n) + ": " + e for n, t, e in context.nameTypeExpr) + "})"

        elif isinstance(context, SetVar.Context):
            return "scope.set({" + ", ".join(repr(n) + ": " + e for n, t, e in context.nameTypeExpr) + "})"

        elif isinstance(context, AttrGet.Context):
            return "get(" + context.expr + ", [" + self.reprPath(context.path) + "], 2000, 2001, \"attr\", " + repr(context.pos) + ")"

        elif isinstance(context, AttrTo.Context):
            return "update(state, scope, {0}, [{1}], {2}, 2002, 2003, \"attr-to\", {3})".format(context.expr, self.reprPath(context.path), context.to, repr(context.pos))

        elif isinstance(context, CellGet.Context):
            return "get(self.cells[{0}].value, [{1}], 2004, 2005, \"cell\", {2})".format(repr(context.cell), self.reprPath(context.path), repr(context.pos))

        elif isinstance(context, CellTo.Context):
            return "self.cells[{0}].update(state, scope, [{1}], {2}, 2006, 2007, \"cell-to\", {3})".format(repr(context.cell), self.reprPath(context.path), context.to, repr(context.pos))

        elif isinstance(context, PoolGet.Context):
            return "get(self.pools[{0}].value, [{1}], 2008, 2009, \"pool\", {2})".format(repr(context.pool), self.reprPath(context.path), repr(context.pos))

        elif isinstance(context, PoolTo.Context):
            return "self.pools[{0}].update(state, scope, [{1}], {2}, {3}, 2010, 2011, \"pool-to\", {4})".format(repr(context.pool), self.reprPath(context.path), context.to, context.init, repr(context.pos))

        elif isinstance(context, PoolDel.Context):
            return "self.pooldel({0}, {1})".format(repr(context.pool), context.dell)

        elif isinstance(context, If.Context):
            if context.elseClause is None:
                return "ifThen(state, scope, lambda state, scope: {0}, lambda state, scope: do({1}))".format(context.predicate, ", ".join(context.thenClause))
            else:
                return "ifThenElse(state, scope, lambda state, scope: {0}, lambda state, scope: do({1}), lambda state, scope: do({2}))".format(context.predicate, ", ".join(context.thenClause), ", ".join(context.elseClause))

        elif isinstance(context, Cond.Context):
            if not context.complete:
                return "cond(state, scope, [{0}])".format(", ".join("(lambda state, scope: {0}, lambda state, scope: do({1}))".format(walkBlock.pred, ", ".join(walkBlock.exprs)) for walkBlock in context.walkBlocks))
            else:
                return "condElse(state, scope, [{0}], lambda state, scope: do({1}))".format(", ".join("(lambda state, scope: {0}, lambda state, scope: do({1}))".format(walkBlock.pred, ", ".join(walkBlock.exprs)) for walkBlock in context.walkBlocks[:-1]), ", ".join(context.walkBlocks[-1].exprs))

        elif isinstance(context, While.Context):
            return "doWhile(state, scope, lambda state, scope: {0}, lambda state, scope: do({1}))".format(context.predicate, ", ".join(context.loopBody))

        elif isinstance(context, DoUntil.Context):
            return "doUntil(state, scope, lambda state, scope: {0}, lambda state, scope: do({1}))".format(context.predicate, ", ".join(context.loopBody))

        elif isinstance(context, For.Context):
            return "doFor(state, scope, lambda state, scope: scope.let({" + ", ".join(repr(n) + ": " + e for n, t, e in context.initNameTypeExpr) + "}), lambda state, scope: " + context.predicate + ", lambda state, scope: scope.set({" + ", ".join(repr(n) + ": " + e for n, t, e in context.stepNameTypeExpr) + "}), lambda state, scope: do(" + ", ".join(context.loopBody) + "))"

        elif isinstance(context, Foreach.Context):
            return "doForeach(state, scope, {0}, {1}, lambda state, scope: do({2}))".format(repr(context.name), context.objExpr, ", ".join(context.loopBody))

        elif isinstance(context, Forkeyval.Context):
            return "doForkeyval(state, scope, {0}, {1}, {2}, lambda state, scope: do({3}))".format(repr(context.forkey), repr(context.forval), context.objExpr, ", ".join(context.loopBody))

        elif isinstance(context, CastCase.Context):
            return "(" + repr(context.name) + ", " + repr(context.toType) + ", lambda state, scope: do(" + ", ".join(context.clause) + "))"

        elif isinstance(context, CastBlock.Context):
            return "cast(state, scope, " + context.expr + ", " + repr(context.exprType) + ", [" + ", ".join(caseRes for castCtx, caseRes in context.cases) + "], " + repr(context.partial) + ", self.parser)"

        elif isinstance(context, Upcast.Context):
            if isinstance(context.retType, titus.datatype.AvroUnion) and not isinstance(context.originalType, titus.datatype.AvroUnion):
                for t in context.retType.types:
                    if t.accepts(context.originalType):
                        return "wrapAsUnion({}, {})".format(context.expr, repr(t.name))
                raise Exception   # type-checking should have prevented this
            else:
                return context.expr

        elif isinstance(context, IfNotNull.Context):
            if context.elseClause is None:
                return "ifNotNull(state, scope, {" + ", ".join(repr(n) + ": " + e for n, t, e in context.symbolTypeResult) + "}, {" + ", ".join(repr(n) + ": '" + repr(t) + "'" for n, t, e in context.symbolTypeResult) + "}, lambda state, scope: do(" + ", ".join(context.thenClause) + "))"
            else:
                return "ifNotNullElse(state, scope, {" + ", ".join(repr(n) + ": " + e for n, t, e in context.symbolTypeResult) + "}, {" + ", ".join(repr(n) + ": '" + repr(t) + "'" for n, t, e in context.symbolTypeResult) + "}, lambda state, scope: do(" + ", ".join(context.thenClause) + "), lambda state, scope: do(" + ", ".join(context.elseClause) + "))"

        elif isinstance(context, Pack.Context):
            return "pack(state, scope, [" + ", ".join("(" + str(d.value) + ", " + str(d) + ")" for d in context.exprsDeclareRes) + "], " + repr(context.pos) + ")"

        elif isinstance(context, Unpack.Context):
            if context.elseClause is None:
                return "unpack(state, scope, " + context.bytes + ", [" + ", ".join(str(x) for x in context.formatter) + "], lambda state, scope: do(" + ", ".join(context.thenClause) + "))"
            else:
                return "unpackElse(state, scope, " + context.bytes + ", [" + ", ".join(str(x) for x in context.formatter) + "], lambda state, scope: do(" + ", ".join(context.thenClause) + "), lambda state, scope: do(" + ", ".join(context.elseClause) + "))"

        elif isinstance(context, Doc.Context):
            return "None"

        elif isinstance(context, Error.Context):
            return "error(" + repr(context.message) + ", " + repr(context.code) + ", " + repr(context.pos) + ")"

        elif isinstance(context, Try.Context):
            return "tryCatch(state, scope, lambda state, scope: do(" + ", ".join(context.exprs) + "), " + repr(context.filter) + ")"

        elif isinstance(context, Log.Context):
            return "self.log([{0}], {1})".format(", ".join(x[1] for x in context.exprTypes), repr(context.namespace))

        else:
            raise PFASemanticException("unrecognized context class: " + str(type(context)), "")

class GeneratePythonPure(GeneratePython):
    """A ``titus.pfaast.Task`` for generating a pure Python executable.

    This is a dummy class; all of the work is done in ``titus.genpy.GeneratePython``. Non-pure styles would be siblings of this class.
    """
    pass

###########################################################################

class ExecutionState(object):
    """Passed through a running PFA engine, carrying the state of that engine.

    Every PFA function implementation gets this state as an argument.

    It includes execution options, random number generators, whether we are in begin, action, or end, etc.
    """

    def __init__(self, options, rand, routine, parser):
        self.rand = rand
        self.parser = parser

        if routine == "begin":
            self.timeout = options.timeout_begin
        elif routine == "action":
            self.timeout = options.timeout_action
        elif routine == "end":
            self.timeout = options.timeout_end

        self.startTime = time.time()

    def checkTime(self):
        if self.timeout > 0 and (time.time() - self.startTime) * 1000 > self.timeout:
            raise PFATimeoutException("exceeded timeout of {0} milliseconds".format(self.timeout))

class SharedState(object):
    """Represents the state of all shared cells and pools at runtime."""

    def __init__(self):
        self.cells = {}
        self.pools = {}

    def __repr__(self):
        return "SharedState({0} cells, {1} pools)".format(len(self.cells), len(self.pools))

class PersistentStorageItem(object):
    """Represents the state of one cell or pool at runtime."""

    def __init__(self, value, shared, rollback, source):
        self.value = value
        self.shared = shared
        self.rollback = rollback
        self.source = source

class Cell(PersistentStorageItem):
    """Represents the state of a cell at runtime."""

    def __init__(self, value, shared, rollback, source):
        if shared:
            self.lock = threading.Lock()
        super(Cell, self).__init__(value, shared, rollback, source)

    def __repr__(self):
        contents = repr(self.value)
        if len(contents) > 30:
            contents = contents[:27] + "..."
        return "Cell(" + ("shared, " if self.shared else "") + ("rollback, " if self.rollback else "") + contents + ")"
            
    def update(self, state, scope, path, to, arrayErrCode, mapErrCode, fcnName, pos):
        result = None
        if self.shared:
            self.lock.acquire()
            self.value = update(state, scope, self.value, path, to, arrayErrCode, mapErrCode, fcnName, pos)
            result = self.value
            self.lock.release()
        else:
            self.value = update(state, scope, self.value, path, to, arrayErrCode, mapErrCode, fcnName, pos)
            result = self.value
        return result

    def maybeSaveBackup(self):
        if self.rollback:
            self.oldvalue = self.value

    def maybeRestoreBackup(self):
        if self.rollback:
            self.value = self.oldvalue

class Pool(PersistentStorageItem):
    """Represents the state of a pool at runtime."""

    def __init__(self, value, shared, rollback, source):
        if shared:
            self.locklock = threading.Lock()
            self.locks = {}
        super(Pool, self).__init__(value, shared, rollback, source)

    def __repr__(self):
        contents = repr(self.value)
        if len(contents) > 30:
            contents = contents[:27] + "..."
        return "Pool(" + ("shared, " if self.shared else "") + ("rollback, " if self.rollback else "") + contents + ")"

    def update(self, state, scope, path, to, init, arrayErrCode, mapErrCode, fcnName, pos):
        result = None

        head, tail = path[0], path[1:]

        if self.shared:
            self.locklock.acquire()
            if head in self.locks:
                self.locks[head].acquire()
            else:
                self.locks[head] = threading.Lock()
                self.locks[head].acquire()
            self.locklock.release()

            if head not in self.value:
                self.value[head] = init
            self.value[head] = update(state, scope, self.value[head], tail, to, arrayErrCode, mapErrCode, fcnName, pos)

            result = self.value[head]
            self.locks[head].release()

        else:
            if head not in self.value:
                self.value[head] = init
            self.value[head] = update(state, scope, self.value[head], tail, to, arrayErrCode, mapErrCode, fcnName, pos)
            result = self.value[head]

        return result

    def maybeSaveBackup(self):
        if self.rollback:
            self.oldvalue = dict(self.value)

    def maybeRestoreBackup(self):
        if self.rollback:
            self.value = self.oldvalue

def labeledFcn(fcn, paramNames):
    """Wraps a function with its parameter names (in-place).

    :type fcn: callable Python object
    :param fcn: function to wrap
    :type paramNames: list of strings
    :param paramNames: parameters to attach to the function
    :rtype: callable Python object
    :return: the original function, modified in-place by adding ``paramNames`` as an attribute
    """

    fcn.paramNames = paramNames
    return fcn

def get(obj, path, arrayErrCode, mapErrCode, fcnName, pos):
    """Apply an "attr", "cell", or "pool" extraction path to an object.

    :type obj: any object
    :param obj: the object to extract an item from
    :type path: list of integers and strings
    :param path: attribute labels from outermost to innermost
    :type arrayErrCode: integer
    :param arrayErrCode: error code to raise if an array index is not found
    :type mapErrCode: integer
    :param mapErrCode: error code to raise if a map key is not found
    :type fcnName: string
    :param fcnName: function name for error reporting
    :type pos: string or ``None``
    :param pos: position from locator marks for error reporting
    :rtype: an object
    :return: the extracted object
    """

    while len(path) > 0:
        head, tail = path[0], path[1:]
        try:
            obj = obj[head]
        except (KeyError, IndexError):
            if isinstance(obj, (list, tuple)):
                raise PFARuntimeException("array index not found", arrayErrCode, fcnName, pos)
            else:
                raise PFARuntimeException("map key not found", mapErrCode, fcnName, pos)
        path = tail

    return obj

def update(state, scope, obj, path, to, arrayErrCode, mapErrCode, fcnName, pos):
    """Return the updated state of a cell or pool at runtime (not in-place).

    :type state: titus.genpy.ExecutionState
    :param state: runtime state object
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type obj: an object
    :param obj: cell or pool data that should be replaced
    :type path: list of integers and strings
    :param path: extraction path
    :type to: an object, possibly callable
    :param to: replacement object; if callable, the function is called to perform the update
    :type arrayErrCode: integer
    :param arrayErrCode: error code to raise if an array index is not found
    :type mapErrCode: integer
    :param mapErrCode: error code to raise if a map key is not found
    :type fcnName: string
    :param fcnName: function name for error reporting
    :type pos: string or ``None``
    :param pos: position from locator marks for error reporting
    :rtype: an object
    :return: an updated version of the object, for the sake of replacement
    """

    if len(path) > 0:
        head, tail = path[0], path[1:]

        if isinstance(obj, dict):
            if len(tail) > 0 and head not in obj:
                raise PFARuntimeException("map key not found", mapErrCode, fcnName, pos)
            out = {}
            for k, v in obj.items():
                if k == head:
                    out[k] = update(state, scope, v, tail, to, arrayErrCode, mapErrCode, fcnName, pos)
                else:
                    out[k] = v
            return out

        elif isinstance(obj, (list, tuple)):
            if (len(tail) > 0 and head >= len(obj)) or head < 0:
                raise PFARuntimeException("array index not found", arrayErrCode, fcnName, pos)
            out = []
            for i, x in enumerate(obj):
                if i == head:
                    out.append(update(state, scope, x, tail, to, arrayErrCode, mapErrCode, fcnName, pos))
                else:
                    out.append(x)
            return out

        else:
            raise Exception

    elif callable(to):
        callScope = DynamicScope(scope)
        callScope.let({to.paramNames[0]: obj})
        return to(state, callScope)

    else:
        return to
        
def do(*exprs):
    """Helper function for chaining expressions.

    The expressions have already been evaluated when this function is called, so this function just returns the last one.

    If the list of expressions is empty, it returns ``None``.
    """

    # You've already done them; just return the right value.
    if len(exprs) > 0:
        return exprs[-1]
    else:
        return None

def ifThen(state, scope, predicate, thenClause):
    """Helper function for constructing an if-then branch as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type predicate: callable
    :param predicate: function that returns ``True`` or ``False``
    :type thenClause: callable
    :param thenClause: function that is called if ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    if predicate(state, DynamicScope(scope)):
        thenClause(state, DynamicScope(scope))
    return None

def ifThenElse(state, scope, predicate, thenClause, elseClause):
    """Helper function for constructing an if-then-else branch as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type predicate: callable
    :param predicate: function that returns ``True`` or ``False``
    :type thenClause: callable
    :param thenClause: function that is called if ``predicate`` returns ``True``
    :type elseClause: callable
    :param elseClause: function that is called if ``predicate`` returns ``False``
    :rtype: return type of ``thenClause`` or ``elseClause``
    :return: if ``predicate`` returns ``True``, the result of ``thenClause``, else the result of ``elseClause``
    """

    if predicate(state, DynamicScope(scope)):
        return thenClause(state, DynamicScope(scope))
    else:
        return elseClause(state, DynamicScope(scope))

def cond(state, scope, ifThens):
    """Helper function for constructing if-elif-elif-...-elif as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type ifThens: list of (callable, callable) pairs
    :param ifThens: list of ``(predicate, thenClause)`` pairs
    :rtype: ``None``
    :return: nothing
    """

    for predicate, thenClause in ifThens:
        if predicate(state, DynamicScope(scope)):
            thenClause(state, DynamicScope(scope))
            break
    return None

def condElse(state, scope, ifThens, elseClause):
    """Helper function for constructing if-elif-elif-...-elif as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type ifThens: list of (callable, callable) pairs
    :param ifThens: list of ``(predicate, thenClause)`` pairs
    :type elseClause: callable
    :param elseClause: function that is called if ``predicate`` returns ``False``
    :rtype: return type of any ``thenClause`` or the ``elseClause``
    :return: if any ``predicate`` returns ``True``, the result of the corresponding ``thenClause``, else the result of ``elseClause``
    """

    for predicate, thenClause in ifThens:
        if predicate(state, DynamicScope(scope)):
            return thenClause(state, DynamicScope(scope))
    return elseClause(state, DynamicScope(scope))
    
def doWhile(state, scope, predicate, loopBody):
    """Helper function for constructing pretest loops as an expression.

    Calls ``state.checkTime()`` on every iteration.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type predicate: callable
    :param predicate: function that returns ``True`` or ``False``
    :type loopBody: callable
    :param loopBody: function that is called while ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    bodyScope = DynamicScope(scope)
    predScope = DynamicScope(bodyScope)
    while predicate(state, predScope):
        state.checkTime()
        loopBody(state, bodyScope)
    return None
    
def doUntil(state, scope, predicate, loopBody):
    """Helper function for constructing posttest loops as an expression.

    Calls ``state.checkTime()`` on every iteration.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type predicate: callable
    :param predicate: function that returns ``True`` or ``False``
    :type loopBody: callable
    :param loopBody: function that is called until ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    bodyScope = DynamicScope(scope)
    predScope = DynamicScope(bodyScope)
    while True:
        state.checkTime()
        loopBody(state, bodyScope)
        if predicate(state, predScope):
            break
    return None

def doFor(state, scope, initLet, predicate, stepSet, loopBody):
    """Helper function for constructing for loops as an expression.

    Calls ``state.checkTime()`` on every iteration.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type initLet: callable
    :param initLet: initialization of for loop variables
    :type predicate: callable
    :param predicate: function that returns ``True`` or ``False``
    :type stepSet: callable
    :param stepSet: updating of for loop variables
    :type loopBody: callable
    :param loopBody: function that is called while ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    loopScope = DynamicScope(scope)
    predScope = DynamicScope(loopScope)
    bodyScope = DynamicScope(loopScope)
    initLet(state, loopScope)
    while predicate(state, predScope):
        state.checkTime()
        loopBody(state, bodyScope)
        stepSet(state, loopScope)
    return None

def doForeach(state, scope, name, array, loopBody):
    """Helper function for constructing foreach loops as an expression.

    Calls ``state.checkTime()`` on every iteration.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type name: string
    :param name: new variable for each array item
    :type name: Python iterable
    :param name: array to loop over
    :type loopBody: callable
    :param loopBody: function that is called while ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    loopScope = DynamicScope(scope)
    bodyScope = DynamicScope(loopScope)
    for item in array:
        state.checkTime()
        loopScope.let({name: item})
        loopBody(state, bodyScope)
    return None

def doForkeyval(state, scope, forkey, forval, mapping, loopBody):
    """Helper function for constructing for key,value loops as an expression.

    Calls ``state.checkTime()`` on every iteration.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type forkey: string
    :param forkey: new variable for each item key
    :type forval: string
    :param forval: new variable for each item value
    :type name: Python dict
    :param name: map of key-value pairs to loop over
    :type loopBody: callable
    :param loopBody: function that is called while ``predicate`` returns ``True``
    :rtype: ``None``
    :return: nothing
    """

    loopScope = DynamicScope(scope)
    bodyScope = DynamicScope(loopScope)
    for key, val in mapping.items():
        state.checkTime()
        loopScope.let({forkey: key, forval: val})
        loopBody(state, bodyScope)
    return None
        
def cast(state, scope, expr, fromType, cases, partial, parser):
    """Helper function for type-safe casting as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type expr: evaluated expression
    :param expr: object to cast
    :type fromType: string
    :param fromType: JSON-serialized Avro type
    :type cases: list of (string, string, callable) triples
    :param cases: list of (new variable for one case, JSON-serialized subtype, function to call if type matches) triples
    :type partial: boolean
    :param partial: if ``True``, allow the set of cases to incompletely cover the ``fromType``
    :type parser: titus.datatype.ForwardDeclarationParser
    :param parser: used to interpret ``fromType``
    :rtype: ``None`` or result of one of the ``cases`` callable
    :return: if ``partial``, returns ``None``, else returns the result of the matching case
    """

    fromType = parser.getAvroType(fromType)

    for name, toType, clause in cases:
        toType = parser.getAvroType(toType)

        if isinstance(fromType, titus.datatype.AvroUnion) and isinstance(expr, dict) and len(expr) == 1:
            tag, = expr.keys()
            value, = expr.values()

            if not ((tag == toType.name) or \
                    (tag == "int" and toType.name in ("long", "float", "double")) or \
                    (tag == "long" and toType.name in ("float", "double")) or \
                    (tag == "float" and toType.name == "double")):
                continue

        else:
            value = expr

        try:
            castValue = titus.datatype.jsonDecoder(toType, value)
        except (AvroException, TypeError):
            pass
        else:
            clauseScope = DynamicScope(scope)
            clauseScope.let({name: castValue})
            out = clause(state, clauseScope)

            if partial:
                return None
            else:
                return out
    return None
            
def wrapAsUnion(expr, typeName):
    """Converts a bare expression to a tagged union with a given type name.

    :type expr: any
    :param expr: PFA value
    :type typeName: string
    :param typeName: name for the type tag
    :rtype: dict with one key-value pair
    :return: tagged PFA value
    """

    if expr is None:
        return expr
    else:
        return {typeName: expr}

def untagUnions(nameExpr, nameType):
    """Converts the ``{"type": value}`` form of a union to ``value``.

    :type nameExpr: dict
    :param nameExpr: maps from new variable names to expressions
    :type nameType: string
    :param nameType: expected type as a JSON-encoded string
    :rtype: type of ``value``
    :return: untagged object
    """

    out = {}
    for name, expr in nameExpr.items():
        if isinstance(expr, dict) and len(expr) == 1:
            tag, = expr.keys()
            value, = expr.values()

            expectedTag = json.loads(nameType[name])
            if isinstance(expectedTag, dict):
                if expectedTag["type"] in ("record", "enum", "fixed"):
                    if "namespace" in expectedTag and expectedTag["namespace"].strip() != "":
                        expectedTag = expectedTag["namespace"] + "." + expectedTag["name"]
                    else:
                        expectedTag = expectedTag["name"]
                else:
                    expectedTag = expectedTag["type"]

            if tag == expectedTag:
                out[name] = value
            else:
                out[name] = expr
        else:
            out[name] = expr

    return out

def ifNotNull(state, scope, nameExpr, nameType, thenClause):
    """Helper function for ifnotnull as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type nameExpr: dict
    :param nameExpr: maps from new variable names to expressions
    :type nameType: string
    :param nameType: expected type as a JSON-encoded string
    :type thenClause: callable
    :param thenClause: function that is called if all expressions are not ``None``
    :rtype: ``None``
    :return: nothing
    """

    if all(x is not None for x in nameExpr.values()):
        thenScope = DynamicScope(scope)
        thenScope.let(untagUnions(nameExpr, nameType))
        thenClause(state, thenScope)

def ifNotNullElse(state, scope, nameExpr, nameType, thenClause, elseClause):
    """Helper function for ifnotnull as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type nameExpr: dict
    :param nameExpr: maps from new variable names to expressions
    :type nameType: string
    :param nameType: expected type as a JSON-encoded string
    :type thenClause: callable
    :param thenClause: function that is called if all expressions are not ``None``
    :type elseClause: callable
    :param elseClause: function that is called if any expressions are ``None``
    :rtype: result of ``thenClause`` or ``elseClause``
    :return: if all expressions are not ``None``, returns the result of ``thenClause``, otherwise returns the result of ``elseClause``
    """

    if all(x is not None for x in nameExpr.values()):
        thenScope = DynamicScope(scope)
        thenScope.let(untagUnions(nameExpr, nameType))
        return thenClause(state, thenScope)
    else:
        return elseClause(state, scope)

def pack(state, scope, exprsDeclareRes, pos):
    """Helper function for pack as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type exprsDeclareRes: list of (object, (object, string, int)) structures
    :param exprsDeclareRes: list of (value to pack, (?, format, length)) structures
    :type pos: string or ``None``
    :param pos: position from locator marks for error reporting
    :rtype: string
    :return: packed byte array
    """

    out = []
    for value, (v, f, l) in exprsDeclareRes:
        if f == "raw":
            if l is not None and len(value) != l:
                raise PFARuntimeException("raw bytes does not have specified size", 3000, "pack", pos)
            out.append(value)

        elif f == "tonull":
            out.append(value)
            out.append(chr(0))

        elif f == "prefixed":
            if len(value) > 255:
                raise PFARuntimeException("length prefixed bytes is larger than 255 bytes", 3001, "pack", pos)
            out.append(chr(len(value)))
            out.append(value)

        elif f == "x":
            out.append(chr(0))

        else:
            out.append(struct.pack(f, value))

    return "".join(out)

class MisalignedPacking(Exception):
    """Exception to raise if the packed length doesn't fit the format."""
    pass

def unpackOne(bytes, scope, s, f, l):
    """Helper function for unpack."""

    if f == "raw":
        this, that = bytes[:l], bytes[l:]
        if len(this) != l:
            raise MisalignedPacking()
        scope.let({s: this})

    elif f == "tonull":
        try:
            nullbyte = bytes.index(chr(0))
        except ValueError:
            raise MisalignedPacking()
        else:
            this = bytes[:nullbyte]
            that = bytes[(nullbyte + 1):]
            scope.let({s: this})

    elif f == "prefixed":
        if len(bytes) < 1:
            raise MisalignedPacking()
        length = ord(bytes[0])
        this = bytes[1:(length + 1)]
        that = bytes[(length + 1):]
        if len(this) != length:
            raise MisalignedPacking()
        scope.let({s: this})

    elif f == "x":
        this, that = bytes[:l], bytes[l:]
        if len(this) != l:
            raise MisalignedPacking()
        struct.unpack(f, this)
        scope.let({s: None})
        
    else:
        this, that = bytes[:l], bytes[l:]
        if len(this) != l:
            raise MisalignedPacking()
        value, = struct.unpack(f, this)
        scope.let({s: value})

    return that

def unpack(state, scope, bytes, format, thenClause):
    """Helper function for unpack as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type bytes: string
    :param bytes: byte array to unpack
    :type format: list of (variable name, format, length) triples
    :type thenClause: callable
    :param thenClause: function that is called if there was no titus.genpy.MisalignedPacking exception
    :rtype: ``None``
    :return: nothing
    """

    thenScope = DynamicScope(scope)
    try:
        for s, f, l in format:
            bytes = unpackOne(bytes, thenScope, s, f, l)
    except MisalignedPacking:
        pass
    else:
        if len(bytes) == 0:
            thenClause(state, thenScope)

def unpackElse(state, scope, bytes, format, thenClause, elseClause):
    """Helper function for unpack with an else clause as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type bytes: string
    :param bytes: byte array to unpack
    :type format: list of (variable name, format, length) triples
    :type thenClause: callable
    :param thenClause: function that is called if there was no titus.genpy.MisalignedPacking exception
    :type elseClause: callable
    :param elseClause: function that is called if there was an titus.genpy.MisalignedPacking exception
    :rtype: result of ``thenClause`` or ``elseClause``
    :return: if there was no titus.genpy.MisalignedPacking exception, returns result of ``thenClause``, otherwise, returns result of ``elseClause``
    """

    thenScope = DynamicScope(scope)
    try:
        for s, f, l in format:
            bytes = unpackOne(bytes, thenScope, s, f, l)
    except MisalignedPacking:
        return elseClause(state, scope)
    else:
        if len(bytes) != 0:
            return elseClause(state, scope)
        else:
            return thenClause(state, thenScope)

def error(message, code, pos):
    """Helper function for raising an exception as an expression.

    :type message: string
    :param message: message for the titus.errors.PFAUserException
    :type code: integer or ``None``
    :param code: code number
    :type pos: integer or ``None``
    :param pos: position in PFA document, determined by locator marks (if any)
    :rtype: bottom type!
    :return: never returns; always raises a titus.errors.PFAUserException
    """
    raise PFAUserException(message, code, pos)

def tryCatch(state, scope, exprs, filter):
    """Helper function for try-catch logic as an expression.

    :type state: titus.genpy.ExecutionState
    :param state: exeuction state
    :type scope: titus.util.DynamicScope
    :param scope: dynamic scope object
    :type exprs: callable
    :param exprs: function called within a try-except guard
    :type filter: ``None`` or list of strings and integers
    :param filter: if the exception message is ``None`` or one of these strings, absorb the exception
    :rtype: ``None``
    :return: nothing or re-raises the exception
    """
    try:
        return exprs(state, scope)
    except Exception as err:
        if filter is None or err.message in filter or err.code in filter:
            return None
        else:
            raise err

def genericLog(message, namespace):
    """Generic log function for use in PFAEngine.log.

    Just prints out the message (with namespace if not ``None``).
    """

    if namespace is None:
        print(" ".join(map(json.dumps, message)))
    else:
        print(namespace + ": " + " ".join(map(json.dumps, message)))
    
class FakeEmitForExecution(titus.fcn.Fcn):
    """Placeholder so that the ``emit`` function looks like any other function to PFA."""
    def __init__(self, engine):
        self.engine = engine

def genericEmit(x):
    """Generic emit function for use in PFAEngine.emit.

    Does nothing.
    """
    pass

def checkForDeadlock(engineConfig, engine):
    """Checks a titus.pfaast.EngineConfig for the possibility of deadlock.

    If any function used as an updator eventually calls some other function that would update state, this function raises titus.errors.PFAInitializationException.
    """

    class WithFcnRef(object):
        def isDefinedAt(self, ast):
            return isinstance(ast, (CellTo, PoolTo)) and isinstance(ast.to, (FcnRef, FcnRefFill))
        def __call__(self, slotTo):
            if engine.hasSideEffects(slotTo.to.name):
                raise PFAInitializationException("{0} references function \"{1}\", which has side-effects".format(slotTo.desc, slotTo.to.name))
    engineConfig.collect(WithFcnRef())

    class CellToOrPoolTo(object):
        def isDefinedAt(self, ast):
            return isinstance(ast, (CellTo, PoolTo))
        def __call__(self, slotTo):
            raise PFAInitializationException("inline function in cell-to or pool-to invokes a " + slotTo.desc)

    class SideEffectFunction(object):
        def isDefinedAt(self, ast):
            return isinstance(ast, Call) and engine.hasSideEffects(ast.name)
        def __call__(self, call):
            raise PFAInitializationException("inline function in cell-to or pool-to invokes function \"{0}\", which has side-effects".format(call.name))

    class WithFcnDef(object):
        def isDefinedAt(self, ast):
            return isinstance(ast, (CellTo, PoolTo)) and isinstance(ast.to, FcnDef)
        def __call__(self, slotTo):
            for x in slotTo.to.body:
                x.collect(CellToOrPoolTo())
                x.collect(SideEffectFunction())
    engineConfig.collect(WithFcnDef())

class PFAEngine(object):
    """Base class for a Titus scoring engine.

    Create instances using one of PFAEngine's staticmethods, then call ``begin`` once, ``action`` once for each datum in the data stream, and ``end`` once (if the stream ever ends). The rest of the functions are for

     - examining the scoring engine (``config``, call graph),
     - handling log output or emit output with callbacks, and
     - taking snapshots of the scoring engine's current state.

    **Examples:**

    Load a PFA file as a scoring engine. Note the ``,`` to extract the single scoring engine from the list this function returns. ::

        import json
        from titus.genpy import PFAEngine
        engine, = PFAEngine.fromJson(json.load(open("myModel.pfa")))

    Assuming (and verifying) that ``method`` is map, run it over an Avro data stream. ::

        assert(engine.config.method == "map")

        inputDataStream = engine.avroInputIterator(open("inputData.avro"))
        outputDataStream = engine.avroOutputDataFileWriter("outputData.avro")

        engine.begin()
        for datum in inputDataStream:
            outputDataStream.append(engine.action(datum))
        engine.end()
        outputDataStream.close()

    Handle the case of ``method`` = emit engines (map and fold are the same). ::

        if engine.config.method == "emit":
            def emit(x):
                outputDataStream.append(x)
            engine.emit = emit
            engine.begin()
            for datum in inputDataStream:
                engine.action(datum)
            engine.end()

        else:
            engine.begin()
            for datum in inputDataStream:
                outputDataStream.append(engine.action(datum))
            engine.end()

    Take a snapshot of a changing model and write it as a new PFA file. ::

        open("snapshot.pfa").write(engine.snapshot().toJson(lineNumbers=False))

    **Data format:**

    Data passed to ``action`` or accepted from ``action`` has to satisfy a particular form. That form is:

     - **null:** Python ``None``
     - **boolean:** Python ``True`` or ``False``
     - **int:** any Python ``int`` or ``long`` (no Numpy numbers, for instance)
     - **long:** any Python ``int`` or ``long``
     - **float:** any Python ``int``, ``long``, or ``float``
     - **double:** any Python ``int``, ``long``, or ``float``
     - **string:** any Python string or ``unicode``
     - **bytes:** any Python string
     - **array(X):** any Python ``list`` or ``tuple`` of **X**
     - **map(X):** any Python dict of **X**
     - **enum:** Python string or ``unicode`` of one of the symbols in this enumeration
     - **fixed:** Python string with length specified by this fixed-length type
     - **record:** Python dict with key-value pairs for all fields required by this record
     - **union:** if **null**, a Python ``None``; otherwise, a dict with one key-value pair representing the type and value. For example, ``None``, ``{"int": 12}``, ``{"double": 12}``, ``{"fully.qualified.record": {"field1": 1, "field2": 2}}``, ``{"array": [1, 2, 3]}``, etc.

    None of the types above are compiled (since this is Python), so anything can be directly created by the user.

    Although all of these types are immutable in PFA, list and dict are *mutable* in Python, but if you modify them, the behavior of the PFA engine is undefined and likely to be wrong. Do not change these objects in place!
    """

    @staticmethod
    def fromAst(engineConfig, options=None, version=None, sharedState=None, multiplicity=1, style="pure", debug=False):
        """Create a collection of instances of this scoring engine from a PFA abstract syntax tree (``titus.pfaast.EngineConfig``).
        
        :type engineConfig: titus.pfaast.EngineConfig
        :param engineConfig: a parsed, interpreted PFA document, i.e. produced by ``titus.reader.jsonToAst``
        :type options: dict of Pythonized JSON
        :param options: options that override those found in the PFA document
        :type version: string
        :param version: PFA version number as a "major.minor.release" string
        :type sharedState: titus.genpy.SharedState
        :param sharedState: external state for shared cells and pools to initialize from and modify; pass ``None`` to limit sharing to instances of a single PFA file
        :type multiplicity: positive integer
        :param multiplicity: number of instances to return (default is 1; a single-item collection)
        :type style: string
        :param style: style of scoring engine; only one currently supported: "pure" for pure-Python
        :type debug: bool
        :param debug: if ``True``, print the Python code generated by this PFA document before evaluating
        :rtype: PFAEngine
        :return: a list of scoring engine instances
        """

        functionTable = titus.pfaast.FunctionTable.blank()

        engineOptions = titus.options.EngineOptions(engineConfig.options, options)
        if version is None:
            version = titus.version.defaultPFAVersion
        pfaVersion = titus.signature.PFAVersion.fromString(version)

        context, code = engineConfig.walk(GeneratePython.makeTask(style), titus.pfaast.SymbolTable.blank(), functionTable, engineOptions, pfaVersion)
        if debug:
            print(code)

        sandbox = {# Scoring engine architecture
                   "PFAEngine": PFAEngine,
                   "ExecutionState": ExecutionState,
                   "DynamicScope": DynamicScope,
                   # Python statement --> expression wrappers
                   "labeledFcn": labeledFcn,
                   "call": titus.util.callfcn,
                   "get": get,
                   "update": update,
                   "do": do,
                   "ifThen": ifThen,
                   "ifThenElse": ifThenElse,
                   "cond": cond,
                   "condElse": condElse,
                   "doWhile": doWhile,
                   "doUntil": doUntil,
                   "doFor": doFor,
                   "doForeach": doForeach,
                   "doForkeyval": doForkeyval,
                   "cast": cast,
                   "wrapAsUnion": wrapAsUnion,
                   "ifNotNull": ifNotNull,
                   "ifNotNullElse": ifNotNullElse,
                   "pack": pack,
                   "unpack": unpack,
                   "unpackElse": unpackElse,
                   "error": error,
                   "tryCatch": tryCatch,
                   # Titus dependencies
                   "checkData": titus.datatype.checkData,
                   # Python libraries
                   "math": math,
                   }

        exec(code, sandbox)
        cls = [x for x in sandbox.values() if getattr(x, "__bases__", None) == (PFAEngine,)][0]
        cls.parser = context.parser

        if sharedState is None:
            sharedState = SharedState()

        for cellName, cellConfig in engineConfig.cells.items():
            if cellConfig.shared and cellName not in sharedState.cells:
                value = titus.datatype.jsonDecoder(cellConfig.avroType, cellConfig.initJsonNode)
                sharedState.cells[cellName] = Cell(value, cellConfig.shared, cellConfig.rollback, cellConfig.source)

        for poolName, poolConfig in engineConfig.pools.items():
            if poolConfig.shared and poolName not in sharedState.pools:
                value = titus.datatype.jsonDecoder(titus.datatype.AvroMap(poolConfig.avroType), poolConfig.initJsonNode)
                sharedState.pools[poolName] = Pool(value, poolConfig.shared, poolConfig.rollback, poolConfig.source)

        out = []
        for index in xrange(multiplicity):
            cells = dict(sharedState.cells)
            pools = dict(sharedState.pools)

            for cellName, cellConfig in engineConfig.cells.items():
                if not cellConfig.shared:
                    value = titus.datatype.jsonDecoder(cellConfig.avroType, cellConfig.initJsonNode)
                    cells[cellName] = Cell(value, cellConfig.shared, cellConfig.rollback, cellConfig.source)

            for poolName, poolConfig in engineConfig.pools.items():
                if not poolConfig.shared:
                    value = titus.datatype.jsonDecoder(titus.datatype.AvroMap(poolConfig.avroType), poolConfig.initJsonNode)
                    pools[poolName] = Pool(value, poolConfig.shared, poolConfig.rollback, poolConfig.source)

            if engineConfig.method == Method.FOLD:
                zero = titus.datatype.jsonDecoder(engineConfig.output, json.loads(engineConfig.zero))
            else:
                zero = None

            if engineConfig.randseed is None:
                rand = random.Random()
            else:
                rand = random.Random(engineConfig.randseed)
                for skip in xrange(index):
                    rand = random.Random(rand.randint(0, 2**31 - 1))

            engine = cls(cells, pools, engineConfig, engineOptions, genericLog, genericEmit, zero, index, rand)

            f = dict(functionTable.functions)
            if engineConfig.method == Method.EMIT:
                f["emit"] = FakeEmitForExecution(engine)
            engine.f = f
            engine.config = engineConfig

            checkForDeadlock(engineConfig, engine)
            engine.initialize()

            out.append(engine)

        return out

    @staticmethod
    def fromJson(src, options=None, version=None, sharedState=None, multiplicity=1, style="pure", debug=False):
        """Create a collection of instances of this scoring engine from a JSON-formatted PFA file.
        
        :type src: JSON string or Pythonized JSON
        :param src: a PFA document in JSON-serialized form; may be a literal JSON string or the kind of Python structure that ``json.loads`` creates from a JSON string
        :type options: dict of Pythonized JSON
        :param options: options that override those found in the PFA document
        :type version: string
        :param version: PFA version number as a "major.minor.release" string
        :type sharedState: titus.genpy.SharedState
        :param sharedState: external state for shared cells and pools to initialize from and modify; pass ``None`` to limit sharing to instances of a single PFA file
        :type multiplicity: positive integer
        :param multiplicity: number of instances to return (default is 1; a single-item collection)
        :type style: string
        :param style: style of scoring engine; only one currently supported: "pure" for pure-Python
        :type debug: bool
        :param debug: if ``True``, print the Python code generated by this PFA document before evaluating
        :rtype: PFAEngine
        :return: a list of scoring engine instances
        """
        return PFAEngine.fromAst(titus.reader.jsonToAst(src), options, version, sharedState, multiplicity, style, debug)

    @staticmethod
    def fromYaml(src, options=None, version=None, sharedState=None, multiplicity=1, style="pure", debug=False):
        """Create a collection of instances of this scoring engine from a YAML-formatted PFA file.
        
        :type src: string
        :param src: a PFA document in YAML-serialized form; must be a string
        :type options: dict of Pythonized JSON
        :param options: options that override those found in the PFA document
        :type version: string
        :param version: PFA version number as a "major.minor.release" string
        :type sharedState: titus.genpy.SharedState
        :param sharedState: external state for shared cells and pools to initialize from and modify; pass ``None`` to limit sharing to instances of a single PFA file
        :type multiplicity: positive integer
        :param multiplicity: number of instances to return (default is 1; a single-item collection)
        :type style: string
        :param style: style of scoring engine; only one currently supported: "pure" for pure-Python
        :type debug: bool
        :param debug: if ``True``, print the Python code generated by this PFA document before evaluating
        :rtype: PFAEngine
        :return: a list of scoring engine instances
        """
        return PFAEngine.fromAst(titus.reader.yamlToAst(src), options, version, sharedState, multiplicity, style, debug)

    @staticmethod
    def fromPmml(src, pmmlOptions=None, pfaOptions=None, version=None, sharedState=None, multiplicity=1, style="pure", debug=False):
        """Translates some types of PMML documents into PFA and creates a collection of scoring engine instances.
        
        :type src: string
        :param src: a PMML document in XML-serialized form; must be a string
        :type pmmlOptions: dict
        :param pmmlOptions: directives for interpreting the PMML document
        :type pfaOptions: dict
        :param pfaOptions: options that override those found in the PFA document
        :type version: string
        :param version: PFA version number as a "major.minor.release" string
        :type sharedState: titus.genpy.SharedState
        :param sharedState: external state for shared cells and pools to initialize from and modify; pass ``None`` to limit sharing to instances of a single PFA file
        :type multiplicity: positive integer
        :param multiplicity: number of instances to return (default is 1; a single-item collection)
        :type style: string
        :param style: style of scoring engine; only one currently supported: "pure" for pure-Python
        :type debug: bool
        :param debug: if ``True``, print the Python code generated by this PFA document before evaluating
        :rtype: PFAEngine
        :return: a list of scoring engine instances
        """
        return PFAEngine.fromAst(pmmlToAst(src, pmmlOptions), pfaOptions, version, sharedState, multiplicity, style, debug)

    def snapshot(self):
        """take a snapshot of the entire scoring engine (all cells and pools) and represent it as an abstract syntax tree that can be used to make new scoring engines.

        Note that you can call ``toJson`` on the ``EngineConfig`` to get a string that can be written to a PFA file.
        """

        newCells = dict((k, AstCell(self.config.cells[k].avroPlaceholder, json.dumps(v.value), v.shared, v.rollback, v.source)) for k, v in self.cells.items())
        newPools = dict((k, AstPool(self.config.pools[k].avroPlaceholder, dict((kk, json.dumps(vv)) for kk, vv in v.value.items()), v.shared, v.rollback, v.source)) for k, v in self.pools.items())

        return EngineConfig(
            self.config.name,
            self.config.method,
            self.config.inputPlaceholder,
            self.config.outputPlaceholder,
            self.config.begin,
            self.config.action,
            self.config.end,
            self.config.fcns,
            self.config.zero,
            self.config.merge,
            newCells,
            newPools,
            self.config.randseed,
            self.config.doc,
            self.config.version,
            self.config.metadata,
            self.config.options)

    def calledBy(self, fcnName, exclude=None):
        """Determine which functions are called by ``fcnName`` by traversing the ``callGraph`` backward.

        :type fcnName: string
        :param fcnName: name of function to look up
        :type exclude: set of string
        :param exclude: set of functions to exclude
        :rtype: set of string
        :return: set of functions that call ``fcnName``
        """

        if exclude is None:
            exclude = set()
        if fcnName in exclude:
            return set()
        else:
            if fcnName in self.callGraph:
                newExclude = exclude.union(set([fcnName]))
                nextLevel = set([])
                for f in self.callGraph[fcnName]:
                    nextLevel = nextLevel.union(self.calledBy(f, newExclude))
                return self.callGraph[fcnName].union(nextLevel)
            else:
                return set()

    def callDepth(self, fcnName, exclude=None, startingDepth=0):
        """Determine call depth of a function by traversing the ``callGraph``.

        :type fcnName: string
        :param fcnName: name of function to look up
        :type exclude: set of string
        :param exclude: set of functions to exclude
        :type startingDepth: integer
        :param startingDepth: used by recursion to count
        :rtype: integer or floating-point inf
        :return: number representing call depth, with positive infinity (which is a ``float``) as a possible result
        """

        if exclude is None:
            exclude = set()
        if fcnName in exclude:
            return float("inf")
        else:
            if fcnName in self.callGraph:
                newExclude = exclude.union(set([fcnName]))
                deepest = startingDepth
                for f in self.callGraph[fcnName]:
                    fdepth = self.callDepth(f, newExclude, startingDepth + 1)
                    if fdepth > deepest:
                        deepest = fdepth
                return deepest
            else:
                return startingDepth

    def isRecursive(self, fcnName):
        """Determine if a function is directly recursive.

        :type fcnName: string
        :param fcnName: name of function to look up
        :rtype: bool
        :return: ``True`` if the function directly calls itself, ``False`` otherwise
        """
        return fcnName in self.calledBy(fcnName)

    def hasRecursive(self, fcnName):
        """Determine if the call depth of a funciton is infinite.

        :type fcnName: string
        :param fcnName: name of function to look up
        :rtype: bool
        :return: ``True`` if the function can eventually call itself through a function that it calls, ``False`` otherwise
        """
        return self.callDepth(fcnName) == float("inf")

    def hasSideEffects(self, fcnName):
        """Determine if a function modifies the scoring engine's persistent state.

        :type fcnName: string
        :param fcnName: name of function to look up
        :rtype: bool
        :return: ``True`` if the function can eventually call ``(cell-to)`` or ``(pool-to)`` on any cell or pool.
        """
        reach = self.calledBy(fcnName)
        return CellTo.desc in reach or PoolTo.desc in reach or PoolDel.desc in reach

    def avroInputIterator(self, inputStream, interpreter="avro"):
        """Create a generator over Avro-serialized input data.

        :type inputStream: open filehandle
        :param inputStream: serialized data
        :rtype: ``avro.datafile.DataFileReader``
        :return: generator of objects suitable for the ``action`` method
        """

        if interpreter == "avro":
            return DataFileReader(inputStream, DatumReader())
        elif interpreter == "fastavro":
            import fastavro
            return fastavro.reader(inputStream)
        elif interpreter == "correct-fastavro":
            return FastAvroCorrector(inputStream, self.config.input)
        else:
            raise ValueError("interpreter must be one of \"avro\", \"fastavro\", and \"correct-fastavro\" (which corrects fastavro's handling of Unicode strings)")

    def avroOutputDataFileWriter(self, fileName):
        """Create an output stream for Avro-serializing scoring engine output.

        Return values from the ``action`` method (or outputs captured by an ``emit`` callback) are suitable for writing to this stream.

        :type fileName: string
        :param fileName: name of the file that will be overwritten by Avro bytes
        :rtype: ``avro.datafile.DataFileWriter``
        :return: an output stream with an ``append`` method for appending output data objects
        """

        return DataFileWriter(open(fileName, "w"), DatumWriter(), self.config.output.schema)

class FastAvroCorrector(object):
    """The fastavro library reads Avro strings as non-Unicode and doesn't tag unions. This wrapper class corrects it."""

    def __init__(self, inputStream, avroType):
        import fastavro
        self.reader = fastavro.reader(inputStream)
        self.avroType = avroType

    def __iter__(self):
        return self

    def next(self):
        return self.correctFastAvro(self.reader.next(), self.avroType)

    def correctFastAvro(self, x, avroType):
        if isinstance(avroType, (titus.datatype.AvroString, titus.datatype.AvroEnum)) and isinstance(x, str):
            return x.decode("utf-8", "replace")

        elif isinstance(avroType, (titus.datatype.AvroBytes, titus.datatype.AvroFixed)) and isinstance(x, unicode):
            return x.encode("utf-8", "replace")

        elif isinstance(avroType, titus.datatype.AvroArray):
            itemType = avroType.items
            for i in xrange(len(x)):
                x[i] = self.correctFastAvro(x[i], itemType)

        elif isinstance(avroType, titus.datatype.AvroMap):
            valueType = avroType.values
            out = {}
            for key, value in x.items():
                out[key.decode("utf-8", "replace")] = self.correctFastAvro(value, valueType)
            return out

        elif isinstance(avroType, titus.datatype.AvroRecord):
            fields = avroType.fieldsDict
            if fields.keys() != x.keys():
                raise KeyError("datum {0} does not match record schema\n{1}".format(x, avroType))
            for key in fields:
                x[key] = self.correctFastAvro(x[key], fields[key].avroType)
            return x

        elif isinstance(avroType, titus.datatype.AvroUnion):
            for tpe in avroType.types:
                if (isinstance(tpe, (titus.datatype.AvroString, titus.datatype.AvroEnum)) and isinstance(x, str)) or \
                   (isinstance(tpe, (titus.datatype.AvroBytes, titus.datatype.AvroFixed)) and isinstance(x, unicode)) or \
                   (isinstance(tpe, titus.datatype.AvroArray) and isinstance(x, (list, tuple))) or \
                   (isinstance(tpe, titus.datatype.AvroMap) and isinstance(x, dict)):
                    return self.correctFastAvro(x, tpe)

                elif (isinstance(tpe, (titus.datatype.AvroString, titus.datatype.AvroEnum)) and isinstance(x, unicode)) or \
                     (isinstance(tpe, (titus.datatype.AvroBytes, titus.datatype.AvroFixed)) and isinstance(x, str)):
                    return x

                elif isinstance(tpe, titus.datatype.AvroRecord) and isinstance(x, dict):
                    try:
                        return self.correctFastAvro(x, tpe)
                    except KeyError:
                        pass   # this is the wrong record type; one of the later record types applies

        else:
            return x
