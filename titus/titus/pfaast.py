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

import base64
import json
import re
from collections import OrderedDict

import titus.lib1.array
import titus.lib1.bytes
import titus.lib1.cast
import titus.lib1.core
import titus.lib1.enum
import titus.lib1.fixed
import titus.lib1.impute
import titus.lib1.la
import titus.lib1.map
import titus.lib1.metric
import titus.lib1.pfamath
import titus.lib1.parse
import titus.lib1.pfastring
import titus.lib1.pfatime
import titus.lib1.prob.dist
import titus.lib1.regex
import titus.lib1.rand
import titus.lib1.spec
import titus.lib1.stat.change
import titus.lib1.stat.sample
import titus.lib1.model.cluster
import titus.lib1.model.neighbor
import titus.lib1.model.tree
import titus.lib1.model.reg

import titus.P as P
import titus.util

from titus.errors import *
from titus.fcn import Fcn
from titus.signature import IncompatibleTypes
from titus.signature import LabelData
from titus.signature import Sig
from titus.datatype import *
import titus.options
from titus.util import avscToPretty

############################################################ utils

TYPE_ERRORS_IN_PRETTYPFA = True
def ts(avroType):
    if TYPE_ERRORS_IN_PRETTYPFA:
        pretty = avscToPretty(avroType.jsonNode(set()))
        if pretty.count("\n") > 0:
            pretty = "\n    " + pretty.replace("\n", "\n    ") + "\n"
        return pretty
    else:
        return repr(avroType)

def inferType(expr, symbols=None, cells=None, pools=None, fcns=None):
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

    context, result = expr.walk(NoTask(), symbolTable, functionTable, titus.options.EngineOptions(None, None))
    return context.retType

############################################################ symbols

def validSymbolName(test):
    return re.match("^[A-Za-z_][A-Za-z0-9_]*$", test) is not None

class SymbolTable(object):
    def __init__(self, parent, symbols, cells, pools, sealedAbove, sealedWithin):
        self.parent = parent
        self.symbols = symbols
        self.cells = cells
        self.pools = pools
        self.sealedAbove = sealedAbove
        self.sealedWithin = sealedWithin

    def getLocal(self, name):
        if name in self.symbols:
            return self.symbols[name]
        else:
            return None

    def getAbove(self, name):
        if self.parent is None:
            return None
        else:
            return self.parent.get(name)

    def get(self, name):
        out = self.getLocal(name)
        if out is None:
            return self.getAbove(name)
        else:
            return out

    def __call__(self, name):
        out = self.get(name)
        if out is None:
            raise KeyError("no symbol named \"{0}\"".format(name))
        else:
            return out

    def writable(self, name):
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
        self.symbols[name] = avroType

    def cell(self, name):
        if name in self.cells:
            return self.cells[name]
        elif self.parent is not None:
            return self.parent.cell(name)
        else:
            return None

    def pool(self, name):
        if name in self.pools:
            return self.pools[name]
        elif self.parent is not None:
            return self.parent.pool(name)
        else:
            return None
        
    def newScope(self, sealedAbove, sealedWithin):
        return SymbolTable(self, {}, {}, {}, sealedAbove, sealedWithin)

    @property
    def inThisScope(self):
        return self.symbols

    @property
    def allInScope(self):
        if self.parent is None:
            out = {}
        else:
            out = self.parent.allInScope(self)

        out.update(self.symbols)
        return out

    @staticmethod
    def blank():
        SymbolTable(None, {}, {}, {}, True, False)

############################################################ functions

def validFunctionName(test):
    return re.match("^[A-Za-z_]([A-Za-z0-9_]|\\.[A-Za-z][A-Za-z0-9_]*)*$", test) is not None

class UserFcn(Fcn):
    def __init__(self, name, sig):
        self.name = name
        self.sig = sig

    def genpy(self, paramTypes, args):
        parNames = [x.keys()[0] for x in self.sig.params]
        return "call(state, DynamicScope(None), self.f[" + repr(self.name) + "], {" + ", ".join([repr(k) + ": " + v for k, v in zip(parNames, args)]) + "})"

    @staticmethod
    def fromFcnDef(n, fcnDef):
        return UserFcn(n, Sig([{k: P.fromType(fcnDef.params[k])} for k in fcnDef.paramNames], P.fromType(fcnDef.ret)))

class EmitFcn(Fcn):
    def __init__(self, outputType):
        self.sig = Sig([{"output": P.fromType(outputType)}], P.Null())

    def genpy(self, paramTypes, args):
        return "self.f[\"emit\"].engine.emit(" + args[0] + ")"

class FunctionTable(object):
    def __init__(self, functions):
        self.functions = functions

    @staticmethod
    def blank():
        functions = {}
        functions.update(titus.lib1.array.provides)
        functions.update(titus.lib1.bytes.provides)
        functions.update(titus.lib1.cast.provides)
        functions.update(titus.lib1.core.provides)
        functions.update(titus.lib1.enum.provides)
        functions.update(titus.lib1.fixed.provides)
        functions.update(titus.lib1.impute.provides)
        functions.update(titus.lib1.la.provides)
        functions.update(titus.lib1.map.provides)
        functions.update(titus.lib1.metric.provides)
        functions.update(titus.lib1.pfamath.provides)
        functions.update(titus.lib1.parse.provides)
        functions.update(titus.lib1.prob.dist.provides)
        functions.update(titus.lib1.pfastring.provides)
        functions.update(titus.lib1.rand.provides)
        functions.update(titus.lib1.regex.provides)
        functions.update(titus.lib1.spec.provides)
        functions.update(titus.lib1.stat.change.provides)
        functions.update(titus.lib1.stat.sample.provides)
        functions.update(titus.lib1.pfatime.provides)
        functions.update(titus.lib1.model.tree.provides)
        functions.update(titus.lib1.model.cluster.provides)
        functions.update(titus.lib1.model.neighbor.provides)
        functions.update(titus.lib1.model.reg.provides)

        # TODO: functions.update(titus.lib1.other.provides)...

        return FunctionTable(functions)

############################################################ type-checking and transforming ASTs

class AstContext(object): pass
class ArgumentContext(AstContext):
    def calls(self):
        raise NotImplementedError
class ExpressionContext(ArgumentContext):
    def retType(self):
        raise NotImplementedError
class FcnContext(ArgumentContext):
    def fcnType(self):
        raise NotImplementedError
class TaskResult(object): pass

class Task(object):
    def __call__(self, astContext, engineOptions, resoledType=None):
        raise NotImplementedError

class NoTask(Task):
    class EmptyResult(TaskResult): pass
    def __call__(self, astContext, engineOptions, resolvedType=None):
        return self.EmptyResult()

def check(engineConfig):
    engineConfig.walk(NoTask(),
                      SymbolTable.blank(),
                      FunctionTable.blank(),
                      titus.options.EngineOptions(engineConfig.options, None))

def isValid(engineConfig):
    try:
        check(engineConfig)
    except (PFASyntaxException, PFASemanticException, PFAInitializationException):
        return False
    else:
        return True

############################################################ AST nodes

class Ast(object):
    def collect(self, pf):
        if pf.isDefinedAt(self):
            return [pf(self)]
        else:
            return []

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return self

    def walk(self, task, symbolTable=None, functionTable=None, engineOptions=None):
        if symbolTable is None and functionTable is None and engineOptions is None:
            self.walk(task, SymbolTable.blank(), FunctionTable.blank(), titus.options.EngineOptions(None, None))
        else:
            raise NotImplementedError

    def toJson(self, lineNumbers=True):
        return json.dumps(self.jsonNode(lineNumbers, set()))

    def jsonNode(self, lineNumbers, memo):
        raise NotImplementedError

    def startDict(self, lineNumbers):
        if lineNumbers and self.pos is not None:
            return OrderedDict({"@": self.pos})
        else:
            return OrderedDict()

class Subs(Ast):
    def __init__(self, name, lineno=None):
        self.name = name
        self.pos = "Substitution at line " + str(lineno)
        self.lineno = lineno
        self.low = lineno
        self.high = lineno
        self.context = None

    def asExpr(self, state):
        self.context = "expr"
        return self

    def asType(self, state):
        self.context = "type"
        return self

    def walk(self, task, symbolTable=None, functionTable=None, engineOptions=None):
        raise PFASyntaxException("Unresolved substitution \"{0}\"".format(self.name), self.pos)

    def toJson(self, lineNumbers=True):
        raise PFASyntaxException("Unresolved substitution \"{0}\"".format(self.name), self.pos)

    def jsonNode(self, lineNumbers, memo):
        return self

    def __repr__(self):
        return "<<" + self.name + ">>"
        
class Method(object):
    MAP = "map"
    EMIT = "emit"
    FOLD = "fold"

@titus.util.case
class EngineConfig(Ast):
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

        if len(self.action) < 1:
            raise PFASyntaxException("\"action\" must contain least one expression", self.pos)

        if method == Method.FOLD and (zero is None or merge is None):
            raise PFASyntaxException("folding engines must include \"zero\" and \"merge\" top-level fields", pos)

        if method != Method.FOLD and (zero is not None or merge is not None):
            raise PFASyntaxException("non-folding engines must not include \"zero\" and \"merge\" top-level fields", pos)

        if merge is not None and len(merge) < 1:
            raise PFASyntaxException("\"merge\" must contain least one expression", self.pos)

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
        return self.inputPlaceholder.avroType

    @property
    def output(self):
        return self.outputPlaceholder.avroType

    def collect(self, pf):
        return super(EngineConfig, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.begin) + \
               titus.util.flatten(x.collect(pf) for x in self.action) + \
               titus.util.flatten(x.collect(pf) for x in self.end) + \
               titus.util.flatten(x.collect(pf) for x in self.fcns.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.cells.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.pools.values())

    def replace(self, pf):
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

    def walk(self, task, symbolTable, functionTable, engineOptions):
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
            fcnContext, fcnResult = fcnDef.walk(task, scope, withUserFunctions, engineOptions)
            userFcnContexts.append((ufname, fcnContext))

        beginScopeWrapper = topWrapper.newScope(True, False)
        beginScopeWrapper.put("name", AvroString())
        beginScopeWrapper.put("instance", AvroInt())
        if self.version is not None:
            beginScopeWrapper.put("version", AvroInt())
        beginScopeWrapper.put("metadata", AvroMap(AvroString()))
        beginScope = beginScopeWrapper.newScope(True, False)

        beginContextResults = [x.walk(task, beginScope, withUserFunctions, engineOptions) for x in self.begin]
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

            mergeContextResults = [x.walk(task, mergeScope, withUserFunctions, engineOptions) for x in self.merge]
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

        endContextResults = [x.walk(task, endScope, withUserFunctions, engineOptions) for x in self.end]
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

        actionContextResults = [x.walk(task, actionScope, withUserFunctions, engineOptions) for x in self.action]
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

@titus.util.case
class Cell(Ast):
    def __init__(self, avroPlaceholder, init, shared, rollback, pos=None):
        if shared and rollback:
            raise PFASyntaxException("shared and rollback are mutually incompatible flags for a Cell", self.pos)

    def equals(self, other):
        if isinstance(other, Cell):
            return self.avroPlaceholder == other.avroPlaceholder and json.loads(self.init) == json.loads(other.init) and self.shared == other.shared and self.rollback == other.rollback
        else:
            return False

    def __hash__(self):
        return hash((self.avroPlaceholder, json.loads(self.init), self.shared, self.rollback))

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context()
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["init"] = json.loads(self.init)
        out["shared"] = self.shared
        out["rollback"] = self.rollback
        return out

    @titus.util.case
    class Context(AstContext):
        def __init__(self): pass

@titus.util.case
class Pool(Ast):
    def __init__(self, avroPlaceholder, init, shared, rollback, pos=None):
        if shared and rollback:
            raise PFASyntaxException("shared and rollback are mutually incompatible flags for a Pool", self.pos)

    def equals(self, other):
        if isinstance(other, Pool):
            return self.avroPlaceholder == other.avroPlaceholder and self.init.keys() == other.init.keys() and all(json.loads(self.init[k]) == json.loads(other.init[k]) for k in self.init.keys()) and self.shared == other.shared and self.rollback == other.rollback
        else:
            return False

    def __hash__(self):
        return hash((self.avroPlaceholder, dict((k, json.loads(v)) for k, v in self.init.items()), self.shared, self.rollback))

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context()
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["type"] = self.avroPlaceholder.jsonNode(memo)
        out["init"] = OrderedDict((k, json.loads(v)) for k, v in self.init.items())
        out["shared"] = self.shared
        out["rollback"] = self.rollback
        return out
            
    @titus.util.case
    class Context(AstContext):
        def __init__(self): pass

class Argument(Ast): pass
class Expression(Argument): pass
class LiteralValue(Expression): pass

class PathIndex(object): pass
@titus.util.case
class ArrayIndex(PathIndex):
    def __init__(self, i, t): pass
@titus.util.case
class MapIndex(PathIndex):
    def __init__(self, k, t): pass
@titus.util.case
class RecordIndex(PathIndex):
    def __init__(self, f, t): pass

class HasPath(object):
    def walkPath(self, avroType, task, symbolTable, functionTable, engineOptions):
        calls = set()
        scope = symbolTable.newScope(True, True)
        walkingType = avroType

        pathIndexes = []
        for indexIndex, expr in enumerate(self.path):
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions)
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
    def __init__(self, paramsPlaceholder, retPlaceholder, body, pos=None):
        if len(self.body) < 1:
            raise PFASyntaxException("function's \"do\" list must contain least one expression", self.pos)

    @property
    def paramNames(self):
        return [t.keys()[0] for t in self.paramsPlaceholder]

    @property
    def params(self):
        return dict((t.keys()[0], t.values()[0].avroType) for t in self.paramsPlaceholder)

    @property
    def ret(self):
        return self.retPlaceholder.avroType

    def collect(self, pf):
        return super(FcnDef, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return FcnDef(self.paramsPlaceholder,
                          self.retPlaceholder,
                          [x.replace(pf) for x in self.body],
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        if len(self.paramsPlaceholder) > 22:
            raise PFASemanticException("function can have at most 22 parameters", self.pos)

        scope = symbolTable.newScope(True, False)
        for name, avroType in self.params.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid parameter name".format(name), self.pos)
            scope.put(name, avroType)

        results = [x.walk(task, scope, functionTable, engineOptions) for x in self.body]

        inferredRetType = results[-1][0].retType
        if not isinstance(inferredRetType, ExceptionType) and not self.ret.accepts(inferredRetType):
            raise PFASemanticException("function's inferred return type is {0} but its declared return type is {1}".format(ts(results[-1][0].retType), ts(self.ret)), self.pos)

        context = self.Context(FcnType([self.params[k] for k in self.paramNames], self.ret), set(titus.util.flatten([x[0].calls for x in results])), self.paramNames, self.params, self.ret, scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["params"] = [{x.keys()[0]: x.values()[0].jsonNode(memo)} for x in self.paramsPlaceholder]
        out["ret"] = self.retPlaceholder.jsonNode(memo)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, paramNames, params, ret, symbols, exprs): pass

@titus.util.case
class FcnRef(Argument):
    def __init__(self, name, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        try:
            if isinstance(fcn.sig, Sig):
                params, ret = fcn.sig.params, fcn.sig.ret
                fcnType = FcnType([P.toType(p.values()[0]) for p in params], P.mustBeAvro(P.toType(ret)))
            else:
                raise IncompatibleTypes()
        except IncompatibleTypes:
            raise PFASemanticException("only one-signature functions without constraints can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)

        context = FcnRef.Context(fcnType, set([self.name]), fcn)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["fcn"] = self.name
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, fcn): pass

@titus.util.case
class FcnRefFill(Argument):
    def __init__(self, name, fill, pos=None):
        if len(self.fill) < 1:
            raise PFASyntaxException("\"fill\" must contain at least one parameter name-argument mapping", self.pos)

    def collect(self, pf):
        return super(FcnRefFill, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.fill.values())

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return FcnRefFill(self.name,
                              dict((k, v.replace(pf)) for k, v in self.fill.items()),
                              self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set([self.name])

        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        fillScope = symbolTable.newScope(True, True)
        argTypeResult = {}
        for name, arg in self.fill.items():
            argCtx, argRes = arg.walk(task, fillScope, functionTable, engineOptions)

            calls = calls.union(argCtx.calls)

            if isinstance(argCtx, FcnContext):
                argTypeResult[name] = (argCtx.fcnType, argRes)
            elif isinstance(argCtx, ExpressionContext):
                argTypeResult[name] = (argCtx.retType, argRes)
            else:
                raise Exception

        try:
            if isinstance(fcn.sig, Sig):
                params, ret = fcn.sig.params, fcn.sig.ret

                originalParamNames = [x.keys()[0] for x in params]
                fillNames = set(argTypeResult.keys())

                if not fillNames.issubset(set(originalParamNames)):
                    raise PFASemanticException("fill argument names (\"{0}\") are not a subset of function \"{1}\" parameter names (\"{2}\")".format("\", \"".join(sorted(fillNames)), self.name, "\", \"".join(originalParamNames)), self.pos)

                fcnType = FcnType([P.mustBeAvro(P.toType(p.values()[0])) for p in params if p.keys()[0] not in fillNames], P.mustBeAvro(P.toType(ret)))
            else:
                raise IncompatibleTypes()
        except IncompatibleTypes:
            raise PFASemanticException("only one-signature functions without constraints can be referenced (wrap \"{0}\" in a function definition with the desired signature)".format(self.name), self.pos)

        context = self.Context(fcnType, calls, fcn, originalParamNames, argTypeResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["fcn"] = self.name
        out["fill"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.fill.items())
        return out

    @titus.util.case
    class Context(FcnContext):
        def __init__(self, fcnType, calls, fcn, originalParamNames, argTypeResult): pass

@titus.util.case
class CallUserFcn(Expression):
    def __init__(self, name, args, pos=None): pass

    def collect(self, pf):
        return super(CallUserFcn, self).collect(pf) + \
               self.name.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.args)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CallUserFcn(self.name,
                               [x.replace(pf) for x in self.args],
                               self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        nameScope = symbolTable.newScope(True, True)
        nameContext, nameResult = self.name.walk(task, nameScope, functionTable, engineOptions)
        if isinstance(nameContext.retType, AvroEnum):
            fcnNames = nameContext.retType.symbols
        else:
            raise PFASemanticException("\"call\" name should be an enum, but is " + ts(nameContext.retType), self.pos)
        nameToNum = dict((x, i) for i, x in enumerate(fcnNames))

        scope = symbolTable.newScope(True, True)
        argResults = [x.walk(task, scope, functionTable, engineOptions) for x in self.args]

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
            sigres = fcn.sig.accepts(argTypes)
            if sigres is not None:
                paramTypes, retType = sigres
                nameToFcn[n] = fcn
                nameToParamTypes[n] = paramTypes
                nameToRetTypes[n] = retType
                retTypes.append(retType)
            else:
                raise PFASemanticException("parameters of function \"{0}\" (in enumeration type) do not accept [{1}]".format(self.name, ",".join(map(ts, argTypes))), self.pos)

        try:
            retType = LabelData.broadestType(retTypes)
        except IncompatibleTypes as err:
            raise PFASemanticException(str(err))

        context = self.Context(retType, calls, nameResult, nameToNum, nameToFcn, [x[1] for x in argResults], [x[0] for x in argResults], nameToParamTypes, nameToRetTypes)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["call"] = self.name.jsonNode(lineNumbers, memo)
        out["args"] = [x.jsonNode(lineNumbers, memo) for x in self.args]
        return out

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, name, nameToNum, nameToFcn, args, argContext, nameToParamTypes, nameToRetTypes): pass

@titus.util.case
class Call(Expression):
    def __init__(self, name, args, pos=None): pass

    def collect(self, pf):
        return super(Call, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.args)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Call(self.name,
                        [x.replace(pf) for x in self.args],
                        self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{0}\" (be sure to include \"u.\" to reference user functions)".format(self.name), self.pos)

        scope = symbolTable.newScope(True, True)
        argResults = [x.walk(task, scope, functionTable, engineOptions) for x in self.args]

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

        sigres = fcn.sig.accepts(argTypes)
        if sigres is not None:
            paramTypes, retType = sigres

            argContexts = [x[0] for x in argResults]
            argTaskResults = [x[1] for x in argResults]

            # Two-parameter task?
            # for i, a in enumerate(self.args):
            #     if isinstance(a, FcnRef):
            #         argTaskResults[i] = task(argContexts[i], engineOptions, paramTypes[i])

            context = self.Context(retType, calls, fcn, argTaskResults, argContexts, paramTypes)

        else:
            raise PFASemanticException("parameters of function \"{0}\" do not accept [{1}]".format(self.name, ",".join(map(ts, argTypes))), self.pos)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out[self.name] = [x.jsonNode(lineNumbers, memo) for x in self.args]
        return out

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, fcn, args, argContexts, paramTypes): pass

@titus.util.case
class Ref(Expression):
    def __init__(self, name, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        if symbolTable.get(self.name) is None:
            raise PFASemanticException("unknown symbol \"{0}\"".format(self.name), self.pos)
        context = self.Context(symbolTable(self.name), set(), self.name)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        return self.name

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, name): pass

@titus.util.case
class LiteralNull(LiteralValue):
    def __init__(self, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        return None

    desc = "(null)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class LiteralBoolean(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroBoolean(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        return self.value

    desc = "(boolean)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralInt(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroInt(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        return self.value

    desc = "(int)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralLong(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroLong(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["long"] = self.value
        return out

    desc = "(long)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralFloat(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroFloat(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["float"] = self.value
        return out

    desc = "(float)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralDouble(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroDouble(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        return self.value

    desc = "(double)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralString(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroString(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["string"] = self.value
        return out

    desc = "(string)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralBase64(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroBytes(), set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["base64"] = base64.b64encode(self.value)
        return out

    desc = "(bytes)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class Literal(LiteralValue):
    def __init__(self, avroPlaceholder, value, pos=None): pass

    def equals(self, other):
        if isinstance(other, Literal):
            return self.avroPlaceholder == other.avroPlaceholder and json.loads(self.value) == json.loads(other.value)
        else:
            return False

    def __hash__(self):
        return hash((self.avroPlaceholder, json.loads(self.value)))

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(self.avroType, set([self.desc]), self.value)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, fields, avroPlaceholder, avroTypeBuilder, pos=None): pass

    def equals(self, other):
        if isinstance(other, NewObject):
            return self.fields == other.fields and self.avroPlaceholder == other.avroPlaceholder
        else:
            return False

    def __hash__(self):
        return hash((self.fields, avroPlaceholder))

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        return super(NewObject, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.fields.values())

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return NewObject(dict((k, v.replace(pf)) for k, v in self.fields.items()),
                             self.avroPlaceholder,
                             self.avroTypeBuilder,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        fieldNameTypeExpr = []
        scope = symbolTable.newScope(True, True)
        for name, expr in self.fields.items():
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions)
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
    def __init__(self, items, avroPlaceholder, avroTypeBuilder, pos=None): pass

    def equals(self, other):
        if isinstance(other, NewArray):
            return self.items == other.items and self.avroPlaceholder == other.avroPlaceholder
        else:
            return False

    def __hash__(self):
        return hash((self.items, self.avroPlaceholder))

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        return super(NewArray, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.items)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return NewArray([x.replace(pf) for x in self.items],
                            self.avroPlaceholder,
                            self.avroTypeBuilder,
                            self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        scope = symbolTable.newScope(True, True)
        itemTypeExpr = []
        for expr in self.items:
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions)
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
    def __init__(self, body, pos=None):
        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" block must contain at least one expression", self.pos)

    def collect(self, pf):
        return super(Do, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Do([x.replace(pf) for x in self.body],
                      self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        scope = symbolTable.newScope(False, False)
        results = [x.walk(task, scope, functionTable, engineOptions) for x in self.body]

        inferredType = results[-1][0].retType
        if isinstance(inferredType, ExceptionType):
            inferredType = AvroNull()

        context = self.Context(inferredType, set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["do"] = [x.jsonNode(lineNumbers, memo) for x in self.body]
        return out

    desc = "do"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprs): pass

@titus.util.case
class Let(Expression):
    def __init__(self, values, pos=None):
        if len(self.values) < 1:
            raise PFASyntaxException("\"let\" must contain at least one declaration", self.pos)

    def collect(self, pf):
        return super(Let, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.values.values())

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Let(dict((k, v.replace(pf)) for k, v in self.values.items()),
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
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
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions)
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
        out = self.startDict(lineNumbers)
        out["let"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.values.items())
        return out

    desc = "let"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, nameTypeExpr): pass

@titus.util.case
class SetVar(Expression):
    def __init__(self, values, pos=None):
        if len(self.values) < 1:
            raise PFASyntaxException("\"set\" must contain at least one assignment", self.pos)

    def collect(self, pf):
        return super(SetVar, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.values.values())

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return SetVar(dict((k, v.replace(pf)) for k, v in self.values.items()),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        nameTypeExpr = []
        for name, expr in self.values.items():
            if symbolTable.get(name) is None:
                raise PFASemanticException("unknown symbol \"{0}\" cannot be assigned with \"set\" (use \"let\" to declare a new symbol)".format(name), self.pos)
            elif not symbolTable.writable(name):
                raise PFASemanticException("symbol \"{0}\" belongs to a sealed enclosing scope; it cannot be modified within this block)".format(name), self.pos)

            scope = symbolTable.newScope(True, True)
            exprContext, exprResult = expr.walk(task, scope, functionTable, engineOptions)
            calls = calls.union(exprContext.calls)

            if not symbolTable(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{0}\" was declared as {1}; it cannot be re-assigned as {2}".format(name, ts(symbolTable(name)), ts(exprContext.retType)), self.pos)

            nameTypeExpr.append((name, symbolTable(name), exprResult))

        context = self.Context(AvroNull(), calls.union(set([self.desc])), nameTypeExpr)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["set"] = OrderedDict((k, v.jsonNode(lineNumbers, memo)) for k, v in self.values.items())
        return out

    desc = "set"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, nameTypeExpr): pass

@titus.util.case
class AttrGet(Expression, HasPath):
    def __init__(self, expr, path, pos=None):
        if len(self.path) < 1:
            raise PFASyntaxException("attr path must have at least one key", self.pos)

    def collect(self, pf):
        return super(AttrGet, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return AttrGet(self.expr.replace(pf),
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        retType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions)
        context = self.Context(retType, calls.union(set([self.desc])), exprResult, exprContext.retType, pathResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["attr"] = self.expr.jsonNode(lineNumbers, memo)
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "attr"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr, exprType, path): pass

@titus.util.case
class AttrTo(Expression, HasPath):
    def __init__(self, expr, path, to, pos=None):
        if len(self.path) < 1:
            raise PFASyntaxException("attr path must have at least one key", self.pos)

    def collect(self, pf):
        return super(AttrTo, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return AttrTo(self.expr.replace(pf),
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        setType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.retType)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.fcnType)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType)   # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType)   # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)
        
    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["attr"] = self.expr.jsonNode(lineNumbers, memo)
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        out["to"] = self.to.jsonNode(lineNumbers, memo)
        return out

    desc = "attr-to"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr, exprType, setType, path, to, toType): pass

@titus.util.case
class CellGet(Expression, HasPath):
    def __init__(self, cell, path, pos=None): pass

    def collect(self, pf):
        return super(CellGet, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CellGet(self.cell,
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{0}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        retType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable, engineOptions)
        context = self.Context(retType, calls.union(set([self.desc])), self.cell, cellType, pathResult, shared)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["cell"] = self.cell
        if len(self.path) > 0:
            out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "cell"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, cell, cellType, path, shared): pass

@titus.util.case
class CellTo(Expression, HasPath):
    def __init__(self, cell, path, to, pos=None): pass

    def collect(self, pf):
        return super(CellTo, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CellTo(self.cell,
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{0}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        setType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable, engineOptions)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.retType, shared)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.fcnType, shared)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, shared)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(cellType, calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, shared)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["cell"] = self.cell
        if len(self.path) > 0:
            out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        out["to"] = self.to.jsonNode(lineNumbers, memo)
        return out

    desc = "cell-to"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, cell, cellType, setType, path, to, toType, shared): pass

@titus.util.case
class PoolGet(Expression, HasPath):
    def __init__(self, pool, path, pos=None):
        if len(self.path) < 1:
            raise PFASyntaxException("pool path must have at least one key", self.pos)

    def collect(self, pf):
        return super(PoolGet, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return PoolGet(self.pool,
                           [x.replace(pf) for x in self.path],
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{0}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        retType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions)
        context = self.Context(retType, calls.union(set([self.desc])), self.pool, pathResult, shared)
        return context, task(context, engineOptions)
        
    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["pool"] = self.pool
        out["path"] = [x.jsonNode(lineNumbers, memo) for x in self.path]
        return out

    desc = "pool"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, pool, path, shared): pass

@titus.util.case
class PoolTo(Expression, HasPath):
    def __init__(self, pool, path, to, init, pos=None):
        if len(self.path) < 1:
            raise PFASyntaxException("pool path must have at least one key", self.pos)

    def collect(self, pf):
        return super(PoolTo, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.path) + \
               self.to.collect(pf) + \
               self.init.collect(pf)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return PoolTo(self.pool,
                          [x.replace(pf) for x in self.path],
                          self.to.replace(pf),
                          self.init.replace(pf),
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{0}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        setType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable, engineOptions)

        initContext, initResult = self.init.walk(task, symbolTable, functionTable, engineOptions)

        if not poolType.accepts(initContext.retType):
            raise PFASemanticException("pool has type {0} but attempting to init with type {1}".format(ts(poolType), ts(initContext.retType)), self.pos)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with type {1}".format(ts(setType), ts(toContext.retType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.retType, initResult, shared)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.fcnType, initResult, shared)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, initResult, shared)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        elif isinstance(toContext, FcnRefFill.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {0} but attempting to assign with a function of type {1}".format(ts(setType), ts(toContext.fcnType)), self.pos)
            context = self.Context(poolType, calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, task(toContext, engineOptions), toContext.fcnType, initResult, shared)  # Two-parameter task?  task(toContext, engineOptions, toContext.fcnType)

        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
        def __init__(self, retType, calls, pool, poolType, setType, path, to, toType, init, shared): pass

@titus.util.case
class If(Expression):
    def __init__(self, predicate, thenClause, elseClause, pos=None):
        if len(self.thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", self.pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", self.pos)

    def collect(self, pf):
        return super(If, self).collect(pf) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return If(self.predicate.replace(pf),
                      [x.replace(pf) for x in self.thenClause],
                      [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                      self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        predScope = symbolTable.newScope(True, True)
        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"if\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        thenScope = symbolTable.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable, engineOptions) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions) for x in self.elseClause]
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
                    raise PFASemanticException(str(err))

            retType, elseTaskResults, elseSymbols = retType, [x[1] for x in elseResults], elseScope.inThisScope
        else:
            retType, elseTaskResults, elseSymbols = AvroNull(), None, None

        context = self.Context(retType, calls.union(set([self.desc])), thenScope.inThisScope, predResult, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, ifthens, elseClause, pos=None):
        if len(self.ifthens) < 1:
            raise PFASyntaxException("\"cond\" must contain at least one predicate-block pair", self.pos)

        for it in ifthens:
            if len(it.thenClause) < 1:
                raise PFASyntaxException("\"then\" clause must contain at least one expression", self.pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", self.pos)

    def collect(self, pf):
        return super(Cond, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.ifthens) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Cond([x.replace(pf) for x in self.ifthens],
                        [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                        self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        walkBlocks = []
        for ifthen in self.ifthens:
            predicate = ifthen.predicate
            thenClause = ifthen.thenClause
            ifpos = ifthen.pos

            predScope = symbolTable.newScope(True, True)
            predContext, predResult = predicate.walk(task, predScope, functionTable, engineOptions)
            if not AvroBoolean().accepts(predContext.retType):
                raise PFASemanticException("\"if\" predicate should be boolean, but is " + ts(predContext.retType), ifpos)
            calls = calls.union(predContext.calls)

            thenScope = symbolTable.newScope(False, False)
            thenResults = [x.walk(task, thenScope, functionTable, engineOptions) for x in thenClause]
            for exprCtx, exprRes in thenResults:
                calls = calls.union(exprCtx.calls)

            walkBlocks.append(self.WalkBlock(thenResults[-1][0].retType, thenScope.inThisScope, predResult, [x[1] for x in thenResults]))

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions) for x in self.elseClause]
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
                    raise PFASemanticException(str(err))

        context = self.Context(retType, calls.union(set([self.desc])), (self.elseClause is not None), walkBlocks)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, predicate, body, pos=None): pass

    def collect(self, pf):
        return super(While, self).collect(pf) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return While(self.predicate.replace(pf),
                         [x.replace(pf) for x in self.body],
                         self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"while\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        loopResults = [x.walk(task, loopScope, functionTable, engineOptions) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, predResult, [x[1] for x in loopResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, body, predicate, pos=None): pass

    def collect(self, pf):
        return super(DoUntil, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body) + \
               self.predicate.collect(pf)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return DoUntil([x.replace(pf) for x in self.body],
                           self.predicate.replace(pf),
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        loopResults = [x.walk(task, loopScope, functionTable, engineOptions) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable, engineOptions)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"until\" predicate should be boolean, but is " + ts(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, [x[1] for x in loopResults], predResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, init, predicate, step, body, pos=None):
        if len(self.init) < 1:
            raise PFASyntaxException("\"for\" must contain at least one declaration", self.pos)

        if len(self.step) < 1:
            raise PFASyntaxException("\"step\" must contain at least one assignment", self.pos)

        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", self.pos)

    def collect(self, pf):
        return super(For, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.init.values()) + \
               self.predicate.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.step.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return For(dict((k, v.replace(pf)) for k, v in self.init.items()),
                       self.predicate.replace(pf),
                       dict((k, v.replace(pf)) for k, v in self.step.items()),
                       [x.replace(pf) for x in self.body],
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
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
            exprContext, exprResult = expr.walk(task, initScope, functionTable, engineOptions)
            calls = calls.union(exprContext.calls)

            newSymbols[name] = exprContext.retType
            
            initNameTypeExpr.append((name, exprContext.retType, exprResult))

        for name, avroType in newSymbols.items():
            loopScope.put(name, avroType)

        predicateScope = loopScope.newScope(True, True)
        predicateContext, predicateResult = self.predicate.walk(task, predicateScope, functionTable, engineOptions)
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
            exprContext, exprResult = expr.walk(task, stepScope, functionTable, engineOptions)
            calls = calls.union(exprContext.calls)

            if not loopScope(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{0}\" was declared as {1}; it cannot be re-assigned as {2}".format(name, ts(loopScope(name)), ts(exprContext.retType)), self.pos)

            stepNameTypeExpr.append((name, loopScope(name), exprResult))

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), initNameTypeExpr, predicateResult, [x[1] for x in bodyResults], stepNameTypeExpr)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, name, array, body, seq, pos=None):
        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", self.pos)

    def collect(self, pf):
        return super(Foreach, self).collect(pf) + \
               self.array.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Foreach(self.name,
                           self.array.replace(pf),
                           [x.replace(pf) for x in self.body],
                           self.seq,
                           self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()
        loopScope = symbolTable.newScope(not self.seq, False)

        if symbolTable.get(self.name) is not None:
            raise PFASemanticException("symbol \"{0}\" may not be redeclared or shadowed".format(self.name), self.pos)

        if not validSymbolName(self.name):
            raise PFASemanticException("\"{0}\" is not a valid symbol name".format(self.name), self.pos)

        objScope = loopScope.newScope(True, True)
        objContext, objResult = self.array.walk(task, objScope, functionTable, engineOptions)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroArray):
            raise PFASemanticException("expression referred to by \"in\" should be an array, but is " + ts(objContext.retType), self.pos)
        elementType = objContext.retType.items

        loopScope.put(self.name, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.name, [x[1] for x in bodyResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, forkey, forval, map, body, pos=None):
        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", self.pos)

    def collect(self, pf):
        return super(Forkeyval, self).collect(pf) + \
               self.map.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Forkeyval(self.forkey,
                             self.forval,
                             self.map.replace(pf),
                             [x.replace(pf) for x in self.body],
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
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
        objContext, objResult = self.map.walk(task, objScope, functionTable, engineOptions)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroMap):
            raise PFASemanticException("expression referred to by \"in\" should be a map, but is " + ts(objContext.retType), self.pos)
        elementType = objContext.retType.values

        loopScope.put(self.forkey, AvroString())
        loopScope.put(self.forval, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable, engineOptions) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union([self.desc]), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.forkey, self.forval, [x[1] for x in bodyResults])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, avroPlaceholder, named, body, pos=None):
        if len(self.body) < 1:
            raise PFASyntaxException("\"do\" must contain at least one statement", self.pos)

        if not validSymbolName(self.named):
            raise PFASyntaxException("\"{0}\" is not a valid symbol name".format(self.named), self.pos)

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        return super(CastCase, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CastCase(self.avroPlaceholder,
                            self.named,
                            [x.replace(pf) for x in self.body],
                            self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        scope = symbolTable.newScope(False, False)
        scope.put(self.named, self.avroType)

        results = [x.walk(task, scope, functionTable, engineOptions) for x in self.body]
        context = self.Context(results[-1][0].retType, self.named, self.avroType, set(titus.util.flatten([x[0].calls for x in results])), scope.inThisScope, [x[1] for x in results])
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, expr, castCases, partial, pos=None): pass

    def collect(self, pf):
        return super(CastBlock, self).collect(pf) + \
               self.expr.collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.castCases)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return CastBlock(self.expr.replace(pf),
                             [x.replace(pf) for x in self.castCases],
                             self.partial,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable, engineOptions)
        calls = calls.union(exprContext.calls)

        exprType = exprContext.retType
        types = [x.avroType for x in self.castCases]

        # did you have anything extraneous?
        for t in types:
            if not exprType.accepts(t):
                raise PFASemanticException("\"cast\" of expression with type {0} can never satisfy case {1}".format(ts(exprType), ts(t)), self.pos)

        cases = [x.walk(task, symbolTable, functionTable, engineOptions) for x in self.castCases]
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
                raise PFASemanticException(str(err))

        context = self.Context(retType, calls.union(set([self.desc])), exprType, exprResult, cases, self.partial)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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
    def __init__(self, expr, avroPlaceholder, pos=None): pass

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        return super(Upcast, self).collect(pf) + \
               self.expr.collect(pf)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Upcast(self.expr.replace(pf),
                          self.avroPlaceholder,
                          self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        scope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, scope, functionTable, engineOptions)

        if not self.avroType.accepts(exprContext.retType):
            raise PFASemanticException("expression results in {0}; cannot expand (\"upcast\") to {1}".format(ts(exprContext.retType), ts(self.avroType)), self.pos)

        context = self.Context(self.avroType, exprContext.calls.union(set([self.desc])), exprResult)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["upcast"] = self.expr.jsonNode(lineNumbers, memo)
        out["as"] = self.avroPlaceholder.jsonNode(memo)
        return out

    desc = "upcast"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr): pass

@titus.util.case
class IfNotNull(Expression):
    def __init__(self, exprs, thenClause, elseClause, pos=None):
        if len(self.exprs) < 1:
            raise PFASyntaxException("\"ifnotnull\" must contain at least one symbol-expression mapping", self.pos)

        if len(self.thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", self.pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", self.pos)

    def collect(self, pf):
        return super(IfNotNull, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return IfNotNull(dict((k, v.replace(pf)) for k, v in self.exprs.items()),
                             [x.replace(pf) for x in self.thenClause],
                             [x.replace(pf) for x in self.elseClause] if self.elseClause is not None else None,
                             self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        calls = set()

        exprArgsScope = symbolTable.newScope(True, True)
        assignmentScope = symbolTable.newScope(False, False)

        symbolTypeResult = []
        for name, expr in self.exprs.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{0}\" is not a valid symbol name".format(name), self.pos)

            exprCtx, exprRes = expr.walk(task, exprArgsScope, functionTable, engineOptions)

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
        thenResults = [x.walk(task, thenScope, functionTable, engineOptions) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable, engineOptions) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            thenType = thenResults[-1][0].retType
            elseType = elseResults[-1][0].retType
            try:
                retType = LabelData.broadestType([thenType, elseType])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err))

            retType, elseTaskResults, elseSymbols = retType, [x[1] for x in elseResults], elseScope.inThisScope
        else:
            retType, elseTaskResults, elseSymbols = AvroNull(), None, None

        context = self.Context(retType, calls.union(set([self.desc])), symbolTypeResult, thenScope.inThisScope, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
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

@titus.util.case
class Doc(Expression):
    def __init__(self, comment, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["doc"] = self.comment
        return out

    desc = "doc"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class Error(Expression):
    def __init__(self, message, code, pos=None): pass

    def walk(self, task, symbolTable, functionTable, engineOptions):
        context = self.Context(ExceptionType(), set([self.desc]), self.message, self.code)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["error"] = self.message
        if self.code is not None:
            out["code"] = self.code
        return out

    desc = "error"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, message, code): pass

@titus.util.case
class Try(Expression):
    def __init__(self, exprs, filter, pos=None): pass

    def collect(self, pf):
        return super(Try, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Try([x.replace(pf) for x in self.exprs],
                       self.filter,
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        scope = symbolTable.newScope(True, True)
        results = [x.walk(task, scope, functionTable, engineOptions) for x in self.exprs]

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
    def __init__(self, exprs, namespace, pos=None): pass

    def collect(self, pf):
        return super(Log, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs)

    def replace(self, pf):
        if pf.isDefinedAt(self):
            return pf(self)
        else:
            return Log([x.replace(pf) for x in self.exprs],
                       self.namespace,
                       self.pos)

    def walk(self, task, symbolTable, functionTable, engineOptions):
        scope = symbolTable.newScope(True, True)
        results = [x.walk(task, scope, functionTable, engineOptions) for x in self.exprs]
        context = self.Context(AvroNull(), set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, results, self.namespace)
        return context, task(context, engineOptions)

    def jsonNode(self, lineNumbers, memo):
        out = self.startDict(lineNumbers)
        out["log"] = [x.jsonNode(lineNumbers, memo) for x in self.exprs]
        if self.namespace is not None:
            out["namespace"] = self.namespace
        return out

    desc = "log"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprTypes, namespace): pass

