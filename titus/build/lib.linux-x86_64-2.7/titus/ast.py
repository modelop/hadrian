#!/usr/bin/env python

import base64
import json
import re

import titus.lib1.array
import titus.lib1.bytes
import titus.lib1.core
import titus.lib1.enum
import titus.lib1.fixed
import titus.lib1.impute
import titus.lib1.map
import titus.lib1.metric
import titus.lib1.pfamath
import titus.lib1.record
import titus.lib1.pfastring
import titus.lib1.prob.dist
import titus.lib1.stat.change
import titus.lib1.stat.sample
import titus.lib1.model.cluster
import titus.lib1.model.tree

import titus.P as P
import titus.util

from titus.errors import PFASemanticException
from titus.errors import PFASyntaxException
from titus.fcn import Fcn
from titus.signature import IncompatibleTypes
from titus.signature import LabelData
from titus.signature import Sig
from titus.datatype import *

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

    context, result = expr.walk(NoTask(), symbolTable, functionTable)
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
            raise KeyError("no symbol named \"{}\"".format(name))
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
                        raise ValueError("no symbol named \"{}\"".format(name))
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
        functions.update(titus.lib1.core.provides)
        functions.update(titus.lib1.enum.provides)
        functions.update(titus.lib1.fixed.provides)
        functions.update(titus.lib1.impute.provides)
        functions.update(titus.lib1.map.provides)
        functions.update(titus.lib1.metric.provides)
        functions.update(titus.lib1.pfamath.provides)
        functions.update(titus.lib1.record.provides)
        functions.update(titus.lib1.pfastring.provides)
        functions.update(titus.lib1.prob.dist.provides)
        functions.update(titus.lib1.stat.change.provides)
        functions.update(titus.lib1.stat.sample.provides)
        functions.update(titus.lib1.model.tree.provides)
        functions.update(titus.lib1.model.cluster.provides)

        # TODO: functions.update(titus.lib1.other.provides)...

        return FunctionTable(functions)

############################################################ type-checking and transforming ASTs

class AstContext(object): pass
class ExpressionContext(object):
    def retType(self):
        raise NotImplementedError
    def calls(self):
        raise NotImplementedError
class TaskResult(object): pass

class Task(object):
    def __call__(self, astContext, resoledType=None):
        raise NotImplementedError

class NoTask(Task):
    class EmptyResult(TaskResult): pass
    def __call__(self, astContext, resolvedType=None):
        return self.EmptyResult()

############################################################ AST nodes

class Ast(object):
    def collect(self, pf):
        if pf.isDefinedAt(self):
            return [pf(self)]
        else:
            return []

    def walk(self, task, symbolTable=None, functionTable=None):
        if symbolTable is None and functionTable is None:
            self.walk(task, SymbolTable.blank, FunctionTable.blank)
        else:
            raise NotImplementedError

    @property
    def jsonNode(self):
        raise NotImplementedError

    def toJson():
        return json.dumps(self.jsonNode)

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
                 cells,
                 pools,
                 randseed,
                 doc,
                 metadata,
                 options,
                 pos=None):
        if len(self.action) < 1:
            raise PFASyntaxException("\"action\" must contain least one expression", self.pos)

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
    cells={cells},
    pools={pools},
    randseed={randseed},
    doc={doc},
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
                         cells=self.cells,
                         pools=self.pools,
                         randseed=self.randseed,
                         doc=self.doc,
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

    def walk(self, task, symbolTable, functionTable):
        topWrapper = SymbolTable(symbolTable, {}, self.cells, self.pools, True, False)

        userFunctions = {}
        for fname, fcnDef in self.fcns.items():
            ufname = "u." + fname
            if not validFunctionName(ufname):
                raise PFASemanticException("\"{}\" is not a valid function name".format(fname), self.pos)
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
            fcnContext, fcnResult = fcnDef.walk(task, scope, withUserFunctions)
            userFcnContexts.append((ufname, fcnContext))

        beginScope = topWrapper.newScope(True, False)
        beginContextResults = [x.walk(task, beginScope, withUserFunctions) for x in self.begin]
        beginResults = [x[1] for x in beginContextResults]
        beginCalls = set(titus.util.flatten([x[0].calls for x in beginContextResults]))

        endScope = topWrapper.newScope(True, False)
        endContextResults = [x.walk(task, endScope, withUserFunctions) for x in self.end]
        endResults = [x[1] for x in endContextResults]
        endCalls = set(titus.util.flatten([x[0].calls for x in endContextResults]))

        # it is important for action to be checked after all functions, begin, and end: see comment below
        actionScopeWrapper = topWrapper.newScope(True, False)
        actionScopeWrapper.put("input", self.input)
        if self.method == Method.FOLD:
            topWrapper.put("tally", self.output)  # note that this is after all user functions are defined, which keeps "tally" out of the user functions' scopes
        actionScope = actionScopeWrapper.newScope(True, False)

        actionContextResults = [x.walk(task, actionScope, withUserFunctions) for x in self.action]
        actionCalls = set(titus.util.flatten([x[0].calls for x in actionContextResults]))

        if self.method == Method.MAP or self.method == Method.FOLD:
            if not self.output.accepts(actionContextResults[-1][0].retType):
                raise PFASemanticException("action's inferred output type is {} but the declared output type is {}".format(actionContextResults[-1][0].retType, self.output), self.pos)

        context = self.Context(
            self.name,
            self.method,
            self.input,
            self.output,
            self.inputPlaceholder.parser.compiledTypes,
            (beginResults,
             beginScope.inThisScope,
             beginCalls),
            ([x[1] for x in actionContextResults],
             dict(list(actionScopeWrapper.inThisScope.items()) + list(actionScope.inThisScope.items())),
             actionCalls),
            (endResults,
             endScope.inThisScope,
             endCalls),
            userFcnContexts,
            self.zero,
            self.cells,
            self.pools,
            self.randseed,
            self.doc,
            self.metadata,
            self.options,
            self.inputPlaceholder.parser)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"name": self.name,
               "method": self.method,
               "input": json.loads(repr(self.inputPlaceholder)),
               "output": json.loads(repr(self.outputPlaceholder)),
               "action": [x.jsonNode for x in self.action],
               "cells": dict((k, v.jsonNode) for k, v in self.cells.items()),
               "pools": dict((k, v.jsonNode) for k, v in self.pools.items()),
               "options": dict((k, v) for k, v in self.options.items())}

        if len(self.begin) > 0:
            out["begin"] = [x.jsonNode for x in self.begin]

        if len(self.end) > 0:
            out["end"] = [x.jsonNode for x in self.end]

        if len(self.fcns) > 0:
            out["fcns"] = dict((k, v.jsonNode) for k, v in self.fcns.items())

        if self.zero is not None:
            out["zero"] = json.loads(self.zero)

        if self.randseed is not None:
            out["randseed"] = self.randseed

        if self.doc is not None:
            out["doc"] = self.doc

        if self.metadata is not None:
            out["metadata"] = self.metadata

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
                     cells,
                     pools,
                     randseed,
                     doc,
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

    def walk(self, task, symbolTable, functionTable):
        context = self.Context()
        return context, task(context)

    @property
    def jsonNode(self):
        return {"type": json.loads(repr(self.avroPlaceholder)), "init": json.loads(self.init), "shared": self.shared, "rollback": self.rollback}

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

    def walk(self, task, symbolTable, functionTable):
        context = self.Context()
        return context, task(context)

    @property
    def jsonNode(self):
        return {"type": json.loads(repr(self.avroPlaceholder)), "init": dict((k, json.loads(v)) for k, v in self.init.items()), "shared": self.shared, "rollback": self.rollback}
            
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
    def walkPath(self, avroType, task, symbolTable, functionTable):
        calls = set()
        scope = symbolTable.newScope(True, True)
        walkingType = avroType

        pathIndexes = []
        for indexIndex, expr in enumerate(self.path):
            exprContext, exprResult = expr.walk(task, scope, functionTable)
            calls = calls.union(exprContext.calls)
            
            if isinstance(walkingType, AvroArray):
                if isinstance(exprContext.retType, AvroLong) or isinstance(exprContext.retType, AvroInt):
                    walkingType = walkingType.items
                    pathIndexes.append(ArrayIndex(exprResult, walkingType))
                else:
                    raise PFASemanticException("path index for an array must resolve to a long or int; item {} is a {}".format(indexIndex, repr(exprContext.retType)), self.pos)

            elif isinstance(walkingType, AvroMap):
                if isinstance(exprContext.retType, AvroString):
                    walkingType = walkingType.values
                    pathIndexes.append(MapIndex(exprResult, walkingType))
                else:
                    raise PFASemanticException("path index for a map must resolve to a string; item {} is a {}".format(indexIndex, repr(exprContext.retType)), self.pos)

            elif isinstance(walkingType, AvroRecord):
                if isinstance(exprContext.retType, AvroString):
                    if isinstance(exprContext, LiteralString.Context):
                        name = exprContext.value
                    elif isinstance(exprContext, Literal.Context) and isinstance(exprContext.retType, AvroString):
                        name = json.loads(exprContext.value)
                    else:
                        raise PFASemanticException("path index for record {} must be a literal string; item {} is an object of type {}".format(repr(walkingType), indexIndex, repr(exprContext.retType)), self.pos)

                    if name in walkingType.fieldsDict.keys():
                        walkingType = walkingType.field(name).avroType
                        pathIndexes.append(RecordIndex(name, walkingType))
                    else:
                        raise PFASemanticException("record {} has no field named \"{}\" (path index {})".format(repr(walkingType), name, indexIndex), self.pos)

                else:
                    raise PFASemanticException("path index for a record must be a string; item {} is a {}".format(indexIndex, repr(exprContext.retType)), self.pos)

            else:
                raise PFASemanticException("path item {} is a {}, which cannot be indexed".format(indexIndex, repr(walkingType)), self.pos)

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

    def walk(self, task, symbolTable, functionTable):
        if len(self.paramsPlaceholder) > 22:
            raise PFASemanticException("function can have at most 22 parameters", self.pos)

        scope = symbolTable.newScope(True, False)
        for name, avroType in self.params.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{}\" is not a valid parameter name".format(name), self.pos)
            scope.put(name, avroType)

        results = [x.walk(task, scope, functionTable) for x in self.body]

        if not self.ret.accepts(results[-1][0].retType):
            raise PFASemanticException("function's inferred return type is {} but its declared return type is {}".format(results[-1][0].retType, self.ret), self.pos)

        context = self.Context(FcnType([self.params[k] for k in self.paramNames], self.ret), set(titus.util.flatten([x[0].calls for x in results])), self.paramNames, self.params, self.ret, scope.inThisScope, [x[1] for x in results])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"params": [{x.keys()[0]: json.loads(repr(x.values()[0]))} for x in self.paramsPlaceholder], "ret": json.loads(repr(self.retPlaceholder)), "do": [x.jsonNode for x in self.body]}

    @titus.util.case
    class Context(AstContext):
        def __init__(self, fcnType, calls, paramNames, params, ret, symbols, exprs): pass

@titus.util.case
class FcnRef(Argument):
    def __init__(self, name, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{}\"".format(self.name), self.pos)

        if isinstance(fcn.sig, Sig):
            params, ret = fcn.sig.params, fcn.sig.ret
            fcnType = FcnType([P.toType(p.values()[0]) for p in params], P.mustBeAvro(P.toType(ret)))
        else:
            raise PFASemanticException("only one-signature functions without constraints can be referenced (wrap \"{}\" in a function definition with the desired signature)".format(self.name), self.pos)

        context = FcnRef.Context(fcnType, set([self.name]), fcn)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"fcnref": self.name}

    @titus.util.case
    class Context(AstContext):
        def __init__(self, fcnType, calls, fcn): pass

@titus.util.case
class Call(Expression):
    def __init__(self, name, args, pos=None): pass

    def collect(self, pf):
        return super(Call, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.args)

    def walk(self, task, symbolTable, functionTable):
        fcn = functionTable.functions.get(self.name, None)
        if fcn is None:
            raise PFASemanticException("unknown function \"{}\"".format(self.name), self.pos)

        scope = symbolTable.newScope(True, True)
        argResults = [x.walk(task, scope, functionTable) for x in self.args]

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

        sigres = fcn.sig.accepts(argTypes)
        if sigres is not None:
            paramTypes, retType = sigres

            argContexts = [x[0] for x in argResults]
            argTaskResults = [x[1] for x in argResults]

            # Two-parameter task?
            # for i, a in enumerate(self.args):
            #     if isinstance(a, FcnRef):
            #         argTaskResults[i] = task(argContexts[i], paramTypes[i])

            context = self.Context(retType, calls, fcn, argTaskResults, argContexts, paramTypes)

        else:
            raise PFASemanticException("parameters of function \"{}\" do not accept [{}]".format(self.name, ",".join(map(repr, argTypes))), self.pos)

        return context, task(context)

    @property
    def jsonNode(self):
        return {self.name: [x.jsonNode for x in self.args]}

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, fcn, args, argContexts, paramTypes): pass

@titus.util.case
class Ref(Expression):
    def __init__(self, name, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        if symbolTable.get(self.name) is None:
            raise PFASemanticException("unknown symbol \"{}\"".format(self.name), self.pos)
        context = self.Context(symbolTable(self.name), set(), self.name)
        return context, task(context)

    @property
    def jsonNode(self):
        return self.name

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, name): pass

@titus.util.case
class LiteralNull(LiteralValue):
    def __init__(self, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context)

    @property
    def jsonNode(self):
        return None

    desc = "(null)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class LiteralBoolean(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroBoolean(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return self.value

    desc = "(boolean)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralInt(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroInt(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return self.value

    desc = "(int)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralLong(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroLong(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"long": self.value}

    desc = "(long)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralFloat(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroFloat(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"float": self.value}

    desc = "(float)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralDouble(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroDouble(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return self.value

    desc = "(double)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralString(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroString(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"string": self.value}

    desc = "(string)"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, value): pass

@titus.util.case
class LiteralBase64(LiteralValue):
    def __init__(self, value, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroBytes(), set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"base64": base64.b64encode(self.value)}

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

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(self.avroType, set([self.desc]), self.value)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"type": json.loads(repr(self.avroPlaceholder)), "value": json.loads(self.value)}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        fieldNameTypeExpr = []
        scope = symbolTable.newScope(True, True)
        for name, expr in self.fields.items():
            exprContext, exprResult = expr.walk(task, scope, functionTable)
            calls = calls.union(exprContext.calls)
            fieldNameTypeExpr.append((name, exprContext.retType, exprResult))

        if isinstance(self.avroType, AvroRecord):
            record = self.avroType
            newType = AvroRecord([AvroField(n, fieldType) for n, fieldType, xr in fieldNameTypeExpr], self.avroType.name, self.avroType.namespace)
            if not record.accepts(newType) or not newType.accepts(record):
                raise PFASemanticException("record constructed with \"new\" has incorrectly-typed fields: {} rather than {}".format(newType, record), self.pos)
            retType = record

        elif isinstance(self.avroType, AvroMap):
            if len(fieldNameTypeExpr) > 0:
                try:
                    newType = LabelData.broadestType([x[1] for x in fieldNameTypeExpr])
                except IncompatibleTypes as err:
                    raise PFASemanticException(str(err), self.pos)
                if not self.avroType.values.accepts(newType):
                    raise PFASemanticException("map constructed with \"new\" has incorrectly-typed items: {} rather than {}".format(newType, self.avroType.values), self.pos)
            retType = AvroMap(self.avroType.values)

        else:
            raise PFASemanticException("object constructed with \"new\" must have record or map type, not {}".format(self.avroType), self.pos)

        context = self.Context(retType, calls.union(set([self.desc])), dict((x[0], x[2]) for x in fieldNameTypeExpr))
        return context, task(context)

    @property
    def jsonNode(self):
        return {"new": dict((k, v.jsonNode) for k, v in self.fields.items()), "type": json.loads(repr(self.avroPlaceholder))}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        scope = symbolTable.newScope(True, True)
        itemTypeExpr = []
        for expr in self.items:
            exprContext, exprResult = expr.walk(task, scope, functionTable)
            calls = calls.union(exprContext.calls)
            itemTypeExpr.append((exprContext.retType, exprResult))

        if isinstance(self.avroType, AvroArray):
            if len(itemTypeExpr) > 0:
                try:
                    newType = LabelData.broadestType([x[0] for x in itemTypeExpr])
                except IncompatibleTypes as err:
                    raise PFASemanticException(str(err), self.pos)
                if not self.avroType.items.accepts(newType):
                    raise PFASemanticException("array constructed with \"new\" has incorrectly-typed items: {} rather than {}".format(newType, self.items), self.pos)
            retType = self.avroType
        else:
            raise PFASemanticException("array constructed with \"new\" must have array type, not {}".format(self.avroType), self.pos)

        context = self.Context(retType, calls.union(set([self.desc])), [x[1] for x in itemTypeExpr])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"new": [x.jsonNode for x in self.items], "type": json.loads(repr(self.avroPlaceholder))}

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

    def walk(self, task, symbolTable, functionTable):
        scope = symbolTable.newScope(False, False)
        results = [x.walk(task, scope, functionTable) for x in self.body]
        context = self.Context(results[-1][0].retType, set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, [x[1] for x in results])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"do": [x.jsonNode for x in self.body]}

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

    def walk(self, task, symbolTable, functionTable):
        if symbolTable.sealedWithin:
            raise PFASemanticException("new variable bindings are forbidden in this scope, but you can wrap your expression with \"do\" to make temporary variables", self.pos)

        calls = set()

        newSymbols = {}

        nameTypeExpr = []
        for name, expr in self.values.items():
            if symbolTable.get(name) is not None:
                raise PFASemanticException("symbol \"{}\" may not be redeclared or shadowed".format(name), self.pos)

            if not validSymbolName(name):
                raise PFASemanticException("\"{}\" is not a valid symbol name".format(name), self.pos)

            scope = symbolTable.newScope(True, True)
            exprContext, exprResult = expr.walk(task, scope, functionTable)
            calls = calls.union(exprContext.calls)

            newSymbols[name] = exprContext.retType

            nameTypeExpr.append((name, exprContext.retType, exprResult))

        for name, avroType in newSymbols.items():
            symbolTable.put(name, avroType)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), nameTypeExpr)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"let": dict((k, v.jsonNode) for k, v in self.values.items())}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        nameTypeExpr = []
        for name, expr in self.values.items():
            if symbolTable.get(name) is None:
                raise PFASemanticException("unknown symbol \"{}\" cannot be assigned with \"set\" (use \"let\" to declare a new symbol)".format(name), self.pos)
            elif not symbolTable.writable(name):
                raise PFASemanticException("symbol \"{}\" belongs to a sealed enclosing scope; it cannot be modified within this block)".format(name), self.pos)

            scope = symbolTable.newScope(True, True)
            exprContext, exprResult = expr.walk(task, scope, functionTable)
            calls = calls.union(exprContext.calls)

            if not symbolTable(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{}\" was declared as {}; it cannot be re-assigned as {}".format(name, symbolTable(name), exprContext.retType), self.pos)

            nameTypeExpr.append((name, symbolTable(name), exprResult))

        context = self.Context(AvroNull(), calls.union(set([self.desc])), nameTypeExpr)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"set": dict((k, v.jsonNode) for k, v in self.values.items())}

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

    def walk(self, task, symbolTable, functionTable):
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        retType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable)
        context = self.Context(retType, calls.union(set([self.desc])), exprResult, exprContext.retType, pathResult)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"attr": self.expr.jsonNode, "path": [x.jsonNode for x in self.path]}

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

    def walk(self, task, symbolTable, functionTable):
        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable)

        if not isinstance(exprContext.retType, (AvroArray, AvroMap, AvroRecord)):
            raise PFASemanticException("expression is not an array, map, or record", self.pos)

        setType, calls, pathResult = self.walkPath(exprContext.retType, task, symbolTable, functionTable)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("attr-and-path has type {} but attempting to assign with type {}".format(repr(setType), repr(toContext.retType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.retType)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, toResult, toContext.fcnType)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("attr-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            context = self.Context(exprContext.retType, calls.union(toContext.calls).union(set([self.desc])), exprResult, exprContext.retType, setType, pathResult, task(toContext), toContext.fcnType)   # Two-parameter task?  task(toContext, toContext.fcnType)

        return context, task(context)
        
    @property
    def jsonNode(self):
        return {"attr": self.expr.jsonNode, "path": [x.jsonNode for x in self.path], "to": self.to.jsonNode}

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

    def walk(self, task, symbolTable, functionTable):
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        retType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable)
        context = self.Context(retType, calls.union(set([self.desc])), self.cell, cellType, pathResult, shared)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"cell": self.cell}
        if len(self.path) > 0:
            out["path"] = [x.jsonNode for x in self.path]
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

    def walk(self, task, symbolTable, functionTable):
        c = symbolTable.cell(self.cell)
        if c is None:
            raise PFASemanticException("no cell named \"{}\"".format(self.cell), self.pos)
        cellType, shared = c.avroType, c.shared

        setType, calls, pathResult = self.walkPath(cellType, task, symbolTable, functionTable)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("cell-and-path has type {} but attempting to assign with type {}".format(repr(setType), repr(toContext.retType)), self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.retType, shared)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, toResult, toContext.fcnType, shared)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("cell-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.cell, cellType, setType, pathResult, task(toContext), toContext.fcnType, shared)  # Two-parameter task?  task(toContext, toContext.fcnType)

        return context, task(context)

    @property
    def jsonNode(self):
        out = {"cell": self.cell, "to": self.to.jsonNode}
        if len(self.path) > 0:
            out["path"] = [x.jsonNode for x in self.path]
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

    def walk(self, task, symbolTable, functionTable):
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        retType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable)
        context = self.Context(retType, calls.union(set([self.desc])), self.pool, pathResult, shared)
        return context, task(context)
        
    @property
    def jsonNode(self):
        return {"pool": self.pool, "path": [x.jsonNode for x in self.path]}

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
               (self.init.collect(pf) if self.init is not None else [])

    def walk(self, task, symbolTable, functionTable):
        p = symbolTable.pool(self.pool)
        if p is None:
            raise PFASemanticException("no pool named \"{}\"".format(self.pool), self.pos)
        poolType, shared = p.avroType, p.shared

        setType, calls, pathResult = self.walkPath(AvroMap(poolType), task, symbolTable, functionTable)

        toContext, toResult = self.to.walk(task, symbolTable, functionTable)

        if isinstance(toContext, ExpressionContext):
            if not setType.accepts(toContext.retType):
                raise PFASemanticException("pool-and-path has type {} but attempting to assign with type {}".format(repr(setType), repr(toContext.retType)), self.pos)
            if self.init is not None:
                raise PFASemanticException("if \"to\" is an expression, \"init\" is not allowed", self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.retType, toResult, shared)

        elif isinstance(toContext, FcnDef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            if self.init is None:
                raise PFASemanticException("if \"to\" is a function, \"init\" must be provided", self.pos)
            initContext, initResult = self.init.walk(task, symbolTable, functionTable)
            if not setType.accepts(initContext.retType):
                raise PFASemanticException("pool-and-path has type {} but attempting to init with type {}".format(repr(setType), repr(initContext.retType)), self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, toResult, toContext.fcnType, initResult, shared)

        elif isinstance(toContext, FcnRef.Context):
            if not FcnType([setType], setType).accepts(toContext.fcnType):
                raise PFASemanticException("pool-and-path has type {} but attempting to assign with a function of type {}".format(repr(setType), repr(toContext.fcnType)), self.pos)
            if self.init is None:
                raise PFASemanticException("if \"to\" is a function, \"init\" must be provided", self.pos)
            initContext, initResult = self.init.walk(task, symbolTable, functionTable)
            if not setType.accepts(initContext.retType):
                raise PFASemanticException("pool-and-path has type {} but attempting to init with type {}".format(repr(setType), repr(initContext.retType)), self.pos)
            context = self.Context(AvroNull(), calls.union(toContext.calls).union(set([self.desc])), self.pool, poolType, setType, pathResult, task(toContext), toContext.fcnType, initResult, shared)  # Two-parameter task?  task(toContext, toContext.fcnType)

        return context, task(context)

    @property
    def jsonNode(self):
        out = {"pool": self.pool, "path": [x.jsonNode for x in self.path], "to": self.to.jsonNode}
        if self.init is not None:
            out["init"] = self.init.jsonNode
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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        predScope = symbolTable.newScope(True, True)
        predContext, predResult = self.predicate.walk(task, predScope, functionTable)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"if\" predicate should be boolean, but is " + repr(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        thenScope = symbolTable.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable) for x in self.elseClause]
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

        context = self.Context(retType, calls.union(set([self.desc])), thenScope.inThisScope, predResult, [x[1] for x in thenResults], elseSymbols, elseTaskResults)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"if": self.predicate.jsonNode, "then": [x.jsonNode for x in self.thenClause]}
        if self.elseClause is not None:
            out["else"] = [x.jsonNode for x in self.elseClause]
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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        walkBlocks = []
        for ifthen in self.ifthens:
            predicate = ifthen.predicate
            thenClause = ifthen.thenClause
            ifpos = ifthen.pos

            predScope = symbolTable.newScope(True, True)
            predContext, predResult = predicate.walk(task, predScope, functionTable)
            if not AvroBoolean().accepts(predContext.retType):
                raise PFASemanticException("\"if\" predicate should be boolean, but is " + predContext.retType, ifpos)
            calls = calls.union(predContext.calls)

            thenScope = symbolTable.newScope(False, False)
            thenResults = [x.walk(task, thenScope, functionTable) for x in thenClause]
            for exprCtx, exprRes in thenResults:
                calls = calls.union(exprCtx.calls)

            walkBlocks.append(self.WalkBlock(thenResults[-1][0].retType, thenScope.inThisScope, predResult, [x[1] for x in thenResults]))

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable) for x in self.elseClause]
            for exprCtx, exprRes in elseResults:
                calls = calls.union(exprCtx.calls)

            walkBlocks.append(Cond.WalkBlock(elseResults[-1][0].retType, elseScope.inThisScope, None, [x[1] for x in elseResults]))
        
        if self.elseClause is None:
            retType = AvroNull()
        else:
            try:
                retType = LabelData.broadestType([x.retType for x in walkBlocks])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err))

        context = self.Context(retType, calls.union(set([self.desc])), (self.elseClause is not None), walkBlocks)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"cond": [x.jsonNode for x in self.ifthens]}
        if self.elseClause is not None:
            out["else"] = [x.jsonNode for x in self.elseClause]
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

    def walk(self, task, symbolTable, functionTable):
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"while\" predicate should be boolean, but is " + repr(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        loopResults = [x.walk(task, loopScope, functionTable) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, predResult, [x[1] for x in loopResults])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"while": self.predicate.jsonNode, "do": [x.jsonNode for x in self.body]}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()
        loopScope = symbolTable.newScope(False, False)
        predScope = loopScope.newScope(True, True)

        loopResults = [x.walk(task, loopScope, functionTable) for x in self.body]
        for exprCtx, exprRes in loopResults:
            calls = calls.union(exprCtx.calls)

        predContext, predResult = self.predicate.walk(task, predScope, functionTable)
        if not AvroBoolean().accepts(predContext.retType):
            raise PFASemanticException("\"until\" predicate should be boolean, but is " + repr(predContext.retType), self.pos)
        calls = calls.union(predContext.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), loopScope.inThisScope, [x[1] for x in loopResults], predResult)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"do": [x.jsonNode for x in self.body], "until": self.predicate.jsonNode}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()
        loopScope = symbolTable.newScope(False, False)

        newSymbols = {}

        initNameTypeExpr = []
        for name, expr in self.init.items():
            if loopScope.get(name) is not None:
                raise PFASemanticException("symbol \"{}\" may not be redeclared or shadowed".format(name), self.pos)

            if not validSymbolName(name):
                raise PFASemanticException("\"{}\" is not a valid symbol name".format(name), self.pos)

            initScope = loopScope.newScope(True, True)
            exprContext, exprResult = expr.walk(task, initScope, functionTable)
            calls = calls.union(exprContext.calls)

            newSymbols[name] = exprContext.retType
            
            initNameTypeExpr.append((name, exprContext.retType, exprResult))

        for name, avroType in newSymbols.items():
            loopScope.put(name, avroType)

        predicateScope = loopScope.newScope(True, True)
        predicateContext, predicateResult = self.predicate.walk(task, predicateScope, functionTable)
        if not AvroBoolean().accepts(predicateContext.retType):
            raise PFASemanticException("predicate should be boolean, but is " + repr(predicateContext.retType), self.pos)
        calls = calls.union(predicateContext.calls)

        stepNameTypeExpr = []
        for name, expr in self.step.items():
            if loopScope.get(name) is None:
                raise PFASemanticException("unknown symbol \"{}\" cannot be assigned with \"step\"".format(name), self.pos)
            elif not loopScope.writable(name):
                raise PFASemanticException("symbol \"{}\" belongs to a sealed enclosing scope; it cannot be modified within this block".format(name), self.pos)

            stepScope = loopScope.newScope(True, True)
            exprContext, exprResult = expr.walk(task, stepScope, functionTable)
            calls = calls.union(exprContext.calls)

            if not loopScope(name).accepts(exprContext.retType):
                raise PFASemanticException("symbol \"{}\" was declared as {}; it cannot be re-assigned as {}".format(name, loopScope(name), exprContext.retType), self.pos)

            stepNameTypeExpr.append((name, loopScope(name), exprResult))

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), initNameTypeExpr, predicateResult, [x[1] for x in bodyResults], stepNameTypeExpr)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"for": dict((k, v.jsonNode) for k, v in self.init.items()), "while": self.predicate.jsonNode, "step": dict((k, v.jsonNode) for k, v in self.step.items()), "do": [x.jsonNode for x in self.body]}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()
        loopScope = symbolTable.newScope(not self.seq, False)

        if symbolTable.get(self.name) is not None:
            raise PFASemanticException("symbol \"{}\" may not be redeclared or shadowed".format(self.name), self.pos)

        if not validSymbolName(self.name):
            raise PFASemanticException("\"{}\" is not a valid symbol name".format(self.name), self.pos)

        objScope = loopScope.newScope(True, True)
        objContext, objResult = self.array.walk(task, objScope, functionTable)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroArray):
            raise PFASemanticException("expression referred to by \"in\" should be an array, but is " + repr(objContext.retType), self.pos)
        elementType = objContext.retType.items

        loopScope.put(self.name, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union(set([self.desc])), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.name, [x[1] for x in bodyResults])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"foreach": self.name, "in": self.array.jsonNode, "do": [x.jsonNode for x in self.body], "seq": self.seq}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()
        loopScope = symbolTable.newScope(False, False)

        if symbolTable.get(self.forkey) is not None:
            raise PFASemanticException("symbol \"{}\" may not be redeclared or shadowed".format(self.forkey), self.pos)
        if symbolTable.get(self.forval) is not None:
            raise PFASemanticException("symbol \"{}\" may not be redeclared or shadowed".format(self.forval), self.pos)

        if not validSymbolName(self.forkey):
            raise PFASemanticException("\"{}\" is not a valid symbol name".format(self.forkey), self.pos)
        if not validSymbolName(self.forval):
            raise PFASemanticException("\"{}\" is not a valid symbol name".format(self.forval), self.pos)

        objScope = loopScope.newScope(True, True)
        objContext, objResult = self.map.walk(task, objScope, functionTable)
        calls = calls.union(objContext.calls)

        if not isinstance(objContext.retType, AvroMap):
            raise PFASemanticException("expression referred to by \"in\" should be a map, but is " + repr(objContext.retType), self.pos)
        elementType = objContext.retType.values

        loopScope.put(self.forkey, AvroString())
        loopScope.put(self.forval, elementType)

        bodyScope = loopScope.newScope(False, False)
        bodyResults = [x.walk(task, bodyScope, functionTable) for x in self.body]
        for exprCtx, exprRes in bodyResults:
            calls = calls.union(exprCtx.calls)

        context = self.Context(AvroNull(), calls.union([self.desc]), dict(list(bodyScope.inThisScope.items()) + list(loopScope.inThisScope.items())), objContext.retType, objResult, elementType, self.forkey, self.forval, [x[1] for x in bodyResults])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"forkey": self.forkey, "forval": self.forval, "in": self.map.jsonNode, "do": [x.jsonNode for x in self.body]}

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
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(self.named), self.pos)

    @property
    def avroType(self):
        return self.avroPlaceholder.avroType

    def collect(self, pf):
        return super(CastCase, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.body)

    def walk(self, task, symbolTable, functionTable):
        scope = symbolTable.newScope(False, False)
        scope.put(self.named, self.avroType)

        results = [x.walk(task, scope, functionTable) for x in self.body]
        context = self.Context(results[-1][0].retType, self.named, self.avroType, set(titus.util.flatten([x[0].calls for x in results])), scope.inThisScope, [x[1] for x in results])
        return context, task(context)

    @property
    def jsonNode(self):
        return {"as": json.loads(repr(self.avroPlaceholder)), "named": self.named, "do": [x.jsonNode for x in self.body]}

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

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        exprScope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, exprScope, functionTable)
        calls = calls.union(exprContext.calls)

        exprType = exprContext.retType
        types = [x.avroType for x in self.castCases]

        # did you have anything extraneous?
        for t in types:
            if not exprType.accepts(t):
                raise PFASemanticException("\"cast\" of expression with type {} can never satisfy case {}".format(exprType, t), self.pos)

        cases = [x.walk(task, symbolTable, functionTable) for x in self.castCases]
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
                    raise PFASemanticException("\"cast\" of expression with type {} does not contain a case for {}".format(exprType, mustFind), self.pos)

            try:
                retType = LabelData.broadestType([x[0].retType for x in cases])
            except IncompatibleTypes as err:
                raise PFASemanticException(str(err))

        context = self.Context(retType, calls.union(set([self.desc])), exprType, exprResult, cases, self.partial)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"cast": self.expr.jsonNode, "cases": [x.jsonNode for x in self.castCases], "partial": self.partial}

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

    def walk(self, task, symbolTable, functionTable):
        scope = symbolTable.newScope(True, True)
        exprContext, exprResult = self.expr.walk(task, scope, functionTable)

        if not self.avroType.accepts(exprContext.retType):
            raise PFASemanticException("expression results in {}; cannot expand (\"upcast\") to {}".format(exprContext.retType, self.avroType), self.pos)

        context = self.Context(self.avroType, exprContext.calls.union(set([self.desc])), exprResult)
        return context, task(context)

    @property
    def jsonNode(self):
        return {"upcast": self.expr.jsonNode, "as": json.loads(repr(self.avroPlaceholder))}

    desc = "upcast"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, expr): pass

@titus.util.case
class IfNotNull(Expression):
    def __init__(self, exprs, thenClause, elseClause, pos=None):
        if len(self.exprs) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", self.pos)

        if len(self.thenClause) < 1:
            raise PFASyntaxException("\"then\" clause must contain at least one expression", self.pos)

        if self.elseClause is not None and len(self.elseClause) < 1:
            raise PFASyntaxException("\"else\" clause must contain at least one expression", self.pos)

    def collect(self, pf):
        return super(IfNotNull, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs.values()) + \
               titus.util.flatten(x.collect(pf) for x in self.thenClause) + \
               (titus.util.flatten(x.collect(pf) for x in self.elseClause) if self.elseClause is not None else [])

    def walk(self, task, symbolTable, functionTable):
        calls = set()

        exprArgsScope = symbolTable.newScope(True, True)
        assignmentScope = symbolTable.newScope(False, False)

        symbolTypeResult = []
        for name, expr in self.exprs.items():
            if not validSymbolName(name):
                raise PFASemanticException("\"{}\" is not a valid symbol name".format(name), self.pos)

            exprCtx, exprRes = expr.walk(task, exprArgsScope, functionTable)

            avroType = None
            if isinstance(exprCtx.retType, AvroUnion) and any(isinstance(x, AvroNull) for x in exprCtx.retType.types):
                if len(exprCtx.retType.types) > 2:
                    avroType = AvroUnion([x for x in exprCtx.retType.types if not isinstance(x, AvroNull)])
                elif len(exprCtx.retType.types) > 1:
                    avroType = [x for x in exprCtx.retType.types if not isinstance(x, AvroNull)][0]
            if avroType is None:
                raise PFASemanticException("\"ifnotnull\" expressions must all be unions of something and null; case \"{}\" has type {}".format(name, exprCtx.retType))

            assignmentScope.put(name, avroType)

            calls = calls.union(exprCtx.calls)

            symbolTypeResult.append((name, avroType, exprRes))

        thenScope = assignmentScope.newScope(False, False)
        thenResults = [x.walk(task, thenScope, functionTable) for x in self.thenClause]
        for exprCtx, exprRes in thenResults:
            calls = calls.union(exprCtx.calls)

        if self.elseClause is not None:
            elseScope = symbolTable.newScope(False, False)

            elseResults = [x.walk(task, elseScope, functionTable) for x in self.elseClause]
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
        return context, task(context)

    @property
    def jsonNode(self):
        jsonExprs = {}
        for name, expr in self.exprs.items():
            jsonExprs[name] = expr.jsonNode
        out = {"ifnotnull": jsonExprs, "then": self.thenClause}
        if self.elseClause is not None:
            out["else"] = [x.jsonNode for x in self.elseClause]
        return out

    desc = "ifnotnull"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbolTypeResult, thenSymbols, thenClause, elseSymbols, elseClause): pass

@titus.util.case
class Doc(Expression):
    def __init__(self, comment, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroNull(), set([self.desc]))
        return context, task(context)

    @property
    def jsonNode(self):
        return {"doc": self.comment}

    desc = "doc"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls): pass

@titus.util.case
class Error(Expression):
    def __init__(self, message, code, pos=None): pass

    def walk(self, task, symbolTable, functionTable):
        context = self.Context(AvroNull(), set([self.desc]), self.message, self.code)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"error": self.message}
        if self.code is not None:
            out["code"] = self.code
        return out

    desc = "error"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, message, code): pass

@titus.util.case
class Log(Expression):
    def __init__(self, exprs, namespace, pos=None): pass

    def collect(self, pf):
        return super(Log, self).collect(pf) + \
               titus.util.flatten(x.collect(pf) for x in self.exprs)

    def walk(self, task, symbolTable, functionTable):
        scope = symbolTable.newScope(True, True)
        results = [x.walk(task, scope, functionTable) for x in self.exprs]
        context = self.Context(AvroNull(), set(titus.util.flatten([x[0].calls for x in results])).union(set([self.desc])), scope.inThisScope, results, self.namespace)
        return context, task(context)

    @property
    def jsonNode(self):
        out = {"log": [x.jsonNode for x in self.exprs]}
        if self.namespace is not None:
            out["namespace"] = self.namespace
        return out

    desc = "log"

    @titus.util.case
    class Context(ExpressionContext):
        def __init__(self, retType, calls, symbols, exprTypes, namespace): pass

