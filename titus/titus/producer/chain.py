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

import json as jsonlib
import sys

import avro.schema

import titus.pfaast
import titus.reader
from titus.util import ts

from titus.genpy import PFAEngine

from titus.datatype import schemaToAvroType
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
from titus.datatype import AvroUnion
from titus.datatype import AvroField
from titus.datatype import AvroTypeBuilder
from titus.datatype import jsonDecoder
from titus.datatype import jsonEncoder

from titus.pfaast import Method
from titus.pfaast import EngineConfig
from titus.pfaast import Cell
from titus.pfaast import Pool
from titus.pfaast import FcnDef
from titus.pfaast import FcnRef
from titus.pfaast import FcnRefFill
from titus.pfaast import CallUserFcn
from titus.pfaast import Call
from titus.pfaast import Ref
from titus.pfaast import LiteralNull
from titus.pfaast import Literal
from titus.pfaast import NewObject
from titus.pfaast import NewArray
from titus.pfaast import Do
from titus.pfaast import CellGet
from titus.pfaast import CellTo
from titus.pfaast import PoolGet
from titus.pfaast import PoolTo
from titus.pfaast import CastCase
from titus.pfaast import Upcast

class PFAChainError(Exception): pass

def jsonNode(pfas, lineNumbers=True, check=True, name=None, randseed=None, doc=None, version=None, metadata={}, options={}, tryYaml=False, verbose=False):
    return ast(pfas, check, name, randseed, doc, version, metadata, options, tryYaml, verbose).jsonNode(lineNumbers, set())

def json(pfas, lineNumbers=True, check=True, name=None, randseed=None, doc=None, version=None, metadata={}, options={}, tryYaml=False, verbose=False):
    return ast(pfas, check, name, randseed, doc, version, metadata, options, tryYaml, verbose).toJson(lineNumbers)

def engine(pfas, name=None, randseed=None, doc=None, version=None, metadata={}, options={}, tryYaml=False, verbose=False, sharedState=None, multiplicity=1, style="pure", debug=False):
    return PFAEngine.fromAst(ast(pfas, False, name, randseed, doc, version, metadata, options, tryYaml, verbose), options, sharedState, multiplicity, style, debug)

def ast(pfas, check=True, name=None, randseed=None, doc=None, version=None, metadata={}, options={}, tryYaml=False, verbose=False):
    # normalize all input forms to ASTs
    if verbose: sys.stderr.write("Converting all inputs to ASTs\n")
    asts = []
    for i, src in enumerate(pfas):
        if verbose: sys.stderr.write("    step {0}\n".format(i + 1))
        if isinstance(src, EngineConfig):
            ast = src
        elif isinstance(src, dict):
            ast = titus.reader.jsonToAst(src)
        else:
            try:
                ast = titus.reader.jsonToAst(src)
            except ValueError:
                if tryYaml:
                    ast = titus.reader.yamlToAst(src)
                else:
                    raise
        asts.append(ast)
    pfas = asts

    # helper functions for transforming names
    def split(t):
        if "." in t:
            return t[:t.rindex(".")], t[t.rindex(".") + 1:]
        else:
            return None, t

    def join(ns, n):
        if ns is None or ns == "":
            return n
        else:
            return ns + "." + n

    def prefixType(i, pfa, t):
        ns, n = split(t)
        return join(ns, "Step{0:d}_{1}_{2}".format(i + 1, pfa.name, n))

    def prefixAction(i, pfa):
        return "step{0:d}_{1}_action".format(i + 1, pfa.name)

    def prefixFcnRef(i, pfa, x):
        if x.startswith("u."):
            return "u.step{0:d}_{1}_fcn_{2}".format(i + 1, pfa.name, x[2:])
        else:
            return x

    def prefixFcnDef(i, pfa, x):
        return "step{0:d}_{1}_fcn_{2}".format(i + 1, pfa.name, x)

    def prefixCell(i, pfa, x):
        return "step{0:d}_{1}_{2}".format(i + 1, pfa.name, x)

    def prefixPool(i, pfa, x):
        return "step{0:d}_{1}_{2}".format(i + 1, pfa.name, x)

    # define new names for all types to avoid type name collisions
    if verbose: sys.stderr.write("Changing type names to avoid collisions\n")
    originalNameToNewName = {}
    for i, pfa in enumerate(pfas):
        originalNameToNewName[i] = {}
        for typeName in pfa.inputPlaceholder.parser.names.names.keys():
            originalNameToNewName[i][typeName] = prefixType(i, pfa, typeName)

    # but any names in the input to the first and the output from the last should not be changed
    def trivialName(i, avroType, memo):
        if isinstance(avroType, AvroArray):
            trivialName(i, avroType.items, memo)
        elif isinstance(avroType, AvroMap):
            trivialName(i, avroType.values, memo)
        elif isinstance(avroType, AvroUnion):
            for t in avroType.types:
                trivialName(i, t, memo)
        elif isinstance(avroType, (AvroFixed, AvroEnum)):
            t = avroType.fullName
            originalNameToNewName[i][t] = t
        elif isinstance(avroType, AvroRecord):
            t = avroType.fullName
            if t not in memo:
                memo.add(t)
                for f in avroType.fields:
                    trivialName(i, f.avroType, memo)
                originalNameToNewName[i][t] = t
    trivialName(0, pfas[0].input, set())
    trivialName(len(pfas) - 1, pfas[-1].output, set())

    # ensure that chained types match and will be given the same names
    if verbose: sys.stderr.write("Verifying that input/output schemas match along the chain\n")
    def chainPair(i, first, second, memo):
        if isinstance(first, AvroNull) and isinstance(second, AvroNull):
            return True
        elif isinstance(first, AvroBoolean) and isinstance(second, AvroBoolean):
            return True
        elif isinstance(first, AvroInt) and isinstance(second, AvroInt):
            return True
        elif isinstance(first, AvroLong) and isinstance(second, AvroLong):
            return True
        elif isinstance(first, AvroFloat) and isinstance(second, AvroFloat):
            return True
        elif isinstance(first, AvroDouble) and isinstance(second, AvroDouble):
            return True
        elif isinstance(first, AvroBytes) and isinstance(second, AvroBytes):
            return True
        elif isinstance(first, AvroFixed) and isinstance(second, AvroFixed):
            t = avroType.fullName
            if first.size == second.size:
                originalNameToNewName[i + 1][second.fullName] = originalNameToNewName[i][first.fullName]
                return True
            else:
                return False
        elif isinstance(first, AvroString) and isinstance(second, AvroString):
            return True
        elif isinstance(first, AvroEnum) and isinstance(second, AvroEnum):
            if first.symbols == second.symbols:
                originalNameToNewName[i + 1][second.fullName] = originalNameToNewName[i][first.fullName]
                return True
            else:
                return False
        elif isinstance(first, AvroArray) and isinstance(second, AvroArray):
            return chainPair(i, first.items, second.items, memo)
        elif isinstance(first, AvroMap) and isinstance(second, AvroMap):
            return chainPair(i, first.values, second.values, memo)
        elif isinstance(first, AvroRecord) and isinstance(second, AvroRecord):
            if first.fullName not in memo:
                memo.add(first.fullName)
                if len(first.fields) != len(second.fields):
                    return False
                for f1, f2 in zip(first.fields, second.fields):
                    if f1.name != f2.name:
                        return False
                    elif not chainPair(i, f1.avroType, f2.avroType, memo):
                        return False
                originalNameToNewName[i + 1][second.fullName] = originalNameToNewName[i][first.fullName]
                return True
        elif isinstance(first, AvroUnion) and isinstance(second, AvroUnion):
            for yt in second.types:
                if not any(chainPair(i, xt, yt, memo) for xt in first.types):
                    return False
            return True
        else:
            return False
    for i in xrange(len(pfas) - 1):
        first = pfas[i].output
        second = pfas[i + 1].input
        if not chainPair(i, first, second, set()):
            raise PFAChainError("output of engine {0}: {1} not compatible with input of engine {2}: {3}".format(i + 1, ts(first), i + 2, ts(second)))

    def rename(i, avroType, memo):
        if isinstance(avroType, AvroArray):
            return AvroArray(rename(i, avroType.items, memo))
        elif isinstance(avroType, AvroMap):
            return AvroMap(rename(i, avroType.values, memo))
        elif isinstance(avroType, AvroUnion):
            return AvroUnion([rename(i, t, memo) for t in avroType.types])
        elif isinstance(avroType, AvroFixed):
            ns, n = split(originalNameToNewName[i][avroType.fullName])
            return AvroFixed(avroType.size, n, ns)
        elif isinstance(avroType, AvroEnum):
            ns, n = split(originalNameToNewName[i][avroType.fullName])
            return AvroEnum(avroType.symbols, n, ns)
        elif isinstance(avroType, AvroRecord):
            newName = originalNameToNewName[i][avroType.fullName]
            if newName in memo:
                return schemaToAvroType(memo[newName])
            else:
                ns, n = split(newName)
                schema = avro.schema.RecordSchema(n, ns, [], avro.schema.Names(), "record")
                memo[newName] = schema
                schema.set_prop("fields", [AvroField(f.name, rename(i, f.avroType, memo), f.default, f.order).schema for f in avroType.fields])
                return schemaToAvroType(memo[newName])
        else:
            return avroType

    avroTypeBuilder = AvroTypeBuilder()
    memo = {}
    def newPlaceholder(i, oldAvroType):
        newAvroType = rename(i, oldAvroType, {})
        return avroTypeBuilder.makePlaceholder(repr(newAvroType), memo)
            
    # combined name, if not explicitly set
    if name is None:
        name = "Chain_" + "_".join(pfa.name for pfa in pfas)

    # combined method (fold not supported yet, but could be)
    method = Method.MAP
    for pfa in pfas:
        if pfa.method == Method.EMIT:
            method = Method.EMIT
        elif pfa.method == Method.FOLD:
            raise NotImplementedError("chaining of fold-type scoring engines has not been implemented yet")

    # no zero or merge until we support fold method
    zero = None
    merge = None

    # input/output types from first and last
    inputPlaceholder = newPlaceholder(0, pfas[0].input)
    outputPlaceholder = newPlaceholder(len(pfas) - 1, pfas[-1].output)

    if verbose: sys.stderr.write("Converting model parameters\n")

    # combined cells with non-clobbering names
    cells = {"name": Cell(newPlaceholder(i, AvroString()), jsonlib.dumps(""), False, False),
             "instance": Cell(newPlaceholder(i, AvroInt()), jsonlib.dumps(0), False, False),
             "metadata": Cell(newPlaceholder(i, AvroMap(AvroString())), jsonlib.dumps({}), False, False),
             "actionsStarted": Cell(newPlaceholder(i, AvroLong()), jsonlib.dumps(0), False, False),
             "actionsFinished": Cell(newPlaceholder(i, AvroLong()), jsonlib.dumps(0), False, False)}
    if version is not None:
        cells["version"] = Cell(newPlaceholder(i, AvroInt()), 0, False, False)

    for i, pfa in enumerate(pfas):
        if verbose and len(pfa.cells) > 0: sys.stderr.write("    step {0}:\n".format(i + 1))
        for cellName, cell in pfa.cells.items():
            if verbose: sys.stderr.write("        cell {0}\n".format(cellName))
            oldAvroType = cell.avroType
            newAvroType = rename(i, oldAvroType, {})
            original = jsonDecoder(oldAvroType, jsonlib.loads(cell.init))
            converted = jsonlib.dumps(jsonEncoder(newAvroType, original))
            cells[prefixCell(i, pfa, cellName)] = Cell(newPlaceholder(i, cell.avroType), converted, cell.shared, cell.rollback, cell.pos)

    # combined pools with non-clobbering names
    pools = {}
    for i, pfa in enumerate(pfas):
        if verbose and len(pfa.pools) > 0: sys.stderr.write("    step {0}:\n".format(i + 1))
        for poolName, pool in pfa.pools.items():
            if verbose: sys.stderr.write("        pool {0}\n".format(poolName))
            oldAvroType = pool.avroType
            newAvroType = rename(i, oldAvroType, {})
            original = jsonDecoder(oldAvroType, jsonlib.loads(pool.init))
            converted = jsonlib.dumps(jsonEncoder(newAvroType, original))
            pools[prefixPool(i, pfa, poolName)] = Pool(newPlaceholder(i, pool.avroType), converted, pool.shared, pool.rollback, pool.pos)

    if verbose: sys.stderr.write("Converting scoring engine algorithm\n")

    # all code will go into user functions, including begin/action/end
    fcns = {}

    begin = [CellTo("name", [], Ref("name")),
             CellTo("instance", [], Ref("instance")),
             CellTo("metadata", [], Ref("metadata"))]
    if version is not None:
        begin.append(CellTo("version", [], Ref("version")))

    action = [CellTo("actionsStarted", [], Ref("actionsStarted")),
              CellTo("actionsFinished", [], Ref("actionsFinished"))]

    end = [CellTo("actionsStarted", [], Ref("actionsStarted")),
           CellTo("actionsFinished", [], Ref("actionsFinished"))]

    for i, pfa in enumerate(pfas):
        if verbose: sys.stderr.write("    step {0}: {1}\n".format(i + 1, pfa.name))

        thisActionFcnName = prefixAction(i, pfa)
        if i + 1 < len(pfas):
            nextActionFcnName = prefixAction(i + 1, pfas[i + 1])
        else:
            nextActionFcnName = None

        # this is a closure; it must be defined in the loop to pick up free variables
        lazyFcnReplacer = None
        def genericReplacer(expr, self):
            if isinstance(expr, FcnDef):
                return FcnDef([{t.keys()[0]: newPlaceholder(i, t.values()[0])} for t in expr.params],
                              newPlaceholder(i, expr.ret),
                              [x.replace(lazyFcnReplacer) for x in expr.body],     # this is the one place where we should pass down fcnReplacer rather than self
                              expr.pos)
            elif isinstance(expr, FcnRef):
                return FcnRef(prefixFcnRef(i, pfa, expr.name), epxr.pos)
            elif isinstance(expr, FcnRefFill):
                return FcnRefFill(prefixFcnRef(i, pfa, expr.name),
                                  dict((k, v.replace(self)) for k, v in expr.fill.items()),
                                  expr.pos)
            elif isinstance(expr, CallUserFcn):   # TODO: need to change the symbols of the corresponding enum
                return CallUserFcn(expr.name.replace(self),
                                   [x.replace(self) for x in expr.args],
                                   expr.pos)
            elif isinstance(expr, Call):
                if pfa.method == Method.EMIT and i + 1 < len(pfas) and expr.name == "emit":
                    return Call("u." + nextActionFcnName,
                                [x.replace(self) for x in expr.args],
                                expr.pos)
                else:
                    return Call(prefixFcnRef(i, pfa, expr.name),
                                [x.replace(self) for x in expr.args],
                                expr.pos)
            elif isinstance(expr, Literal):
                return Literal(newPlaceholder(i, expr.avroType),
                               expr.value,
                               expr.pos)
            elif isinstance(expr, NewObject):
                return NewObject(dict((k, v.replace(self)) for k, v in expr.fields.items()),
                                 newPlaceholder(i, expr.avroType),
                                 expr.pos)
            elif isinstance(expr, NewArray):
                return NewArray([x.replace(self) for x in expr.items],
                                newPlaceholder(i, expr.avroType),
                                expr.pos)
            elif isinstance(expr, CellGet):
                return CellGet(prefixCell(i, pfa, expr.cell),
                               [x.replace(self) for x in expr.path],
                               expr.pos)
            elif isinstance(expr, CellTo):
                return CellTo(prefixCell(i, pfa, expr.cell),
                              [x.replace(self) for x in expr.path],
                              expr.to.replace(self),
                              expr.pos)
            elif isinstance(expr, PoolGet):
                return PoolGet(prefixPool(i, pfa, expr.pool),
                               [x.replace(self) for x in expr.path],
                               expr.pos)
            elif isinstance(expr, PoolTo):
                return PoolTo(prefixPool(i, pfa, expr.pool),
                              [x.replace(self) for x in expr.path],
                              expr.to.replace(self),
                              expr.init.replace(self),
                              expr.pos)
            elif isinstance(expr, CastCase):
                return CastCase(newPlaceholder(i, expr.avroType),
                                expr.named,
                                [x.replace(self) for x in expr.body],
                                expr.pos)
            elif isinstance(expr, Upcast):
                return Upcast(expr.expr.replace(self),
                              newPlaceholder(i, expr.avroType),
                              expr.pos)
        genericReplacer.isDefinedAt = lambda x: isinstance(x, (FcnDef, FcnRef, FcnRefFill, CallUserFcn, Call, Literal, NewObject, CellGet, CellTo, PoolGet, PoolTo, CastCase, Upcast))

        def fcnReplacer(expr):
            return genericReplacer(expr, fcnReplacer)
        fcnReplacer.isDefinedAt = genericReplacer.isDefinedAt

        lazyFcnReplacer = fcnReplacer

        # add statements to begin
        def beginReplacer(expr):
            if isinstance(expr, Ref):
                if expr.name in ("name", "instance", "metadata") or (version is not None and expr.name == "version"):
                    return CellGet(expr.name, [], expr.pos)
                else:
                    return expr
            else:
                return genericReplacer(expr, beginReplacer)
        beginReplacer.isDefinedAt = lambda x: isinstance(x, Ref) or genericReplacer.isDefinedAt(x)
        begin.extend([x.replace(beginReplacer) for x in pfa.begin])

        # add statements to end
        def endReplacer(expr):
            if isinstance(expr, Ref):
                if expr.name in ("name", "instance", "metadata", "actionsStarted", "actionsFinished") or (version is not None and expr.name == "version"):
                    return CellGet(expr.name, [], expr.pos)
                else:
                    return expr
            else:
                return genericReplacer(expr, endReplacer)
        endReplacer.isDefinedAt = lambda x: isinstance(x, Ref) or genericReplacer.isDefinedAt(x)
        end.extend([x.replace(endReplacer) for x in pfa.end])

        # convert the action into a user function
        def actionReplacer(expr):
            if isinstance(expr, Ref):
                if expr.name in ("name", "instance", "metadata", "actionsStarted", "actionsFinished") or (version is not None and expr.name == "version"):
                    return CellGet(expr.name, [], expr.pos)
                else:
                    return expr
            else:
                return genericReplacer(expr, actionReplacer)
        actionReplacer.isDefinedAt = lambda x: isinstance(x, Ref) or genericReplacer.isDefinedAt(x)

        body = [x.replace(actionReplacer) for x in pfa.action]

        if method == Method.MAP:
            # if the overall method is MAP, then we know that all of the individual engines are MAP
            # the overall action calls a nested chain of engines-as-functions and each engine-as-a-function just does its job and returns (body is unmodified)
            fcns[thisActionFcnName] = FcnDef([{"input": newPlaceholder(i, pfa.input)}], newPlaceholder(i, pfa.output), body)
            if i == 0:
                action.append(Call("u." + thisActionFcnName, [Ref("input")]))
            else:
                action[-1] = Call("u." + thisActionFcnName, [action[-1]])

        elif method == Method.EMIT:
            # if the overall method is EMIT, then some individual engines might be MAP or might be EMIT
            # the overall action calls the first engine-as-a-function and the engines-as-functions call each other (body is modified)
            if pfa.method == Method.MAP and i + 1 < len(pfas):
                body = [Call("u." + nextActionFcnName, [Do(body)]), LiteralNull()]
            elif pfa.method == Method.MAP:
                body = [Call("emit", [Do(body)])]
            elif pfa.method == Method.EMIT:
                body.append(LiteralNull())

            fcns[thisActionFcnName] = FcnDef([{"input": newPlaceholder(i, pfa.input)}], newPlaceholder(i, AvroNull()), body)
            if i == 0:
                action.append(Call("u." + thisActionFcnName, [Ref("input")]))

        # convert all of the user functions into user functions
        for fcnName, fcnDef in pfa.fcns.items():
            # note: some of these user-defined functions may call emit; if so, they'll call the right emit
            fcns[prefixFcnDef(i, pfa, fcnName)] = FcnDef([{t.keys()[0]: newPlaceholder(i, t.values()[0])} for t in fcnDef.paramsPlaceholder],
                                                         newPlaceholder(i, fcnDef.ret),
                                                         [x.replace(fcnReplacer) for x in fcnDef.body],
                                                         fcnDef.pos)

    # make sure all the types work together
    if verbose: sys.stderr.write("Resolving types\n")
    avroTypeBuilder.resolveTypes()

    # randseed, doc, version, metadata, and options need to be explicitly set

    # return a (possibly checked) AST
    out = EngineConfig(name,
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
                       options)
    if check:
        if verbose: sys.stderr.write("Verifying PFA validity\n")
        PFAEngine.fromAst(out)

    if verbose: sys.stderr.write("Done\n")
    return out




## TODO: turn these into unit tests

## ### BEGIN testing

## import titus.prettypfa

## pfas = [titus.prettypfa.json('''
## input: record(value: string, Input)
## output: record(key: string, value: string, Output)
## action:
##   new(Output, key: input.value, value: input.value);
## '''), titus.prettypfa.json('''
## input: record(key: string, value: string, Input)
## output: record(number: int, Output)
## method: emit
## cells:
##   hold(Input) = {key: "what", value: "99"}
## end:
##   log(actionsFinished)
## action:
##   var one = 1;
##   u.parser(input.value, one);
##   u.parser(hold.value, one);
##   hold.value = s.concat(hold.value, "9");
##   u.parser(input.value, cast.int(actionsStarted));
## fcns:
##   parser = fcn(x: string, i: int -> null) emit(new(Output, number: parse.int(x, 10) + i));
## '''), titus.prettypfa.json('''
## input: record(number: int, Input)
## output: record(bigger: string, Output)
## action:
##   log(name);
##   new(Output, bigger: s.concat(s.number(input.number), "X"));
## ''')]

## eng, = engine(pfas)

## def emit(x):
##     print x
## eng.emit = emit
## eng.begin()
## eng.action({"value": "6"})
## eng.action({"value": "6"})
## eng.action({"value": "6"})
## eng.action({"value": "6"})
## eng.action({"value": "6"})
## eng.end()

## ### END testing
