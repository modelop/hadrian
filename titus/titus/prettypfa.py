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

import ast as pythonast
import base64
import json as jsonlib
import re
from collections import OrderedDict

from titus.pfaast import validSymbolName
from titus.pfaast import validFunctionName
from titus.pfaast import Ast
from titus.pfaast import Method
from titus.pfaast import EngineConfig
from titus.pfaast import Cell
from titus.pfaast import Pool
from titus.pfaast import Argument
from titus.pfaast import Expression
from titus.pfaast import LiteralValue
from titus.pfaast import PathIndex
from titus.pfaast import ArrayIndex
from titus.pfaast import MapIndex
from titus.pfaast import RecordIndex
from titus.pfaast import HasPath
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
from titus.pfaast import Doc
from titus.pfaast import Error
from titus.pfaast import Try
from titus.pfaast import Log
from titus.datatype import AvroTypeBuilder
from titus.errors import PFASyntaxException
from titus.errors import PrettyPfaException
from titus.genpy import PFAEngine
import titus.pfaast
import titus.util

class Token(object):
    def __init__(self, t, v, lineno):
        self.t = t
        self.v = v
        self.lineno = lineno
    def __repr__(self):
        return "{}({})".format(self.t, self.v)

class InterpretationState(object):
    def __init__(self):
        self.avroTypeBuilder = AvroTypeBuilder()
        self.avroTypeAlias = {}
        self.functionNames = set(titus.pfaast.FunctionTable.blank().functions)
        self.cellNames = set()
        self.poolNames = set()

class Section(object):
    def __init__(self, name, content, lineno):
        self.name = name
        self.content = content
        self.lineno = lineno

        if name == "name":
            pass
        elif name == "method":
            pass
        elif name == "input":
            pass
        elif name == "output":
            pass
        elif name == "types":
            if isinstance(content, MiniCall) and content.name == "do":
                self.content = content.args
            elif not isinstance(content, (list, tuple)):
                self.content = [content]
        elif name == "begin":
            if isinstance(content, MiniCall) and content.name == "do":
                self.content = content.args
            elif not isinstance(content, (list, tuple)):
                self.content = [content]
        elif name == "action":
            if isinstance(content, MiniCall) and content.name == "do":
                self.content = content.args
            elif not isinstance(content, (list, tuple)):
                self.content = [content]
        elif name == "end":
            if isinstance(content, MiniCall) and content.name == "do":
                self.content = content.args
            elif not isinstance(content, (list, tuple)):
                self.content = [content]
        elif name == "fcns":
            pass
        elif name == "zero":
            pass
        elif name == "merge":
            if isinstance(content, MiniCall) and content.name == "do":
                self.content = content.args
            elif not isinstance(content, (list, tuple)):
                self.content = [content]
        elif name == "cells":
            pass
        elif name == "pools":
            pass
        elif name == "randseed":
            pass
        elif name == "doc":
            pass
        elif name == "version":
            pass
        elif name == "metadata":
            pass
        elif name == "options":
            pass
        else:
            raise PrettyPfaException("Unrecognized section heading \"{}\" at {}".format(name, lineno))

    def __repr__(self):
        return "Section({}, {} at {})".format(self.name, self.content, self.pos)

    def input(self, state):
        return state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.content.asType(state)))

    def output(self, state):
        return state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.content.asType(state)))

    def cells(self, state):
        return dict(x.asCell(state) for x in self.content)

    def pools(self, state):
        return dict(x.asPool(state) for x in self.content)

    def method(self):
        if isinstance(self.content, MiniString):
            out = self.content.value
        elif isinstance(self.content, MiniDotName):
            out = self.content.name
        else:
            raise PrettyPfaException("method must be a string at PrettyPFA line {}".format(self.lineno))
        if out == "map":
            return Method.MAP
        elif out == "emit":
            return Method.EMIT
        elif out == "fold":
            return Method.FOLD
        else:
            raise PrettyPfaException("method must be \"map\", \"emit\", or \"fold\" at PrettyPFA line {}".format(self.lineno))

    def types(self, state):
        for x in self.content:
            x.defType(state)

    def begin(self, state):
        return [x.asExpr(state) for x in self.content]

    def action(self, state):
        return [x.asExpr(state) for x in self.content]

    def end(self, state):
        return [x.asExpr(state) for x in self.content]

    def fcns(self, state):
        return dict(x.asFcn(state) for x in self.content)

    def zero(self):
        return jsonlib.dumps(self.content.asJson())

    def merge(self, state):
        return [x.asExpr(state) for x in self.content]

    def randseed(self):
        if not isinstance(self.content, MiniNumber) or not isinstance(self.content.value, (int, long)):
            raise PrettyPfaException("randseed must be an integer at PrettyPFA line {}".format(self.lineno))
        return self.content.value

    def doc(self):
        if isinstance(self.content, MiniString):
            out = self.content.value
        elif isinstance(self.content, MiniDotName):
            out = self.content.name
        else:
            raise PrettyPfaException("doc must be a string at PrettyPFA line {}".format(self.lineno))
        return out

    def version(self):
        if not isinstance(self.content, MiniNumber) or not isinstance(self.content.value, (int, long)):
            raise PrettyPfaException("version must be an integer at PrettyPFA line {}".format(self.lineno))
        return self.content.value

    def metadata(self):
        return self.content.asJson()

    def options(self):
        return self.content.asJson()

class MiniAst(object):
    def __init__(self, low, high):
        self.low, self.high = low, high
    @property
    def pos(self):
        if self.low == self.high:
            return "PrettyPFA line {}".format(self.low)
        else:
            return "PrettyPFA lines {}-{}".format(self.low, self.high)
    def asExpr(self, state):
        raise PrettyPfaException("{} ({}) is not an expression at {}".format(self, type(self), self.pos))
    def asType(self, state):
        raise PrettyPfaException("{} ({}) is not a type specification at {}".format(self, type(self), self.pos))
    def asJson(self):
        raise PrettyPfaException("{} ({}) is not JSON at {}".format(self, type(self), self.pos))
    def asFcn(self):
        raise PrettyPfaException("{} ({}) is not a function definition at {}".format(self, type(self), self.pos))
    def asCell(self, state):
        raise PrettyPfaException("{} ({}) is not a cell definition at {}".format(self, type(self), self.pos))
    def asPool(self, state):
        raise PrettyPfaException("{} ({}) is not a cell definition at {}".format(self, type(self), self.pos))
    def defType(self, state):
        raise PrettyPfaException("{} ({}) is not a type definition at {}".format(self, type(self), self.pos))

class MiniGenGet(MiniAst):
    def __init__(self, expr, args, low, high):
        self.expr = expr
        self.args = args
        super(MiniGenGet, self).__init__(low, high)
    def __repr__(self):
        return "MiniGenGet({}, {})".format(self.expr, self.args)
    def asExpr(self, state):
        return AttrGet(self.expr.asExpr(state), [x.asExpr(state) for x in self.args], self.pos)

class MiniGet(MiniAst):
    def __init__(self, name, args, low, high):
        self.name = name
        self.args = args
        super(MiniGet, self).__init__(low, high)
    def __repr__(self):
        return "MiniGet({}, {})".format(self.name, self.args)
    def asExpr(self, state):
        if "." in self.name:
            pieces = self.name.split(".")
            base = pieces[0]
            path = [LiteralString(x, self.pos) for x in pieces[1:]] + [x.asExpr(state) for x in self.args]
        else:
            base = self.name
            path = [x.asExpr(state) for x in self.args]
        if base in state.cellNames:
            return CellGet(base, path, self.pos)
        elif base in state.poolNames:
            return PoolGet(base, path, self.pos)
        else:
            return AttrGet(Ref(base, self.pos), path, self.pos)

class MiniTo(MiniAst):
    def __init__(self, name, args, direct, to, init, low, high):
        self.name = name
        self.args = args
        self.direct = direct
        self.to = to
        self.init = init
        super(MiniTo, self).__init__(low, high)
    def __repr__(self):
        return "MiniTo({}, {}, {}, {}, {})".format(self.name, self.args, self.direct, self.to, self.init)
    def asExpr(self, state):
        if "." in self.name:
            pieces = self.name.split(".")
            base = pieces[0]
            path = [LiteralString(x, self.pos) for x in pieces[1:]] + [x.asExpr(state) for x in self.args]
        else:
            base = self.name
            path = [x.asExpr(state) for x in self.args]

        to = self.to.asExpr(state)
        if self.direct:
            if isinstance(to, FcnRef):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to.name, to.pos))
            elif isinstance(to, (FcnDef, FcnRefFill)):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to, to.pos))
        else:
            if not isinstance(to, (FcnRef, FcnDef, FcnRefFill)):
                raise PrettyPfaException("indirect assignments (with a \"to\" keyword) must refer to functions, not {} at {}".format(to, to.pos))
            if base in state.poolNames:
                if self.init is None:
                    raise PrettyPfaException("indirect pool assignments (with a \"to\" keyword) must also have an \"init\" at {}".format(self.pos))

        if base not in state.poolNames:
            if self.init is not None:
                raise PrettyPfaException("non-pool assignments must not have an \"init\" at {}".format(self.pos))

        if base in state.cellNames:
            return CellTo(base, path, to, self.pos)
        elif base in state.poolNames:
            if self.init is None:
                init = self.to.asExpr(state)
            else:
                init = self.init.asExpr(state)
            return PoolTo(base, path, to, init, self.pos)
        else:
            return AttrTo(Ref(base, self.pos), path, to, self.pos)

class MiniEnumSymbol(MiniAst):
    def __init__(self, enumType, enumValue, low, high):
        self.enumType = enumType
        self.enumValue = enumValue
        super(MiniEnumSymbol, self).__init__(low, high)
    def __repr__(self):
        return "MiniEnumSymbols({}, {})".format(self.enumType, self.enumValue)
    def asExpr(self, state):
        return Literal(state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.enumType)), jsonlib.dumps(self.enumValue), self.pos)

class MiniDotName(MiniAst):
    def __init__(self, name, lineno):
        self.name = name
        super(MiniDotName, self).__init__(lineno, lineno)
    def __eq__(self, other):
        return isinstance(other, MiniDotName) and self.name == other.name
    def __repr__(self):
        return "MiniDotName({})".format(self.name)
    def asExpr(self, state):
        if self.name == "null":
            return LiteralNull()
        elif self.name == "true":
            return LiteralBoolean(True)
        elif self.name == "false":
            return LiteralBoolean(False)
        elif self.name in state.functionNames:
            return FcnRef(self.name, self.pos)
        else:
            if "." in self.name:
                pieces = self.name.split(".")
                base = pieces[0]
                path = [LiteralString(x, self.pos) for x in pieces[1:]]
                if base in state.cellNames:
                    return CellGet(base, path, self.pos)
                elif base in state.poolNames:
                    return PoolGet(base, path, self.pos)
                else:
                    return AttrGet(Ref(base, self.pos), path, self.pos)
            else:
                if self.name in state.cellNames:
                    return CellGet(self.name, [], self.pos)
                elif self.name in state.poolNames:
                    return PoolGet(self.name, [], self.pos)
                else:
                    return Ref(self.name, self.pos)
    def asType(self, state):
        if self.name in state.avroTypeAlias:
            return state.avroTypeAlias[self.name]
        else:
            return self.name
    def asJson(self):
        return self.name

class MiniNumber(MiniAst):
    def __init__(self, value, lineno):
        self.value = value
        super(MiniNumber, self).__init__(lineno, lineno)
    def __repr__(self):
        return "MiniNumber({})".format(repr(self.value))
    def asExpr(self, state):
        if isinstance(self.value, (int, long)):
            return LiteralInt(self.value, self.pos)
        else:
            return LiteralDouble(self.value, self.pos)
    def asJson(self):
        return self.value

class MiniString(MiniAst):
    def __init__(self, value, lineno):
        self.value = value
        super(MiniString, self).__init__(lineno, lineno)
    def __repr__(self):
        return "MiniString({})".format(repr(self.value))
    def asExpr(self, state):
        return LiteralString(self.value, self.pos)
    def asJson(self):
        return self.value

class MiniCall(MiniAst):
    def __init__(self, name, args, low0, high0):
        self.name = name
        self.args = args
        if len(args) > 0:
            low = min([x.low for x in args])
            high = max([x.high for x in args])
            if low0 is not None:
                low = min(low, low0)
            if high0 is not None:
                high = max(high, high0)
        else:
            low, high = low0, high0
        super(MiniCall, self).__init__(low, high)
    def __repr__(self):
        return "MiniCall({}, [{}])".format(self.name, ", ".join(map(repr, self.args)))

    def asExpr(self, state):
        if self.name == "json":
            if len(self.args) != 2:
                raise PrettyPfaException("json function must have 2 arguments, not {}, at {}".format(len(self.args), self.pos))
            td = self.args[0].asType(state)
            v = self.args[1].asJson()
            if td == "null":
                return LiteralNull(self.pos)
            elif td == "int":
                return LiteralInt(v, self.pos)
            elif td == "long":
                return LiteralLong(v, self.pos)
            elif td == "float":
                return LiteralFloat(v, self.pos)
            elif td == "double":
                return LiteralDouble(v, self.pos)
            elif td == "string":
                return LiteralString(v, self.pos)
            elif td == "bytes":
                return LiteralBase64(base64.b64decode(v), self.pos)
            else:
                return Literal(state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(td)), jsonlib.dumps(v), self.pos)

        elif self.name == "int":
            if len(self.args) != 1:
                raise PrettyPfaException("int function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralInt(self.args[0].asJson(), self.pos)

        elif self.name == "long":
            if len(self.args) != 1:
                raise PrettyPfaException("long function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralLong(self.args[0].asJson(), self.pos)

        elif self.name == "float":
            if len(self.args) != 1:
                raise PrettyPfaException("float function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralFloat(self.args[0].asJson(), self.pos)

        elif self.name == "double":
            if len(self.args) != 1:
                raise PrettyPfaException("double function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralDouble(self.args[0].asJson(), self.pos)

        elif self.name == "string":
            if len(self.args) != 1:
                raise PrettyPfaException("string function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralString(self.args[0].asJson(), self.pos)

        elif self.name == "bytes":
            if len(self.args) != 1:
                raise PrettyPfaException("bytes function must have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return LiteralBase64(base64.b64decode(self.args[0].asJson()), self.pos)

        elif self.name == "do":
            return Do([x.asExpr(state) for x in self.args])

        elif self.name == "apply":
            if len(self.args) < 1:
                raise PrettyPfaException("apply function must have at least 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return CallUserFcn(self.args[0].asExpr(state), [x.asExpr(state) for x in self.args[1:]], self.pos)

        elif self.name == "update":
            if len(self.args) != 2:
                raise PrettyPfaException("update function must have exactly 2 arguments, not {}, at {}".format(len(self.args), self.pos))
            rec = self.args[0].asExpr(state)
            if not isinstance(self.args[1], MiniParam):
                raise PrettyPfaException("update function second argument must be a key-value pair, not a plain expression, at {}".format(self.pos))
            return AttrTo(rec, [LiteralString(self.args[1].name, self.pos)], self.args[1].typeExpr.asExpr(state), self.pos)

        elif self.name == "new":
            if len(self.args) < 2:
                raise PrettyPfaException("new function must have at least 2 arguments, not {}, at {}".format(len(self.args), self.pos))
            td = self.args[0].asType(state)
            if isinstance(td, dict) and td.get("type") == "array":
                if any(isinstance(x, MiniParam) for x in self.args[1:]):
                    raise PrettyPfaException("new array must only consist of plain expressions, not key-value pairs, at {}".format(self.pos))
                return NewArray([x.asExpr(state) for x in self.args[1:]], state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(td)), state.avroTypeBuilder, self.pos)
            else:
                if not all(isinstance(x, MiniParam) for x in self.args[1:]):
                    raise PrettyPfaException("new map/record must only consist of key-value pairs, not plain expressions, at {}".format(self.pos))
                return NewObject(dict((x.name, x.typeExpr.asExpr(state)) for x in self.args[1:]), state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(td)), state.avroTypeBuilder, self.pos)

        elif self.name == "upcast":
            if len(self.args) != 2:
                raise PrettyPfaException("upcast function requires exactly 2 arguments, not {}, at {}".format(len(self.args), self.pos))
            t = state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.args[0].asType(state)))
            v = self.args[1].asExpr(state)
            return Upcast(v, t, self.pos)

        elif self.name == "doc":
            if len(self.args) != 1:
                raise PrettyPfaException("doc function has only 1 argument, not {}, at {}".format(len(self.args), self.pos))
            if not isinstance(self.args[0], MiniString):
                raise PrettyPfaException("doc function argument must be a string, not {}, at {}".format(self.args[0], self.pos))
            return Doc(self.args[0].value)

        elif self.name == "error":
            params = [x for x in self.args if isinstance(x, MiniParam)]
            if len(params) == 0:
                code = None
            elif len(params) == 1:
                if params[0].name != "code":
                    raise PrettyPfaException("error function has only 1 optional parameter, \"code\", not {}, at {}".format(params[0].name, self.pos))
                if not isinstance(params[0].typeExpr, MiniNumber) and not isinstance(params[0].typeExpr.value, (int, long)):
                    raise PrettyPfaException("error function has optional parameter \"code\" must be an integer, not {}, at {}".format(params[0].typeExpr, self.pos))
                code = params[0].typeExpr.value
            else:
                raise PrettyPfaException("error function has only 1 optional parameter, not {}, at {}".format(len(params), self.pos))

            others = [x for x in self.args if not isinstance(x, MiniParam)]
            if len(others) != 1:
                raise PrettyPfaException("error function requires exactly 1 argument, not {}, at {}".format(len(others), self.pos))
            if not isinstance(others[0], (MiniDotName, MiniString)):
                raise PrettyPfaException("error function argument must be a string, not {}, at {}".format(others[0], self.pos))
            if isinstance(others[0], MiniDotName):
                message = others[0].name
            else:
                message = others[0].value
            return Error(message, code, self.pos)

        elif self.name == "log":
            params = [x for x in self.args if isinstance(x, MiniParam)]
            if len(params) == 0:
                namespace = None
            elif len(params) == 1:
                if params[0].name != "namespace":
                    raise PrettyPfaException("log function has only 1 optional parameter, \"namespace\", not {}, at {}".format(params[0].name, self.pos))
                if not isinstance(params[0].typeExpr, (MiniDotName, MiniString)):
                    raise PrettyPfaException("log function has optional parameter \"namespace\" must be a string, not {}, at {}".format(params[0].typeExpr, self.pos))
                if isinstance(params[0].typeExpr, MiniDotName):
                    namespace = params[0].typeExpr.name
                else:
                    namespace = params[0].typeExpr.value
            else:
                raise PrettyPfaException("log function has only 1 optional parameter, not {}, at {}".format(len(params), self.pos))
            return Log([x.asExpr(state) for x in self.args if not isinstance(x, MiniParam)], namespace, self.pos)

        elif any(isinstance(x, MiniParam) for x in self.args):
            fill = dict((x.name, x.typeExpr.asExpr(state)) for x in self.args if isinstance(x, MiniParam))
            return FcnRefFill(self.name, fill, self.pos)

        else:
            return Call(self.name, [x.asExpr(state) for x in self.args], self.pos)

    def asType(self, state):
        def split(name):
            if "." in name:
                pieces = name.split(".")
                return ".".join(pieces[:-1]), pieces[-1]
            else:
                return None, name

        if self.name == "array":
            if len(self.args) != 1:
                raise PrettyPfaException("array type should have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return {"type": "array", "items": self.args[0].asType(state)}

        elif self.name == "map":
            if len(self.args) != 1:
                raise PrettyPfaException("map type should have 1 argument, not {}, at {}".format(len(self.args), self.pos))
            return {"type": "map", "values": self.args[0].asType(state)}

        elif self.name == "union":
            if len(self.args) < 2:
                raise PrettyPfaException("union type should have at least 2 arguments, not {}, at {}".format(len(self.args), self.pos))
            return [x.asType(state) for x in self.args]

        elif self.name == "fixed":
            if len(self.args) < 1 or len(self.args) > 2:
                raise PrettyPfaException("fixed type should have 1 or 2 arguments, not {}, at {}".format(len(self.args), self.pos))

            if not isinstance(self.args[0], MiniNumber) or not isinstance(self.args[0].value, (int, long)) or self.args[0].value <= 0:
                raise PrettyPfaException("fixed type first argument should be a positive integer, not {}, at {}".format(self.args[0], self.pos))
            size = self.args[0].value

            if len(self.args) == 2:
                if not isinstance(self.args[1], MiniDotName):
                    raise PrettyPfaException("fixed type second argument should be an identifier, not \"{}\", at {}".format(type(self.args[1]), self.pos))
                namespace, name = split(self.args[1].name)
            else:
                namespace, name = None, titus.util.uniqueFixedName()

            out = {"type": "fixed", "size": size, "name": name}
            if namespace is not None:
                out["namespace"] = namespace
            return out

        elif self.name == "enum":
            if len(self.args) < 1 or len(self.args) > 2:
                raise PrettyPfaException("enum type should have 1 or 2 arguments, not {} at {}".format(len(self.args), self.pos))

            if not isinstance(self.args[0], MiniBracketedArgs) or any(not isinstance(x, MiniDotName) for x in self.args[0].args):
                raise PrettyPfaException("enum type first argument should be a bracketed list of symbols, not \"{}\", at {}".format(type(self.args[0]), self.pos))
            symbols = [x.name for x in self.args[0].args]

            if len(self.args) == 2:
                if not isinstance(self.args[1], MiniDotName):
                    raise PrettyPfaException("fixed type second argument should be an identifier, not \"{}\", at {}".format(type(self.args[1]), self.pos))
                namespace, name = split(self.args[1].name)
            else:
                namespace, name = None, titus.util.uniqueFixedName()

            out = {"type": "enum", "symbols": symbols, "name": name}
            if namespace is not None:
                out["namespace"] = namespace
            return out

        elif self.name == "record":
            fields = []
            others = []
            for arg in self.args:
                if isinstance(arg, MiniParam):
                    fields.append({"name": arg.name, "type": arg.typeExpr.asType(state)})
                else:
                    others.append(arg)

            if len(others) == 0:
                namespace, name = None, titus.util.uniqueRecordName()
            elif len(others) == 1:
                namespace, name = split(others[0].name)
            else:
                raise PrettyPfaException("apart from the field specifiers (which have colons), record type takes 0 or 1 argument, not {} at {}".format(len(others), self.pos))

            out = {"type": "record", "name": name, "fields": fields}
            if namespace is not None:
                out["namespace"] = namespace
            return out

        else:
            raise PrettyPfaException("unrecognized type function \"{}\" at {}".format(self.name, self.pos))

    def defType(self, state):
        if self.name in ("record", "enum", "fixed"):
            jsonNode = self.asType(state)
            alias = jsonNode["name"]
            if "namespace" in jsonNode:
                alias = jsonNode["namespace"] + "." + alias
            state.avroTypeAlias[alias] = jsonNode
            state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(jsonNode))
        else:
            super(MiniCall, self).defType(state)

class MiniBlock(MiniAst):
    def __init__(self, exprs, low, high):
        self.exprs = exprs
        super(MiniBlock, self).__init__(low, high)
    def __repr__(self):
        return "MiniBlock({})".format(", ".join(repr(x) for x in self.exprs))
    def asJson(self):
        out = {}
        for expr in self.exprs:
            if not isinstance(expr, MiniParam):
                raise PrettyPfaException("JSON object must contain only key-value pairs at {}".format(self.pos))
            out[expr.name] = expr.typeExpr.asJson()
        return out

class MiniBracketedArgs(MiniAst):
    def __init__(self, args, low, high):
        self.args = args
        super(MiniBracketedArgs, self).__init__(low, high)
    def __repr__(self):
        return "MiniBracketedArgs({})".format(", ".join([repr(x) for x in self.args]))
    def asJson(self):
        return [x.asJson() for x in self.args]

class MiniParam(MiniAst):
    def __init__(self, name, typeExpr, low, high):
        self.name = name
        self.typeExpr = typeExpr
        super(MiniParam, self).__init__(low, high)
    def __repr__(self):
        return "MiniParam({}, {})".format(self.name, repr(self.typeExpr))
    def asExpr(self, state):
        return {self.name: state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.typeExpr.asType(state)))}

class MiniFcnDef(MiniAst):
    def __init__(self, parameters, retType, definition, low, high):
        self.parameters = parameters
        self.retType = retType
        self.definition = definition
        super(MiniFcnDef, self).__init__(low, high)
    def __repr__(self):
        return "MiniFcnDef({}, {}, {})".format(", ".join(map(repr, self.parameters)), repr(self.retType), ", ".join(map(repr, self.definition)))
    def asExpr(self, state):
        return FcnDef([x.asExpr(state) for x in self.parameters],
                      state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.retType.asType(state))),
                      [x.asExpr(state) for x in self.definition])

class MiniNamedFcnDef(MiniAst):
    def __init__(self, name, parameters, retType, definition, low, high):
        self.name = name
        self.parameters = parameters
        self.retType = retType
        self.definition = definition
        super(MiniNamedFcnDef, self).__init__(low, high)
    def __repr__(self):
        return "MiniNamedFcnDef({}, {}, {}, {})".format(self.name, ", ".join(map(repr, self.parameters)), repr(self.retType), ", ".join(map(repr, self.definition)))
    def asFcn(self, state):
        return (self.name, FcnDef([x.asExpr(state) for x in self.parameters],
                                  state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.retType.asType(state))),
                                  [x.asExpr(state) for x in self.definition]))

class MiniCellPool(MiniAst):
    def __init__(self, name, objType, init, shared, rollback, low, high):
        self.name = name
        self.objType = objType
        self.init = init
        self.shared = shared
        self.rollback = rollback
        super(MiniCellPool, self).__init__(low, high)
    def __repr__(self):
        return "MiniCellPool({}, {}, {}, {}, {})".format(self.name, self.objType, self.init, self.shared, self.rollback)
    def asCell(self, state):
        return (self.name, Cell(state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.objType.asType(state))),
                                jsonlib.dumps(self.init.asJson()),
                                self.shared,
                                self.rollback,
                                self.pos))
    def asPool(self, state):
        init = self.init.asJson()
        if not isinstance(init, dict):
            raise PrettyPfaException("pool's init must be a JSON object, not {}, at {}".format(init, self.pos))
        return (self.name, Pool(state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.objType.asType(state))),
                                dict((k, jsonlib.dumps(v)) for k, v in init.items()),
                                self.shared,
                                self.rollback,
                                self.pos))

class MiniIf(MiniAst):
    def __init__(self, pairs, elseClause, low, high):
        self.pairs = pairs
        self.elseClause = elseClause
        super(MiniIf, self).__init__(low, high)
    def __repr__(self):
        return "MiniIf({}, {})".format(self.pairs, self.elseClause)
    def asExpr(self, state):
        pairs = []
        for predicate, thenClause in self.pairs:
            if isinstance(thenClause, MiniCall) and thenClause.name == "do":
                thenClause = [x.asExpr(state) for x in thenClause.args]
            elif isinstance(thenClause, (list, tuple)):
                thenClause = [x.asExpr(state) for x in thenClause]
            else:
                thenClause = [thenClause.asExpr(state)]
            pairs.append(If(predicate.asExpr(state), thenClause, None, self.pos))

        if self.elseClause is None:
            elseClause = None
        elif isinstance(self.elseClause, MiniCall) and self.elseClause.name == "do":
            elseClause = [x.asExpr(state) for x in self.elseClause.args]
        elif isinstance(self.elseClause, (list, tuple)):
            elseClause = [x.asExpr(state) for x in self.elseClause]
        else:
            elseClause = [self.elseClause.asExpr(state)]

        if len(pairs) == 1:
            pairs[0].elseClause = elseClause
            return pairs[0]
        else:
            return Cond(pairs, elseClause, self.pos)

class MiniAsBlock(MiniAst):
    def __init__(self, astype, named, body, low, high):
        self.astype = astype
        self.named = named
        self.body = body
        super(MiniAsBlock, self).__init__(low, high)
    def __repr__(self):
        return "MiniAsBlock({}, {}, {})".format(self.astype, self.named, self.body)
    def asExpr(self, state):
        t = state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(self.astype.asType(state)))
        if isinstance(self.body, MiniCall) and self.body.name == "do":
            body = [x.asExpr(state) for x in self.body.args]
        elif isinstance(self.body, (list, tuple)):
            body = [x.asExpr(state) for x in self.body]
        else:
            body = [self.body.asExpr(state)]
        return CastCase(t, self.named, body, self.pos)

class MiniCast(MiniAst):
    def __init__(self, expression, asblocks, partial, low, high):
        self.expression = expression
        self.asblocks = asblocks
        self.partial = partial
        super(MiniCast, self).__init__(low, high)
    def __repr__(self):
        return "MiniCast({}, {}, {})".format(self.expression, self.asblocks, self.partial)
    def asExpr(self, state):
        return CastBlock(self.expression.asExpr(state), [x.asExpr(state) for x in self.asblocks], self.partial, self.pos)

class MiniIfNotNull(MiniAst):
    def __init__(self, params, thenClause, elseClause, low, high):
        self.params = params
        self.thenClause = thenClause
        self.elseClause = elseClause
        super(MiniIfNotNull, self).__init__(low, high)
    def __repr__(self):
        return "MiniIfNotNull({}, {}, {})".format(self.params, self.thenClause, self.elseClause)
    def asExpr(self, state):
        params = dict((x.name, x.typeExpr.asExpr(state)) for x in self.params)

        if isinstance(self.thenClause, MiniCall) and self.thenClause.name == "do":
            thenClause = [x.asExpr(state) for x in self.thenClause.args]
        elif isinstance(self.thenClause, (list, tuple)):
            thenClause = [x.asExpr(state) for x in self.thenClause]
        else:
            thenClause = [self.thenClause.asExpr(state)]

        if self.elseClause is None:
            elseClause = None
        elif isinstance(self.elseClause, MiniCall) and self.elseClause.name == "do":
            elseClause = [x.asExpr(state) for x in self.elseClause.args]
        elif isinstance(self.elseClause, (list, tuple)):
            elseClause = [x.asExpr(state) for x in self.elseClause]
        else:
            elseClause = [self.elseClause.asExpr(state)]

        return IfNotNull(params, thenClause, elseClause, self.pos)
        
class MiniWhile(MiniAst):
    def __init__(self, predicate, body, pretest, low, high):
        self.predicate = predicate
        self.body = body
        self.pretest = pretest
        super(MiniWhile, self).__init__(low, high)
    def __repr__(self):
        return "MiniWhile({}, {}, {})".format(self.predicate, self.body, self.pretest)
    def asExpr(self, state):
        if isinstance(self.body, MiniCall) and self.body.name == "do":
            body = [x.asExpr(state) for x in self.body.args]
        elif isinstance(self.body, (list, tuple)):
            body = [x.asExpr(state) for x in self.body]
        else:
            body = [self.body.asExpr(state)]
        if self.pretest:
            return While(self.predicate.asExpr(state), body, self.pos)
        else:
            return DoUntil(body, self.predicate.asExpr(state), self.pos)

class MiniFor(MiniAst):
    def __init__(self, init, predicate, step, body, low, high):
        self.init = init
        self.predicate = predicate
        self.step = step
        self.body = body
        super(MiniFor, self).__init__(low, high)
    def __repr__(self):
        return "MiniFor({}, {}, {}, {})".format(self.init, self.predicate, self.step, self.body)
    def asExpr(self, state):
        if isinstance(self.body, MiniCall) and self.body.name == "do":
            body = [x.asExpr(state) for x in self.body.args]
        elif isinstance(self.body, (list, tuple)):
            body = [x.asExpr(state) for x in self.body]
        else:
            body = [self.body.asExpr(state)]

        return For(dict((k, v.asExpr(state)) for k, v in self.init.pairs.items()),
                   self.predicate.asExpr(state),
                   dict((k, v.asExpr(state)) for k, v in self.step.pairs.items()),
                   body)

class MiniForeach(MiniAst):
    def __init__(self, name, array, body, seq, low, high):
        self.name = name
        self.array = array
        self.body = body
        self.seq = seq
        super(MiniForeach, self).__init__(low, high)
    def __repr__(self):
        return "MiniForeach({}, {}, {})".format(self.name, self.array, self.body, self.seq)
    def asExpr(self, state):
        if isinstance(self.body, MiniCall) and self.body.name == "do":
            body = [x.asExpr(state) for x in self.body.args]
        elif isinstance(self.body, (list, tuple)):
            body = [x.asExpr(state) for x in self.body]
        else:
            body = [self.body.asExpr(state)]

        if isinstance(self.name, (list, tuple)):
            forkey, forval = self.name
            return Forkeyval(forkey, forval, self.array.asExpr(state), body, self.pos)
        else:
            return Foreach(self.name, self.array.asExpr(state), body, self.seq, self.pos)

class MiniTry(MiniAst):
    def __init__(self, body, filters, low, high):
        self.body = body
        self.filters = filters
        super(MiniTry, self).__init__(low, high)
    def __repr__(self):
        return "MiniTry({}, {})".format(self.body, self.filters)
    def asExpr(self, state):
        if self.filters is None:
            filters = None
        else:
            if any(not isinstance(x, (MiniDotName, MiniString)) for x in self.filters):
                raise PrettyPfaException("try filters must all be strings, not {}, at {}".format(self.filters, self.pos))
            filters = [x.name if isinstance(x, MiniDotName) else x.value for x in self.filters]

        if isinstance(self.body, MiniCall) and self.body.name == "do":
            body = [x.asExpr(state) for x in self.body.args]
        elif isinstance(self.body, (list, tuple)):
            body = [x.asExpr(state) for x in self.body]
        else:
            body = [self.body.asExpr(state)]

        return Try(body, filters, self.pos)

class MiniGenAssignment(MiniAst):
    def __init__(self, expr, args, rhs, low, high):
        self.expr = expr
        self.args = args
        self.rhs = rhs
        super(MiniGenAssignment, self).__init__(low, high)
    def __repr__(self):
        return "MiniGenAssignment({}, {}, {})".format(self.expr, self.args, self.rhs)
    def asExpr(self, state):
        return AttrTo(self.expr.asExpr(state), [x.asExpr(state) for x in self.args], self.rhs.asExpr(state), self.pos)

class MiniAssignment(MiniAst):
    def __init__(self, pairs, qualifier, low, high):
        self.pairs = pairs
        self.qualifier = qualifier
        super(MiniAssignment, self).__init__(low, high)
    def __repr__(self):
        return "MiniAssignment({}, {})".format(self.pairs, self.qualifier)
    def asExpr(self, state):
        if self.qualifier is None and len(self.pairs) == 1 and "." in self.pairs.keys()[0]:
            pieces = self.pairs.keys()[0].split(".")
            base = pieces[0]
            path = [LiteralString(x, self.pos) for x in pieces[1:]]

            to = self.pairs.values()[0].asExpr(state)
            if isinstance(to, FcnRef):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to.name, to.pos))
            elif isinstance(to, FcnDef):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to, to.pos))

            if base in state.cellNames:
                return CellTo(base, path, to, self.pos)
            elif base in state.poolNames:
                return PoolTo(base, path, to, self.pairs.values()[0].asExpr(state), self.pos)
            else:
                return AttrTo(Ref(base, self.pos), path, to, self.pos)

        elif self.qualifier is None and len(self.pairs) == 1:
            (name, to), = self.pairs.items()
            
            to = to.asExpr(state)
            if isinstance(to, FcnRef):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to.name, to.pos))
            elif isinstance(to, FcnDef):
                raise PrettyPfaException("direct assignments (with an = sign) cannot refer to functions, such as {} at {}".format(to, to.pos))

            if name in state.cellNames:
                return CellTo(name, [], to, self.pos)
            elif name in state.poolNames:
                return PoolTo(name, [], to, self.pairs.values()[0].asExpr(state), self.pos)
            else:
                return SetVar({name: to}, self.pos)
            
        if any("." in x for x in self.pairs.keys()):
            raise PrettyPfaException("cannot assign multiple deep objects (name contains dots) at the same time; separate with semicolons, rather than commas, at {}".format(self.pos))

        if self.qualifier == "var":
            return Let(dict((k, v.asExpr(state)) for k, v in self.pairs.items()), self.pos)
        else:
            return SetVar(dict((k, v.asExpr(state)) for k, v in self.pairs.items()), self.pos)

    def defType(self, state):
        if self.qualifier is None and len(self.pairs) == 1:
            (alias, typeExpr), = self.pairs.items()
            jsonNode = typeExpr.asType(state)
            state.avroTypeAlias[alias] = jsonNode
            state.avroTypeBuilder.makePlaceholder(jsonlib.dumps(jsonNode))
        else:
            super(MiniAssignment, self).defType(state)

class Parser(object):
    def __init__(self):
        self.initialized = False

    def initialize(self, lex, yacc):
        tokens = ["SECTION_HEADER_START", "SECTION_HEADER",
                  "NUMBER", "STRING", "RAWSTRING", "DOTNAME",
                  "LPAREN", "RPAREN", "LBRACKET", "RBRACKET", "LCURLY", "RCURLY", "RARROW", 
                  "PLUS", "MINUS", "TIMES", "FDIV", "MOD", "REM", "POW", "EQ", "NE", "LT", "LE", "GT", "GE", "AND", "OR", "XOR", "NOT", "BITAND", "BITOR", "BITXOR", "BITNOT"]

        reserved = {"idiv": "IDIV",
                    "fcn": "FCN",
                    "if": "IF",
                    "else": "ELSE",
                    "while": "WHILE",
                    "do": "DO",
                    "until": "UNTIL",
                    "for": "FOR",
                    "foreach": "FOREACH",
                    "cast": "CAST",
                    "as": "AS",
                    "ifnotnull": "IFNOTNULL",
                    "try": "TRY",
                    "var": "VAR",
                    "to": "TO",
                    "init": "INIT"}
        tokens += reserved.values()

        literals = [",", ";", ":", "=", "@"]

        def insertArrow(lines, which):
            lines[which] = lines[which] + "     <----"
            return lines

        def t_COMMENT(t):
            r"//.*"
            pass

        def t_SECTION_HEADER_START(t):
            r"^[a-z]+:"
            t.value = Token("SECTION_HEADER_START", t.value[:-1], t.lexer.lineno)
            return t

        def t_SECTION_HEADER(t):
            r"(\r\n?|\n)+[a-z]+:"
            t.lexer.lineno += len(re.findall(r"(\r\n?|\n)", t.value))
            t.value = Token("SECTION_HEADER", t.value.lstrip("\n")[:-1], t.lexer.lineno)
            return t

        def t_DOTNAME(t):
            r"[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*)*"
            if t.value in reserved:
                t.type = reserved[t.value]
                t.value = Token(t.type, None, t.lexer.lineno)
            else:
                t.value = Token("DOTNAME", t.value, t.lexer.lineno)
            return t

        def t_NUMBER(t):
            r"-?(0|[1-9][0-9]*)(\.[0-9]*)?([eE][-+]?[0-9]+)?"
            try:
                t.value = Token("NUMBER", int(t.value), t.lexer.lineno)
            except ValueError:
                t.value = Token("NUMBER", float(t.value), t.lexer.lineno)
            return t

        def t_STRING(t):
            r'\"([^\\\n]|(\\.))*?\"'
            t.value = Token("STRING", pythonast.literal_eval(t.value), t.lexer.lineno)
            return t

        def t_RAWSTRING(t):
            r"'[^']*'"
            t.value = Token("RAWSTRING", t.value[1:-1], t.lexer.lineno)
            return t

        def t_RARROW(t):
            r"->"
            t.value = Token("RARROW", None, t.lexer.lineno)
            return t

        def t_LPAREN(t):
            r"\("
            t.value = Token("LPAREN", None, t.lexer.lineno)
            return t

        def t_RPAREN(t):
            r"\)"
            t.value = Token("RPAREN", None, t.lexer.lineno)
            return t

        def t_LBRACKET(t):
            r"\["
            t.value = Token("LBRACKET", None, t.lexer.lineno)
            return t

        def t_RBRACKET(t):
            r"\]"
            t.value = Token("RBRACKET", None, t.lexer.lineno)
            return t

        def t_LCURLY(t):
            r"\{"
            t.value = Token("LCURLY", None, t.lexer.lineno)
            return t

        def t_RCURLY(t):
            r"\}"
            t.value = Token("RCURLY", None, t.lexer.lineno)
            return t

        def t_PLUS(t):
            r"\+"
            t.value = Token("PLUS", None, t.lexer.lineno)
            return t

        def t_MINUS(t):
            r"-"
            t.value = Token("MINUS", None, t.lexer.lineno)
            return t

        def t_POW(t):
            r"\*\*"
            t.value = Token("POW", None, t.lexer.lineno)
            return t

        def t_TIMES(t):
            r"\*"
            t.value = Token("TIMES", None, t.lexer.lineno)
            return t

        def t_FDIV(t):
            r"/"
            t.value = Token("FDIV", None, t.lexer.lineno)
            return t

        def t_REM(t):
            r"%%"
            t.value = Token("REM", None, t.lexer.lineno)
            return t

        def t_MOD(t):
            r"%"
            t.value = Token("MOD", None, t.lexer.lineno)
            return t

        def t_EQ(t):
            r"=="
            t.value = Token("EQ", None, t.lexer.lineno)
            return t

        def t_NE(t):
            r"!="
            t.value = Token("NE", None, t.lexer.lineno)
            return t

        def t_LE(t):
            r"<="
            t.value = Token("LE", None, t.lexer.lineno)
            return t

        def t_LT(t):
            r"<"
            t.value = Token("LT", None, t.lexer.lineno)
            return t

        def t_GE(t):
            r">="
            t.value = Token("GE", None, t.lexer.lineno)
            return t

        def t_GT(t):
            r">"
            t.value = Token("GT", None, t.lexer.lineno)
            return t

        def t_AND(t):
            r"&&"
            t.value = Token("AND", None, t.lexer.lineno)
            return t

        def t_OR(t):
            r"\|\|"
            t.value = Token("OR", None, t.lexer.lineno)
            return t

        def t_XOR(t):
            r"\^\^"
            t.value = Token("XOR", None, t.lexer.lineno)
            return t

        def t_NOT(t):
            r"!"
            t.value = Token("NOT", None, t.lexer.lineno)
            return t

        def t_BITAND(t):
            r"&"
            t.value = Token("BITAND", None, t.lexer.lineno)
            return t

        def t_BITOR(t):
            r"\|"
            t.value = Token("BITOR", None, t.lexer.lineno)
            return t

        def t_BITXOR(t):
            r"\^"
            t.value = Token("BITXOR", None, t.lexer.lineno)
            return t

        def t_BITNOT(t):
            r"~"
            t.value = Token("BITNOT", None, t.lexer.lineno)
            return t

        t_ignore = " \t"

        def t_newline(t):
            r"(\r\n?|\n)+"
            t.lexer.lineno += len(re.findall(r"(\r\n?|\n)", t.value))

        def t_error(t):
            lineno = t.lexer.lineno
            offendingLine = "\n".join(insertArrow(self.text.split("\n")[(lineno - 1):(lineno + 2)], 1))
            raise PrettyPfaException("Tokenizing syntax error: \"{}\" at PrettyPFA line {}:\n{}".format(t.value[0], lineno, offendingLine))

        self.lexer = lex.lex()

        def p_document(p):
            r'''document : sections'''

            sectionDict = p[1]
            keys = set(sectionDict.keys())

            _name = None
            _method = Method.MAP
            _input = None
            _output = None
            _begin = []
            _action = []
            _end = []
            _fcns = {}
            _zero = None
            _merge = None
            _cells = {}
            _pools = {}
            _randseed = None
            _doc = None
            _version = None
            _metadata = {}
            _options = {}

            if "name" in keys:
                _name = sectionDict["name"].content.name
            else:
                _name = titus.util.uniqueEngineName()

            if "method" in keys:
                _method = sectionDict["method"].method()

            state = InterpretationState()

            if "types" in keys:
                sectionDict["types"].types(state)

            if "input" in keys:
                _input = sectionDict["input"].input(state)
            if "output" in keys:
                _output = sectionDict["output"].output(state)
            if "cells" in keys:
                _cells = sectionDict["cells"].cells(state)
            if "pools" in keys:
                _pools = sectionDict["pools"].pools(state)

            if "fcns" in keys:
                for x in sectionDict["fcns"].content:
                    if isinstance(x, MiniNamedFcnDef):
                        state.functionNames.add("u." + x.name)
            state.cellNames = set(_cells.keys())
            state.poolNames = set(_pools.keys())

            if "begin" in keys:
                _begin = sectionDict["begin"].begin(state)
            if "action" in keys:
                _action = sectionDict["action"].action(state)
            if "end" in keys:
                _end = sectionDict["end"].end(state)
            if "fcns" in keys:
                _fcns = sectionDict["fcns"].fcns(state)

            if "zero" in keys:
                _zero = sectionDict["zero"].zero()
            if "merge" in keys:
                _merge = sectionDict["merge"].merge(state)
            if "randseed" in keys:
                _randseed = sectionDict["randseed"].randseed()
            if "doc" in keys:
                _doc = sectionDict["doc"].doc()
            if "version" in keys:
                _version = sectionDict["version"].version()
            if "metadata" in keys:
                _metadata = sectionDict["metadata"].metadata()
            if "options" in keys:
                _options = sectionDict["options"].options()

            required = set(["action", "input", "output"])
            if keys.intersection(required) != required:
                raise PFASyntaxException("missing top-level fields: {}".format(", ".join(required.difference(keys))), "PrettyPFA document")
            else:
                p[0] = EngineConfig(_name, _method, _input, _output, _begin, _action, _end, _fcns, _zero, _merge, _cells, _pools, _randseed, _doc, _version, _metadata, _options, "PrettyPFA document")
                state.avroTypeBuilder.resolveTypes()

        def p_section(p):
            r'''section : SECTION_HEADER_START anything
                        | SECTION_HEADER anything'''
            p[0] = Section(p[1].v, p[2], p[1])

        def p_sections(p):
            r'''sections : section
                         | sections section'''
            if len(p) == 2:
                p[0] = {p[1].name: p[1]}
            else:
                p[1].update({p[2].name: p[2]})
                p[0] = p[1]

        def p_anything(p):
            r'''anything : expression
                         | expressions
                         | bracketedargs
                         | namedfcndefs
                         | cellpools'''
            p[0] = p[1]

        def p_expressions(p):
            r'''expressions : expression
                            | expressions ";" expression'''
            if len(p) == 2:
                p[0] = [p[1]]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_expressions_extrasemi(p):
            r'''expressions : expressions ";"'''
            p[0] = p[1]

        def p_block(p):
            r'''block : LCURLY expressions RCURLY
                      | LCURLY parameters RCURLY'''
            lcurly, exprs, rcurly = p[1:]
            p[0] = MiniBlock(exprs, lcurly.lineno, rcurly.lineno)

        def p_bracketedargs(p):
            r'''bracketedargs : LBRACKET arguments RBRACKET'''
            lbracket, args, rbracket = p[1:]
            p[0] = MiniBracketedArgs(args, lbracket.lineno, rbracket.lineno)

        def p_argument(p):
            r'''argument : expression
                         | parameter
                         | bracketedargs
                         | fcndef'''
            p[0] = p[1]

        def p_arguments(p):
            r'''arguments : empty
                          | argument
                          | arguments "," argument'''
            if len(p) == 2:
                if p[1] is None:
                    p[0] = []
                else:
                    p[0] = [p[1]]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_parameters(p):
            r'''parameters : empty
                           | parameter
                           | parameters "," parameter'''
            if len(p) == 2:
                if p[1] is None:
                    p[0] = []
                else:
                    p[0] = [p[1]]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_parameter(p):
            r'''parameter : DOTNAME ":" argument
                          | STRING ":" argument
                          | RAWSTRING ":" argument'''
            p[0] = MiniParam(p[1].v, p[3], p[1].lineno, p[3].high)

        precedence = [("left", "OR"),
                      ("left", "XOR"),
                      ("left", "AND"),
                      ("right", "NOT"),
                      ("nonassoc", "EQ", "NE", "LT", "LE", "GT", "GE"),
                      ("left", "BITOR"),
                      ("left", "BITXOR"),
                      ("left", "BITAND"),
                      ("left", "PLUS", "MINUS"),
                      ("left", "TIMES", "FDIV", "IDIV", "MOD", "REM"),
                      ("right", "UMINUS", "BITNOT"),
                      ("left", "POW")]

        def p_expression_call(p):
            r'''expression : DOTNAME LPAREN arguments RPAREN'''
            name, arguments = p[1], p[3]
            p[0] = MiniCall(name.v, arguments, name.lineno, name.lineno)

        def p_expression_uminus(p):
            r'''expression : MINUS expression %prec UMINUS'''
            op, expr = p[1:]
            p[0] = MiniCall("u-", [expr], op.lineno, op.lineno)

        def p_expression_bitnot(p):
            r'''expression : BITNOT expression'''
            op, expr = p[1:]
            p[0] = MiniCall("~", [expr], op.lineno, op.lineno)

        def p_expression_not(p):
            r'''expression : NOT expression'''
            op, expr = p[1:]
            p[0] = MiniCall("!", [expr], op.lineno, op.lineno)

        def p_expression_binop(p):
            r'''expression : expression OR expression
                           | expression XOR expression
                           | expression AND expression
                           | expression EQ expression
                           | expression NE expression
                           | expression LT expression
                           | expression LE expression
                           | expression GT expression
                           | expression GE expression
                           | expression BITOR expression
                           | expression BITXOR expression
                           | expression BITAND expression
                           | expression PLUS expression
                           | expression MINUS expression
                           | expression TIMES expression
                           | expression FDIV expression
                           | expression IDIV expression
                           | expression MOD expression
                           | expression REM expression
                           | expression POW expression'''
            left, op, right = p[1:]
            if op.t == "OR":
                p[0] = MiniCall("||", [left, right], None, None)
            elif op.t == "XOR":
                p[0] = MiniCall("^^", [left, right], None, None)
            elif op.t == "AND":
                p[0] = MiniCall("&&", [left, right], None, None)
            elif op.t == "EQ":
                p[0] = MiniCall("==", [left, right], None, None)
            elif op.t == "NE":
                p[0] = MiniCall("!=", [left, right], None, None)
            elif op.t == "LT":
                p[0] = MiniCall("<", [left, right], None, None)
            elif op.t == "LE":
                p[0] = MiniCall("<=", [left, right], None, None)
            elif op.t == "GT":
                p[0] = MiniCall(">", [left, right], None, None)
            elif op.t == "GE":
                p[0] = MiniCall(">=", [left, right], None, None)
            elif op.t == "BITOR":
                p[0] = MiniCall("|", [left, right], None, None)
            elif op.t == "BITXOR":
                p[0] = MiniCall("^", [left, right], None, None)
            elif op.t == "BITAND":
                p[0] = MiniCall("&", [left, right], None, None)
            elif op.t == "PLUS":
                p[0] = MiniCall("+", [left, right], None, None)
            elif op.t == "MINUS":
                p[0] = MiniCall("-", [left, right], None, None)
            elif op.t == "TIMES":
                p[0] = MiniCall("*", [left, right], None, None)
            elif op.t == "FDIV":
                p[0] = MiniCall("/", [left, right], None, None)
            elif op.t == "IDIV":
                p[0] = MiniCall("//", [left, right], None, None)
            elif op.t == "MOD":
                p[0] = MiniCall("%", [left, right], None, None)
            elif op.t == "REM":
                p[0] = MiniCall("%%", [left, right], None, None)
            elif op.t == "POW":
                p[0] = MiniCall("**", [left, right], None, None)

        def p_expression_group(p):
            r'''expression : LPAREN expression RPAREN'''
            p[0] = p[2]

        def p_rettype(p):
            r'''rettype : RARROW expression'''
            p[0] = p[2]

        def p_fcndef(p):
            r'''fcndef : FCN LPAREN parameters rettype RPAREN expression
                       | FCN LPAREN parameters rettype RPAREN block'''
            parameters = p[3]
            retType = p[4]
            definition = p[6]
            if isinstance(definition, MiniBlock):
                definition = definition.exprs
            else:
                definition = [definition]
            if isinstance(p[6], (list, tuple)):
                if len(p[6]) == 0:
                    high = p[5].lineno
                else:
                    high = max(x.high for x in p[6])
            else:
                high = p[6].high
            p[0] = MiniFcnDef(parameters, retType, definition, p[2].lineno, high)

        def p_namedfcndef(p):
            r'''namedfcndef : DOTNAME "=" FCN LPAREN parameters rettype RPAREN expression
                            | DOTNAME "=" FCN LPAREN parameters rettype RPAREN block'''
            parameters = p[5]
            retType = p[6]
            definition = p[8]
            if isinstance(definition, MiniBlock):
                definition = definition.exprs
            else:
                definition = [definition]
            if isinstance(p[8], (list, tuple)):
                if len(p[8]) == 0:
                    high = p[7].lineno
                else:
                    high = max(x.high for x in p[8])
            else:
                high = p[8].high
            p[0] = MiniNamedFcnDef(p[1].v, parameters, retType, definition, p[1].lineno, high)

        def p_namedfcndefs(p):
            r'''namedfcndefs : empty
                             | namedfcndef
                             | namedfcndefs ";" namedfcndef'''
            if len(p) == 2:
                if p[1] is None:
                    p[0] = []
                else:
                    p[0] = [p[1]]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_namedfcndefs_extrasemi(p):
            r'''namedfcndefs : namedfcndefs ";"'''
            p[0] = p[1]

        def p_cellpool(p):
            r'''cellpool : DOTNAME LPAREN arguments RPAREN "=" block
                         | DOTNAME LPAREN arguments RPAREN "=" bracketedargs
                         | DOTNAME LPAREN arguments RPAREN "=" NUMBER
                         | DOTNAME LPAREN arguments RPAREN "=" STRING
                         | DOTNAME LPAREN arguments RPAREN "=" RAWSTRING
                         | DOTNAME LPAREN arguments RPAREN "=" DOTNAME'''
            name = p[1].v
            token = p[6]
            if isinstance(token, Token):
                if token.t == "NUMBER":
                    init = MiniNumber(token.v, token.lineno)
                elif token.t == "STRING":
                    init = MiniString(token.v, token.lineno)
                elif token.t == "RAWSTRING":
                    init = MiniString(token.v, token.lineno)
                elif token.t == "DOTNAME":
                    init = MiniDotName(token.v, token.lineno)
                high = token.lineno
            else:
                init = token
                high = token.high
          
            shared = False
            rollback = False
            objType = None
            others = []
            for param in p[3]:
                if isinstance(param, MiniParam):
                    if param.name == "shared":
                        if param.typeExpr == MiniDotName("true", None):
                            shared = True
                        elif param.typeExpr == MiniDotName("false", None):
                            shared = False
                        else:
                            raise PrettyPfaException("cell/pool parameter \"shared\" must be boolean, not {}, at {}".format(param.typeExpr, param.pos))
                    elif param.name == "rollback":
                        if param.typeExpr == MiniDotName("true", None):
                            rollback = True
                        elif param.typeExpr == MiniDotName("false", None):
                            rollback = False
                        else:
                            raise PrettyPfaException("cell/pool parameter \"rollback\" must be true or false, not {}, at {}".format(param.typeExpr, param.pos))
                    elif param.name == "type":
                        objType = param.typeExpr
                    else:
                        raise PrettyPfaException("only \"type\", \"shared\", and \"rollback\" are recognized as cell/pool parameters")
                else:
                    others.append(param)

            if objType is None and len(others) == 1:
                objType = others[0]
            elif objType is not None and len(others) == 0:
                pass
            else:
                raise PrettyPfaException("only one unnamed parameter, the type, can be provided in a cell/pool declaration, and only if it is not also provided as a named parameter at PrettyPFA line {}".format(p[1].lineno))

            p[0] = MiniCellPool(name, objType, init, shared, rollback, p[1].lineno, high)

        def p_cellpools(p):
            r'''cellpools : cellpool
                          | cellpools ";" cellpool'''
            if len(p) == 2:
                p[0] = [p[1]]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_cellpools_extrasemi(p):
            r'''cellpools : cellpools ";"'''
            p[0] = p[1]

        def p_ifthen(p):
            r'''expression : IF LPAREN expression RPAREN expression
                           | IF LPAREN expression RPAREN expression ELSE expression'''
            predicate = p[3]
            thenClause = p[5]
            if len(p) == 8:
                elseClause = p[7]
                high = elseClause.high
            else:
                elseClause = None
                high = thenClause.high

            if isinstance(elseClause, MiniIf):
                p[0] = MiniIf([(predicate, thenClause)] + elseClause.pairs, elseClause.elseClause, p[1].lineno, high)
            else:
                p[0] = MiniIf([(predicate, thenClause)], elseClause, p[1].lineno, high)

        def p_whileloop(p):
            r'''expression : WHILE LPAREN expression RPAREN expression'''
            predicate = p[3]
            body = p[5]
            p[0] = MiniWhile(predicate, body, True, p[1].lineno, body.high)

        def p_doloop(p):
            r'''expression : DO expression UNTIL LPAREN expression RPAREN'''
            predicate = p[5]
            body = p[2]
            p[0] = MiniWhile(predicate, body, False, p[1].lineno, body.high)

        def p_forloop(p):
            r'''expression : FOR LPAREN assignments ";" expression ";" assignments RPAREN expression'''
            p[0] = MiniFor(p[3], p[5], p[7], p[9], p[2].lineno, p[9].high)

        def p_foreach(p):
            r'''expression : FOREACH LPAREN parameter RPAREN expression
                           | FOREACH LPAREN parameter "," parameter RPAREN expression'''
            name = p[3].name
            array = p[3].typeExpr
            if len(p) > 6:
                if p[5].name != "seq":
                    raise PrettyPfaException("optional second parameter in foreach must be \"seq\", not {}, at {}".format(p[5].name, p[5].pos))
                if p[5].typeExpr == MiniDotName("true", None):
                    seq = True
                elif p[5].typeExpr == MiniDotName("false", None):
                    seq = False
                else:
                    raise PrettyPfaException("optional second parameter in foreach must be boolean, not {}, at {}".format(p[5].typeExpr, p[5].pos))
                body = p[7]
            else:
                body = p[5]
                seq = False
            p[0] = MiniForeach(name, array, body, seq, p[1].lineno, body.high)

        def p_forkeyval(p):
            r'''expression : FOREACH LPAREN STRING "," parameter RPAREN expression
                           | FOREACH LPAREN RAWSTRING "," parameter RPAREN expression
                           | FOREACH LPAREN DOTNAME "," parameter RPAREN expression'''
            forkey = p[3].v
            forval = p[5].name
            container = p[5].typeExpr
            body = p[7]
            p[0] = MiniForeach((forkey, forval), container, body, None, p[1].lineno, body.high)

        def p_asblock(p):
            r'''asblock : AS LPAREN parameter RPAREN expression'''
            p[0] = MiniAsBlock(p[3].typeExpr, p[3].name, p[5], p[1].lineno, p[5].high)

        def p_asblocks(p):
            r'''asblocks : asblock
                         | asblocks asblock'''
            if len(p) == 2:
                p[0] = [p[1]]
            else:
                p[1].append(p[2])
                p[0] = p[1]

        def p_cast(p):
            r'''expression : CAST LPAREN expression RPAREN LCURLY asblocks RCURLY
                           | CAST LPAREN expression "," parameter RPAREN LCURLY asblocks RCURLY'''
            expression = p[3]
            if len(p) > 8:
                if p[5].name != "partial":
                    raise PrettyPfaException("optional second parameter in cast must be \"partial\", not {}, at {}".format(p[5].name, p[5].pos))
                if p[5].typeExpr == MiniDotName("true", None):
                    partial = True
                elif p[5].typeExpr == MiniDotName("false", None):
                    partial = False
                else:
                    raise PrettyPfaException("optional second parameter in cast must be boolean, not {}, at {}".format(p[5].typeExpr, p[5].pos))
                asblocks = p[8]
            else:
                asblocks = p[6]
                partial = False
            p[0] = MiniCast(expression, asblocks, partial, p[1].lineno, asblocks[-1].high)

        def p_ifnotnull(p):
            r'''expression : IFNOTNULL LPAREN parameters RPAREN expression
                           | IFNOTNULL LPAREN parameters RPAREN expression ELSE expression'''
            params = p[3]
            thenClause = p[5]
            if len(p) > 6:
                elseClause = p[7]
                high = elseClause.high
            else:
                elseClause = None
                high = thenClause.high
            p[0] = MiniIfNotNull(params, thenClause, elseClause, p[1].lineno, high)

        def p_try(p):
            r'''expression : TRY expression
                           | TRY LPAREN arguments RPAREN expression'''
            if len(p) > 3:
                filters = p[3]
                body = p[5]
            else:
                filters = None
                body = p[2]
            p[0] = MiniTry(body, filters, p[1].lineno, body.high)

        def p_genassignment(p):
            r'''expression : expression LBRACKET arguments RBRACKET "=" expression'''
            p[0] = MiniGenAssignment(p[1], p[3], p[6], p[1].low, p[6].high)

        def p_assignment(p):
            r'''assignment : DOTNAME "=" expression'''
            p[0] = MiniAssignment({p[1].v: p[3]}, None, p[1].lineno, p[3].high)

        def p_assignments(p):
            r'''assignments : assignment
                            | assignments "," assignment'''
            if len(p) > 2:
                p[1].pairs.update(p[3].pairs)
                p[1].high = p[3].high
            p[0] = p[1]

        def p_assignmentlet(p):
            r'''expression : VAR assignments'''
            p[2].qualifier = "var"
            p[0] = p[2]

        def p_assignmentset(p):
            r'''expression : assignments'''
            p[1].qualifier = None
            p[0] = p[1]

        def p_genextraction(p):
            r'''expression : expression LBRACKET arguments RBRACKET'''
            p[0] = MiniGenGet(p[1], p[3], p[1].low, p[4].lineno)

        def p_extraction(p):
            r'''expression : DOTNAME LBRACKET arguments RBRACKET'''
            p[0] = MiniGet(p[1].v, p[3], p[1].lineno, p[4].lineno)

        def p_deepassignment_1(p):
            r'''expression : DOTNAME LBRACKET arguments RBRACKET "=" expression
                           | DOTNAME LBRACKET arguments RBRACKET TO argument
                           | DOTNAME LBRACKET arguments RBRACKET TO argument INIT expression'''
            if len(p) > 7:
                init = p[8]
                high = p[8].high
            else:
                init = None
                high = p[6].high
            p[0] = MiniTo(p[1].v, p[3], (p[5] == "="), p[6], init, p[1].lineno, high)

        def p_deepassignment_2(p):
            r'''expression : DOTNAME TO argument
                           | DOTNAME TO argument INIT expression'''
            if len(p) > 4:
                init = p[5]
                high = p[5].high
            else:
                init = None
                high = p[3].high
            p[0] = MiniTo(p[1].v, [], False, p[3], init, p[1].lineno, high)

        def p_enumsymbol(p):
            r'''expression : DOTNAME "@" DOTNAME'''
            p[0] = MiniEnumSymbol(p[1].v, p[3].v, p[1].lineno, p[3].lineno)

        def p_expression_simple(p):
            r'''expression : block
                           | NUMBER
                           | STRING
                           | RAWSTRING
                           | DOTNAME'''
            token = p[1]
            if isinstance(token, Token):
                if token.t == "NUMBER":
                    p[0] = MiniNumber(token.v, token.lineno)
                elif token.t == "STRING":
                    p[0] = MiniString(token.v, token.lineno)
                elif token.t == "RAWSTRING":
                    p[0] = MiniString(token.v, token.lineno)
                elif token.t == "DOTNAME":
                    p[0] = MiniDotName(token.v, token.lineno)
            else:
                if all(isinstance(x, MiniParam) for x in token.exprs):
                    p[0] = p[1]
                else:
                    p[0] = MiniCall("do", token.exprs, token.low, token.high)

        def p_empty(p):
            r'''empty : '''

        def p_error(p):
            if p is None:
                raise PrettyPfaException("Parsing syntax error")
            else:
                lineno = p.lineno
                offendingLine = "\n".join(insertArrow(self.text.split("\n")[(lineno - 1):(lineno + 2)], 1))
                raise PrettyPfaException("Parsing syntax error on line {}:\n{}".format(p.lineno, offendingLine))

        self.yacc = yacc.yacc(debug=False, write_tables=False)
        self.initialized = True

    def parse(self, text):
        self.lexer.lineno = 1
        self.text = text
        return self.yacc.parse(text)

parser = Parser()

def ast(text, check=True):
    if not parser.initialized:
        try:
            import ply.lex as lex
            import ply.yacc as yacc
        except ImportError:
            raise ImportError("ply (used to parse the PrettyPFA) is not available on your system")
        else:
            parser.initialize(lex, yacc)

    out = parser.parse(text)
    if check:
        titus.pfaast.check(out)
    return out

def jsonNode(text, lineNumbers=True, check=True):
    return ast(text, check).jsonNode(lineNumbers, set())

def json(text, lineNumbers=True, check=True):
    return ast(text, check).toJson(lineNumbers)

def engine(text, options=None, sharedState=None, multiplicity=1, style="pure", debug=False):
    return PFAEngine.fromAst(ast(text), options, sharedState, multiplicity, style, debug)

def avscToPretty(avsc, indent=0):
    if isinstance(avsc, basestring):
        return " " * indent + avsc
    elif isinstance(avsc, dict) and "type" in avsc:
        tpe = avsc["type"]

        if tpe in ("null", "boolean", "int", "long", "float", "double", "bytes", "string"):
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
            return " " * indent + "fixed(" + name + ", size: " + str(avsc["size"]) + ")"

        elif tpe == "enum":
            name = avsc["name"]
            if "namespace" in avsc and len(avsc["namespace"]) > 0:
                name = avsc["namespace"] + "." + name
            return " " * indent + "enum(" + name + ", symbols: [" + ", ".join(x for x in avsc["symbols"]) + "])"

    elif isinstance(avsc, (list, tuple)):
        variants = []
        for x in avsc:
            variants.append(avscToPretty(x, indent + 6))
        return " " * indent + "union(" + ",\n".join(variants).lstrip() + ")"
