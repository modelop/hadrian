#!/usr/bin/env python

import json
import base64

import titus.util
from titus.util import pos

from titus.ast import validSymbolName
from titus.ast import validFunctionName
from titus.ast import Ast
from titus.ast import Method
from titus.ast import EngineConfig
from titus.ast import Cell
from titus.ast import Pool
from titus.ast import Argument
from titus.ast import Expression
from titus.ast import LiteralValue
from titus.ast import PathIndex
from titus.ast import ArrayIndex
from titus.ast import MapIndex
from titus.ast import RecordIndex
from titus.ast import HasPath
from titus.ast import FcnDef
from titus.ast import FcnRef
from titus.ast import Call
from titus.ast import Ref
from titus.ast import LiteralNull
from titus.ast import LiteralBoolean
from titus.ast import LiteralInt
from titus.ast import LiteralLong
from titus.ast import LiteralFloat
from titus.ast import LiteralDouble
from titus.ast import LiteralString
from titus.ast import LiteralBase64
from titus.ast import Literal
from titus.ast import NewObject
from titus.ast import NewArray
from titus.ast import Do
from titus.ast import Let
from titus.ast import SetVar
from titus.ast import AttrGet
from titus.ast import AttrTo
from titus.ast import CellGet
from titus.ast import CellTo
from titus.ast import PoolGet
from titus.ast import PoolTo
from titus.ast import If
from titus.ast import Cond
from titus.ast import While
from titus.ast import DoUntil
from titus.ast import For
from titus.ast import Foreach
from titus.ast import Forkeyval
from titus.ast import CastCase
from titus.ast import CastBlock
from titus.ast import Upcast
from titus.ast import IfNotNull
from titus.ast import Doc
from titus.ast import Error
from titus.ast import Log
from titus.errors import PFASyntaxException
from titus.datatype import AvroTypeBuilder

def jsonToAst(jsonInput):
    if isinstance(jsonInput, file):
        jsonInput = jsonInput.read()
    if isinstance(jsonInput, basestring):
        jsonInput = json.loads(jsonInput)
    
    avroTypeBuilder = AvroTypeBuilder()
            
    result = _readEngineConfig(jsonInput, avroTypeBuilder)
    avroTypeBuilder.resolveTypes()
    return result

def yamlToAst(yamlInput):
    import yaml

    def read(parser):
        try:
            while True:
                event = parser.next()
        
                if isinstance(event, yaml.ScalarEvent):
                    if not event.implicit[0]:
                        return event.value
                    elif event.value in ("yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON"):
                        return True
                    elif event.value in ("no", "No", "NO", "false", "False", "FALSE", "off", "Off", "OFF"):
                        return False
                    elif event.value in ("null", "Null", "NULL"):
                        return None
                    else:
                        try:
                            return int(event.value)
                        except ValueError:
                            try:
                                return float(event.value)
                            except ValueError:
                                return event.value

                elif isinstance(event, yaml.SequenceStartEvent):
                    out = []
                    while True:
                        item = read(parser)
                        if isinstance(item, yaml.SequenceEndEvent):
                            return out
                        elif isinstance(item, yaml.events.Event):
                            raise PFASyntaxException("malformed YAML", "line {}".format(event.end_mark.line))
                        else:
                            out.append(item)

                elif isinstance(event, yaml.MappingStartEvent):
                    out = {}
                    while True:
                        startLine = event.start_mark.line
                        key = read(parser)
                        if isinstance(key, yaml.MappingEndEvent):
                            endLine = key.end_mark.line
                            if startLine == endLine:
                                out["@"] = "YAML line {}".format(startLine)
                            else:
                                out["@"] = "YAML lines {}-{}".format(startLine, endLine)

                            return out

                        elif isinstance(key, yaml.events.Event):
                            raise PFASyntaxException("malformed YAML", "line {}".format(event.end_mark.line))
                        elif not isinstance(key, basestring):
                            raise PFASyntaxException("YAML keys must be strings, not {}".format(key), "line {}".format(event.end_mark.line))

                        value = read(parser)
                        if isinstance(value, yaml.events.Event):
                            raise PFASyntaxException("malformed YAML", "line {}".format(event.end_mark.line))
                        
                        out[key] = value

                elif isinstance(event, (yaml.SequenceEndEvent, yaml.MappingEndEvent, yaml.DocumentEndEvent, yaml.StreamEndEvent)):
                    return event

        except StopIteration:
            return event

    obj = read(yaml.parse(yamlInput, Loader=yaml.SafeLoader))
    if isinstance(obj, yaml.events.Event):
        raise PFASyntaxException("YAML document does not contain any elements that map to JSON", "")

    return jsonToAst(obj)

def jsonToExpressionAst(jsonInput):
    if isinstance(jsonInput, file):
        jsonInput = jsonInput.read()
    if isinstance(jsonInput, basestring):
        jsonInput = json.loads(jsonInput)

    avroTypeBuilder = AvroTypeBuilder()

    result = _readExpression(jsonInput, "", avroTypeBuilder)
    avroTypeBuilder.resolveTypes()
    return result

jsonToAst.expr = jsonToExpressionAst

def jsonToExpressionsAst(jsonInput):
    if isinstance(jsonInput, file):
        jsonInput = jsonInput.read()
    if isinstance(jsonInput, basestring):
        jsonInput = json.loads(jsonInput)

    avroTypeBuilder = AvroTypeBuilder()

    result = _readExpressionArray(jsonInput, "", avroTypeBuilder)
    avroTypeBuilder.resolveTypes()
    return result

jsonToAst.exprs = jsonToExpressionsAst

def jsonToFcnDef(jsonInput):
    if isinstance(jsonInput, file):
        jsonInput = jsonInput.read()
    if isinstance(jsonInput, basestring):
        jsonInput = json.loads(jsonInput)

    avroTypeBuilder = AvroTypeBuilder()
    
    result = _readFcnDef(jsonInput, "", avroTypeBuilder)
    avroTypeBuilder.resolveTypes()
    return result

jsonToAst.fcn = jsonToFcnDef

def jsonToFcnDefs(jsonInput):
    if isinstance(jsonInput, file):
        jsonInput = jsonInput.read()
    if isinstance(jsonInput, basestring):
        jsonInput = json.loads(jsonInput)

    avroTypeBuilder = AvroTypeBuilder()
    
    result = _readFcnDefMap(jsonInput, "", avroTypeBuilder)
    avroTypeBuilder.resolveTypes()
    return result

jsonToAst.fcns = jsonToFcnDefs

def _trunc(x):
    if len(x) > 30:
        return x[:27] + "..."
    else:
        return x

def _stripAtSigns(data):
    if isinstance(data, dict):
        return dict((k, _stripAtSigns(v)) for k, v in data.items() if k != "@")
    elif isinstance(data, (list, tuple)):
        return [_stripAtSigns(x) for x in data]
    else:
        return data

def _readEngineConfig(data, avroTypeBuilder):
    at = data.get("@")

    if not isinstance(data, dict):
        raise PFASyntaxException("PFA engine must be a JSON object, not " + _trunc(repr(data)), at)

    keys = set(x for x in data.keys() if x != "@")

    _method = Method.MAP
    _begin = []
    _end = []
    _fcns = {}
    _zero = None
    _cells = {}
    _pools = {}
    _randseed = None
    _doc = None
    _metadata = None
    _options = {}

    for key in keys:
        if key == "name": _name = _readString(data[key], key)
        elif key == "method":
            x = _readString(data[key], key)
            if x == "map":
                _method = Method.MAP
            elif x == "emit":
                _method = Method.EMIT
            elif x == "fold":
                _method = Method.FOLD
            else:
                raise PFASyntaxException("expected one of \"map\", \"emit\", \"fold\", not \"{}\"".format(x), at)
        elif key == "input": _input = _readAvroPlaceholder(data[key], key, avroTypeBuilder)
        elif key == "output": _output = _readAvroPlaceholder(data[key], key, avroTypeBuilder)
        elif key == "begin":
            if isinstance(data[key], (list, tuple)):
                _begin = _readExpressionArray(data[key], key, avroTypeBuilder)
            else:
                _begin = [_readExpression(data[key], key, avroTypeBuilder)]
        elif key == "action":
            if isinstance(data[key], (list, tuple)):
                _action = _readExpressionArray(data[key], key, avroTypeBuilder)
            else:
                _action = [_readExpression(data[key], key, avroTypeBuilder)]
        elif key == "end":
            if isinstance(data[key], (list, tuple)):
                _end = _readExpressionArray(data[key], key, avroTypeBuilder)
            else:
                _end = [_readExpression(data[key], key, avroTypeBuilder)]
        elif key == "fcns": _fcns = _readFcnDefMap(data[key], key, avroTypeBuilder)
        elif key == "zero": _zero = _readJsonToString(data[key], key)
        elif key == "cells": _cells = _readCells(data[key], key, avroTypeBuilder)
        elif key == "pools": _pools = _readPools(data[key], key, avroTypeBuilder)
        elif key == "randseed": _randseed = _readLong(data[key], key)
        elif key == "doc": _doc = _readString(data[key], key)
        elif key == "metadata": _metadata = _readJsonNode(data[key], key)
        elif key == "options": _options = _readJsonNodeMap(data[key], key)
        else:
            raise PFASyntaxException("unexpected top-level field: {}".format(key), at)

    if "name" not in keys:
        _name = titus.util.uniqueEngineName()

    if _method == Method.FOLD and "zero" not in keys:
        raise PFASyntaxException("folding engines must include a \"zero\" to begin the calculation", at)

    required = set(["action", "input", "output"])
    if keys.intersection(required) != required:
        raise PFASyntaxException("missing top-level fields: {}".format(", ".join(required.diff(fields))), at)
    else:
        return EngineConfig(_name, _method, _input, _output, _begin, _action, _end, _fcns, _zero, _cells, _pools, _randseed, _doc, _metadata, _options, at)

def _readJsonToString(data, dot):
    return json.dumps(_stripAtSigns(data))

def _readJsonNode(data, dot):
    return _stripAtSigns(data)

def _readAvroPlaceholder(data, dot, avroTypeBuilder):
    return avroTypeBuilder.makePlaceholder(json.dumps(_stripAtSigns(data)))

def _readJsonNodeMap(data, dot):
    if isinstance(data, dict):
        return _stripAtSigns(data)
    else:
        raise PFASyntaxException("expected map of JSON objects, not " + _trunc(repr(data)), dot)

def _readJsonToStringMap(data, dot):
    if isinstance(data, dict):
        at = data.get("@")
        return dict((k, _readJsonToString(v, dot + "." + k)) for k, v in data.items() if k != "@")
    else:
        raise PFASyntaxException("expected map of JSON objects, not " + _trunc(repr(data)), pos(dot, at))

def _readBoolean(data, dot):
    if isinstance(data, bool):
        return data
    else:
        raise PFASyntaxException("expected boolean, not " + _trunc(repr(data)), dot)

def _readInt(data, dot):
    if isinstance(data, int):
        if -2147483648 <= data <= 2147483647:
            return data
        else:
            raise PFASyntaxException("int out of range: {}".format(data), dot)
    else:
        raise PFASyntaxException("expected int, not " + _trunc(repr(data)), dot)

def _readLong(data, dot):
    if isinstance(data, (int, long)):
        if -9223372036854775808 <= data <= 9223372036854775807:
            return data
        else:
            raise PFASyntaxException("long out of range: {}".format(data), dot)
    else:
        raise PFASyntaxException("expected long, not " + _trunc(repr(data)), dot)

def _readFloat(data, dot):
    if isinstance(data, (int, long, float)):
        return float(data)
    else:
        raise PFASyntaxException("expected float, not " + _trunc(repr(data)), dot)

def _readDouble(data, dot):
    if isinstance(data, (int, long, float)):
        return float(data)
    else:
        raise PFASyntaxException("expected double, not " + _trunc(repr(data)), dot)

def _readStringArray(data, dot):
    if isinstance(data, (list, tuple)):
        return [_readString(x, dot + "." + str(i)) for i, x in enumerate(data)]
    else:
        raise PFASyntaxException("expected array of strings, not " + _trunc(repr(data)), dot)

def _readString(data, dot):
    if isinstance(data, basestring):
        return data
    else:
        raise PFASyntaxException("expected string, not " + _trunc(repr(data)), dot)

def _readBase64(data, dot):
    if isinstance(data, basestring):
        return base64.b64decode(data)
    else:
        raise PFASyntaxException("expected base64 data, not " + _trunc(repr(data)), dot)

def _readExpressionArray(data, dot, avroTypeBuilder):
    if isinstance(data, (list, tuple)):
        return [_readExpression(x, dot + "." + str(i), avroTypeBuilder) for i, x in enumerate(data)]
    else:
        raise PFASyntaxException("expected array of expressions, not " + _trunc(repr(data)), dot)

def _readExpressionMap(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        return dict((k, _readExpression(v, dot + "." + k, avroTypeBuilder)) for k, v in data.items() if k != "@")
    else:
        raise PFASyntaxException("expected map of expressions, not " + _trunc(repr(data)), pos(dot, at))

def _readCastCaseArray(data, dot, avroTypeBuilder):
    if isinstance(data, (list, tuple)):
        return [_readCastCase(x, dot + "." + str(i), avroTypeBuilder) for i, x in enumerate(data)]
    else:
        raise PFASyntaxException("expected array of cast-cases, not " + _trunc(repr(data)), dot)

def _readCastCase(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        keys = set(x for x in data.keys() if x != "@")

        for key in keys:
            if key == "as": _as = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "named": _named = _readString(data[key], dot + "." + key)
            elif key == "do":
                if isinstance(data[key], (list, tuple)):
                    _body = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _body = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            else:
                raise PFASyntaxException("unexpected field in cast-case: {}".format(key), pos(dot, at))

    if "named" in keys and not validSymbolName(_named):
        raise PFASyntaxException("\"{}\" is not a valid symbol name".format(_named), pos(dot, at))

    required = set(["as", "named", "do"])
    if (keys != required):
        raise PFASyntaxException("wrong set of fields for a cast-case: \"{}\"".format(", ".join(keys)), pos(dot, at))
    else:
        return CastCase(_as, _named, _body)

def _readExpression(data, dot, avroTypeBuilder):
    out = _readArgument(data, dot, avroTypeBuilder)
    if isinstance(out, Expression):
        return out
    else:
        raise PFASyntaxException("argument appears outside of argument list", dot)

def _readArgumentArray(data, dot, avroTypeBuilder):
    if isinstance(data, (list, tuple)):
        return [_readArgument(x, dot + "." + str(i), avroTypeBuilder) for i, x in enumerate(data)]
    else:
        raise PFASyntaxException("expected array of arguments, not " + _trunc(repr(data)), dot)

def _readArgument(data, dot, avroTypeBuilder):
    if data is None:
        return LiteralNull(dot)
    elif isinstance(data, bool):
        return LiteralBoolean(data, dot)
    elif isinstance(data, int):
        if -2147483648 <= data <= 2147483647:
            return LiteralInt(data, dot)
        elif -9223372036854775808 <= data <= 9223372036854775807:
            return LiteralLong(data, dot)
        else:
            raise PFASyntaxException("integer out of range: " + str(data), dot)
    elif isinstance(data, float):
        return LiteralDouble(data, dot)
    elif isinstance(data, basestring):
        if "." in data:
            words = data.split(".")
            ref = words[0]
            rest = words[1:]
            if not validSymbolName(ref):
                raise PFASyntaxException("\"{}\" is not a valid symbol name".format(ref), dot)
            for i in xrange(len(rest)):
                try:
                    asint = int(rest[i])
                except ValueError:
                    rest[i] = LiteralString(rest[i], dot)
                else:
                    rest[i] = LiteralInt(asint, dot)
            return AttrGet(Ref(ref), rest, dot)
        elif validSymbolName(data):
            return Ref(data, dot)
        else:
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(data), dot)

    elif isinstance(data, (list, tuple)):
        if len(data) == 1 and isinstance(data[0], basestring):
            return LiteralString(data[0], dot)
        else:
            raise PFASyntaxException("expecting expression, which may be [\"string\"], but no other array can be used as an expression", dot)

    elif isinstance(data, dict):
        at = data.get("@")
        keys = set(x for x in data.keys() if x != "@")

        _path = []
        _init = None
        _seq = False
        _partial = False
        _code = 0
        _newObject = None
        _newArray = None

        for key in keys:
            if key == "int": _int = _readInt(data[key], dot + "." + key)
            elif key == "long": _long = _readLong(data[key], dot + "." + key)
            elif key == "float": _float = _readFloat(data[key], dot + "." + key)
            elif key == "double": _double = _readDouble(data[key], dot + "." + key)
            elif key == "string": _string = _readString(data[key], dot + "." + key)
            elif key == "base64": _bytes = _readBase64(data[key], dot + "." + key)
            elif key == "type": _avroType = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "value": _value = _readJsonToString(data[key], dot + "." + key)

            elif key == "let": _let = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "set": _set = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "for": _forlet = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "step": _forstep = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "ifnotnull": _ifnotnull = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "do":
                if isinstance(data[key], (list, tuple)):
                    _body = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _body = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            elif key == "then":
                if isinstance(data[key], (list, tuple)):
                    _thenClause = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _thenClause = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            elif key == "else":
                if isinstance(data[key], (list, tuple)):
                    _elseClause = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _elseClause = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            elif key == "log":
                if isinstance(data[key], (list, tuple)):
                    _log = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _log = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            elif key == "path":
                _path = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "attr": _attr = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "if": _ifPredicate = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "while": _whilePredicate = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "until": _until = _readExpression(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "cond":
                _cond = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                if any(x.elseClause is not None for x in _cond):
                    raise PFASyntaxException("cond expression must only contain else-less if expressions", pos(dot, at))

            elif key == "cases": _cases = _readCastCaseArray(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "foreach": _foreach = _readString(data[key], dot + "." + key)
            elif key == "forkey": _forkey = _readString(data[key], dot + "." + key)
            elif key == "forval": _forval = _readString(data[key], dot + "." + key)
            elif key == "fcnref": _fcnref = _readString(data[key], dot + "." + key)
            elif key == "cell": _cell = _readString(data[key], dot + "." + key)
            elif key == "pool": _pool = _readString(data[key], dot + "." + key)

            elif key == "in": _in = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "cast": _cast = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "upcast": _upcast = _readExpression(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "init": _init = _readExpression(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "seq": _seq = _readBoolean(data[key], dot + "." + key)
            elif key == "partial": _partial = _readBoolean(data[key], dot + "." + key)

            elif key == "doc": _doc = _readString(data[key], dot + "." + key)
            elif key == "error": _error = _readString(data[key], dot + "." + key)
            elif key == "code": _code = _readInt(data[key], dot + "." + key)
            elif key == "namespace": _namespace = _readString(data[key], dot + "." + key)

            elif key == "new":
                if isinstance(data[key], dict):
                    _newObject = _readExpressionMap(data[key], dot + "." + key, avroTypeBuilder)
                elif isinstance(data[key], (list, tuple)):
                    _newArray = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    raise PFASyntaxException("\"new\" must be an object (map, record) or an array", pos(dot, at))

            elif key == "params": _params = _readParams(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "ret": _ret = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "as": _as = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)

            elif key == "to": _to = _readArgument(data[key], dot + "." + key, avroTypeBuilder)

            else:
                _callName = key
                if isinstance(data[key], (list, tuple)):
                    _callArgs = _readArgumentArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _callArgs = [_readArgument(data[key], dot + "." + key, avroTypeBuilder)]

        if "foreach" in keys and not validSymbolName(_foreach):
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(data[keys]), pos(dot, at))
        if "forkey" in keys and not validSymbolName(_forkey):
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(data[keys]), pos(dot, at))
        if "forval" in keys and not validSymbolName(_forval):
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(data[keys]), pos(dot, at))
        if "fcnref" in keys and not validFunctionName(_fcnref):
            raise PFASyntaxException("\"{}\" is not a valid function name".format(data[keys]), pos(dot, at))

        if keys == set(["int"]):                             return LiteralInt(_int, pos(dot, at))
        elif keys == set(["long"]):                          return LiteralLong(_long, pos(dot, at))
        elif keys == set(["float"]):                         return LiteralFloat(_float, pos(dot, at))
        elif keys == set(["double"]):                        return LiteralDouble(_double, pos(dot, at))
        elif keys == set(["string"]):                        return LiteralString(_string, pos(dot, at))
        elif keys == set(["base64"]):                        return LiteralBase64(_bytes, pos(dot, at))
        elif keys == set(["type", "value"]):                 return Literal(_avroType, _value, pos(dot, at))

        elif keys == set(["new", "type"]) and _newObject is not None:
                                                             return NewObject(_newObject, _avroType, avroTypeBuilder, pos(dot, at))
        elif keys == set(["new", "type"]) and _newArray is not None:
                                                             return NewArray(_newArray, _avroType, avroTypeBuilder, pos(dot, at))

        elif keys == set(["do"]):                            return Do(_body, pos(dot, at))
        elif keys == set(["let"]):                           return Let(_let, pos(dot, at))
        elif keys == set(["set"]):                           return SetVar(_set, pos(dot, at))

        elif keys == set(["attr", "path"]):                  return AttrGet(_attr, _path, pos(dot, at))
        elif keys == set(["attr", "path", "to"]):            return AttrTo(_attr, _path, _to, pos(dot, at))
        elif keys == set(["cell"]) or \
             keys == set(["cell", "path"]):                  return CellGet(_cell, _path, pos(dot, at))
        elif keys == set(["cell", "to"]) or \
             keys == set(["cell", "path", "to"]):            return CellTo(_cell, _path, _to, pos(dot, at))
        elif keys == set(["pool", "path"]):                  return PoolGet(_pool, _path, pos(dot, at))
        elif keys == set(["pool", "path", "to"]) or \
             keys == set(["pool", "path", "to", "init"]):    return PoolTo(_pool, _path, _to, _init, pos(dot, at))

        elif keys == set(["if", "then"]):                    return If(_ifPredicate, _thenClause, None, pos(dot, at))
        elif keys == set(["if", "then", "else"]):            return If(_ifPredicate, _thenClause, _elseClause, pos(dot, at))
        elif keys == set(["cond"]):                          return Cond(_cond, None, pos(dot, at))
        elif keys == set(["cond", "else"]):                  return Cond(_cond, _elseClause, pos(dot, at))

        elif keys == set(["while", "do"]):                   return While(_whilePredicate, _body, pos(dot, at))
        elif keys == set(["do", "until"]):                   return DoUntil(_body, _until, pos(dot, at))
        elif keys == set(["for", "while", "step", "do"]):    return For(_forlet, _whilePredicate, _forstep, _body, pos(dot, at))

        elif keys == set(["foreach", "in", "do"]) or \
             keys == set(["foreach", "in", "do", "seq"]):    return Foreach(_foreach, _in, _body, _seq, pos(dot, at))
        elif keys == set(["forkey", "forval", "in", "do"]):  return Forkeyval(_forkey, _forval, _in, _body, pos(dot, at))

        elif keys == set(["cast", "cases"]) or \
             keys == set(["cast", "cases", "partial"]):      return CastBlock(_cast, _cases, _partial, pos(dot, at))
        elif keys == set(["upcast", "as"]):                  return Upcast(_upcast, _as, pos(dot, at))

        elif keys == set(["ifnotnull", "then"]):             return IfNotNull(_ifnotnull, _thenClause, None, pos(dot, at))
        elif keys == set(["ifnotnull", "then", "else"]):     return IfNotNull(_ifnotnull, _thenClause, _elseClause, pos(dot, at))

        elif keys == set(["doc"]):                           return Doc(_doc, pos(dot, at))

        elif keys == set(["error"]):                         return Error(_error, None, pos(dot, at))
        elif keys == set(["error", "code"]):                 return Error(_error, _code, pos(dot, at))
        elif keys == set(["log"]):                           return Log(_log, None, pos(dot, at))
        elif keys == set(["log", "namespace"]):              return Log(_log, _namespace, pos(dot, at))

        elif keys == set(["params", "ret", "do"]):           return FcnDef(_params, _ret, _body, pos(dot, at))
        elif keys == set(["fcnref"]):                        return FcnRef(_fcnref, pos(dot, at))

        elif len(keys) == 1 and list(keys)[0] not in \
             set(["as", "base64", "cases", "cast", "cell", "code", "cond", "do", "doc", "double", "else", "error", "fcnref",
                  "float", "for", "foreach", "forkey", "forval", "if", "ifnotnull", "in", "init", "int", "let", "log", "long",
                  "namespace", "new", "params", "partial", "path", "pool", "ret", "seq", "set", "step", "string", "then",
                  "to", "type", "upcast", "until", "value", "while"]):
                                                             return Call(_callName, _callArgs, pos(dot, at))

        else: raise PFASyntaxException("unrecognized special form: {} (not enough arguments? too many?)".format(", ".join(keys)), pos(dot, at))

    else:
        raise PFASyntaxException("expected expression, not " + _trunc(repr(data)), pos(dot, at))

def _readFcnDefMap(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        for k in data.keys():
            if k != "@" and not validFunctionName(k):
                raise PFASyntaxException("\"{}\" is not a valid function name".format(k), pos(dot, at))
        return dict((k, _readFcnDef(v, dot + "." + k, avroTypeBuilder)) for k, v in data.items() if k != "@")
    else:
        raise PFASyntaxException("expected map of function definitions, not " + _trunc(repr(data)), pos(dot, at))

def _readFcnDef(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        keys = set(x for x in data.keys() if x != "@")

        for key in keys:
            if key == "params": _params = _readParams(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "ret": _ret = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "do":
                if isinstance(data[key], (list, tuple)):
                    _body = _readExpressionArray(data[key], dot + "." + key, avroTypeBuilder)
                else:
                    _body = [_readExpression(data[key], dot + "." + key, avroTypeBuilder)]
            else:
                raise PFASyntaxException("unexpected field in function definition: " + key, pos(dot, at))

        required = set(["params", "ret", "do"])
        if (keys != required):
            raise PFASyntaxException("wrong set of fields for a function definition: " + ", ".join(keys), pos(dot, at))
        else:
            return FcnDef(_params, _ret, _body, pos(dot, at))
    else:
        raise PFASyntaxException("expected function definition, not " + _trunc(repr(data)), pos(dot, at))

def _readParams(data, dot, avroTypeBuilder):
    if isinstance(data, (list, tuple)):
        return [_readParam(x, dot + "." + str(i), avroTypeBuilder) for i, x in enumerate(data)]
    else:
        raise PFASyntaxException("expected array of function parameters, not " + _trunc(repr(data)), dot)

def _readParam(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        keys = set(x for x in data.keys() if x != "@")
        if len(keys) != 1:
            raise PFASyntaxException("function parameter name-type map should have only one pair", pos(dot, at))
        n = list(keys)[0]
        if not validSymbolName(n):
            raise PFASyntaxException("\"{}\" is not a valid symbol name".format(n))

        t = _readAvroPlaceholder(data[n], dot + "." + n, avroTypeBuilder)
        return {n: t}
    else:
        raise PFASyntaxException("expected function parameter name-type singleton map, not " + _trunc(repr(data)), pos(dot, at))

def _readCells(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        for k in data.keys():
            if k != "@" and not validSymbolName(k):
                raise PFASyntaxException("\"{}\" is not a valid symbol name".format(k), pos(dot, at))
        return dict((k, _readCell(data[k], dot, avroTypeBuilder)) for k, v in data.items() if k != "@")
    else:
        raise PFASyntaxException("expected map of cells, not " + _trunc(repr(data)), pos(dot, at))

def _readCell(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        _shared = False
        _rollback = False
        keys = set(x for x in data.keys() if x != "@")
        for key in keys:
            if key == "type": _avroType = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "init": _init = _readJsonToString(data[key], dot + "." + key)
            elif key == "shared": _shared = _readBoolean(data[key], dot + "." + key)
            elif key == "rollback": _rollback = _readBoolean(data[key], dot + "." + key)
            else:
                raise PFASyntaxException("unexpected cell property: \"{}\"".format(key), pos(dot, at))

        if ("type" not in keys) or ("init" not in keys) or (not keys.issubset(set(["type", "init", "shared", "rollback"]))):
            raise PFASyntaxException("wrong set of fields for a cell: " + ", ".join(keys), pos(dot, at))
        else:
            return Cell(_avroType, _init, _shared, _rollback, pos(dot, at))
    else:
        raise PFASyntaxException("expected cell, not " + _trunc(repr(data)), pos(dot, at))

def _readPools(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        for k in data.keys():
            if k != "@" and not validSymbolName(k):
                raise PFASyntaxException("\"{}\" is not a valid symbol name".format(k), pos(dot, at))
        return dict((k, _readPool(data[k], dot, avroTypeBuilder)) for k, v in data.items() if k != "@")
    else:
        raise PFASyntaxException("expected map of pools, not " + _trunc(repr(data)), pos(dot, at))

def _readPool(data, dot, avroTypeBuilder):
    if isinstance(data, dict):
        at = data.get("@")
        _init = {}
        _shared = False
        _rollback = False
        keys = set(x for x in data.keys() if x != "@")
        for key in keys:
            if key == "type": _avroType = _readAvroPlaceholder(data[key], dot + "." + key, avroTypeBuilder)
            elif key == "init": _init = _readJsonToStringMap(data[key], dot + "." + key)
            elif key == "shared": _shared = _readBoolean(data[key], dot + "." + key)
            elif key == "rollback": _rollback = _readBoolean(data[key], dot + "." + key)

        if ("type" not in keys) or (not keys.issubset(set(["type", "init", "shared", "rollback"]))):
            raise PFASyntaxException("wrong set of fields for a pool: " + ", ".join(keys), pos(dot, at))
        else:
            return Pool(_avroType, _init, _shared, _rollback, pos(dot, at))
    else:
        raise PFASyntaxException("expected pool, not " + _trunc(repr(data)), pos(dot, at))
