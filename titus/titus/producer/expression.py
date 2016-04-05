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

import ast
from collections import OrderedDict

class PythonToPfaException(Exception):
    """Exception for errors encountered when converting Python code into PFA."""
    pass

defaultOptions = {"lineNumbers": True,
                  "wantArray": False}

def pfa(expr, subs=None, symbols=None, cells=None, pools=None, options=None, **kwds):
    """Convert Python code into a PFA expression (not a whole PFA document).

    :type expr: string
    :param expr: Python code in a string
    :type subs: dict from substitution strings to their values
    :param subs: Python identifiers to expand to given PFA structures
    :type symbols: list of strings
    :param symbols: Python identifiers to assume are PFA expressions in scope
    :type cells: list of strings
    :param cells: Python identifiers to assume are PFA cell names
    :type pools: list of strings
    :param pools: Python identifiers to assume are PFA pool names
    :type options: dict from option names to their values
    :param options: options for interpreting the Python and producing PFA, such as ``lineNumbers`` and ``wantArray``
    :type kwds: dict from substitution strings to their values
    :param kwds: added to ``subs`` (a more convenient way to pass them)
    :rtype: Pythonized JSON
    :return: the resulting PFA expression
    """

    if subs is None:
        subs = {}
    if len(kwds) > 0:
        subs = dict(subs)
        subs.update(kwds)

    if symbols is None:
        symbols = set()
    else:
        symbols = set(symbols)
    if cells is None:
        cells = set()
    else:
        cells = set(cells)
    if pools is None:
        pools = set()
    else:
        pools = set(pools)

    combinedOptions = dict(defaultOptions)
    if options is not None:
        combinedOptions.update(options)

    state = {"subs": subs, "symbols": symbols, "cells": cells, "pools": pools, "options": combinedOptions}
    out = _statements(ast.parse(expr).body, state)
    if not combinedOptions["wantArray"] and len(out) == 1:
        out = out[0]
    return out

def fcn(params, ret, expr, subs=None, cells=None, pools=None, options=None, **kwds):
    """Convert Python code into a PFA function (not a whole PFA document).

    :type params: Pythonized JSON
    :param params: PFA function params, form is ``[{"argName1": argType1}, {"argName2": argType2}, ...]`` where the ``argTypes`` are Avro type schemas
    :type ret: Pythonized JSON
    :param ret: PFA function ret, form is Avro type schema
    :type subs: dict from substitution strings to their values
    :param subs: Python identifiers to expand to given PFA structures
    :type cells: list of strings
    :param cells: Python identifiers to assume are PFA cell names
    :type pools: list of strings
    :param pools: Python identifiers to assume are PFA pool names
    :type options: dict from option names to their values
    :param options: options for interpreting the Python and producing PFA, such as ``lineNumbers`` and ``wantArray``
    :type kwds: dict from substitution strings to their values
    :param kwds: added to ``subs`` (a more convenient way to pass them)
    :rtype: Pythonized JSON
    :return: the resulting PFA function
    """
    combinedOptions = dict(defaultOptions)
    if options is not None:
        combinedOptions.update(options)
    return OrderedDict([("params", params), ("ret", ret), ("do", pfa(expr, subs, None, cells, pools, options, **kwds))])

def _form(state, lineno, others):
    if state["options"]["lineNumbers"] and lineno is not None:
        return OrderedDict([("@", "Python line {0}".format(lineno))] + others.items())
    else:
        return others

def _newscope(state, newvars=None):
    newstate = dict(state)
    newstate["symbols"] = set(newstate["symbols"])
    if newvars is not None:
        for x in newvars:
            newstate["symbols"].add(x)
    return newstate

def _statements(stmts, state):
    return [_statement(x, state) for x in stmts]

def _statement(stmt, state):
    if isinstance(stmt, ast.Assign):
        if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            name = stmt.targets[0].id
            expr = _expression(stmt.value, state)
            if name in state["symbols"]:
                letset = "set"
            else:
                letset = "let"
            state["symbols"].add(name)
            return _form(state, stmt.lineno, {letset: {name: expr}})

        elif len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Tuple) and all(isinstance(x, ast.Name) for x in stmt.targets[0].elts) and isinstance(stmt.value, ast.Tuple):
            out = OrderedDict()
            lets = 0
            sets = 0
            for name, value in zip(stmt.targets[0].elts, stmt.value.elts):
                if name.id in state["symbols"]:
                    sets += 1
                else:
                    lets += 1
                    state["symbols"].add(name.id)
                out[name.id] = _expression(value, state)
            if lets > 0 and sets == 0:
                letset = "let"
            elif lets == 0 and sets > 0:
                letset = "set"
            else:
                raise PythonToPfaException("cannot declare {0} symbols and reassign {1} in the same statement (source line {2})".format(lets, sets, stmt.lineno))
            return _form(state, stmt.lineno, {letset: out})

    elif isinstance(stmt, ast.Print):
        return _form(state, stmt.lineno, {"log": [_expression(x, state) for x in stmt.values]})

    elif isinstance(stmt, ast.For):
        if isinstance(stmt.target, ast.Name):
            newvar = stmt.target.id
            array = _expression(stmt.iter, _newscope(state))
            body = _statements(stmt.body, _newscope(state, [newvar]))
            return _form(state, stmt.lineno, OrderedDict([("foreach", newvar), ("in", array), ("do", body), ("seq", True)]))

        elif isinstance(stmt.target, ast.Tuple) and len(stmt.target.elts) == 2 and all(isinstance(x, ast.Name) for x in stmt.target.elts):
            keyvar = stmt.target.elts[0].id
            valvar = stmt.target.elts[1].id
            mapping = _expression(stmt.iter, _newscope(state))
            body = _statements(stmt.body, _newscope(state, [keyvar, valvar]))
            return _form(state, stmt.lineno, OrderedDict([("forkey", keyvar), ("forval", valvar), ("in", mapping), ("do", body)]))
            
    elif isinstance(stmt, ast.While):
        test = _expression(stmt.test, _newscope(state))
        body = _statements(stmt.body, _newscope(state))
        return _form(state, stmt.lineno, OrderedDict([("while", test), ("do", body)]))

    elif isinstance(stmt, ast.If):
        chain = _ifchain(state, stmt, [])
        if len(chain) == 1:
            return _form(state, stmt.lineno, chain[0])
        elif len(chain) == 2 and not isinstance(chain[1], OrderedDict):
            chain[0]["else"] = chain[1]
            return _form(state, stmt.lineno, chain[0])
        elif not isinstance(chain[-1], OrderedDict):
            ifThens, elseClause = chain[:-1], chain[-1]
            return _form(state, stmt.lineno, OrderedDict([("cond", ifThens), ("else", elseClause)]))
        else:
            return _form(state, stmt.lineno, OrderedDict([("cond", chain)]))

    elif isinstance(stmt, ast.Raise):
        if isinstance(stmt.type, ast.Str):
            return _form(state, stmt.lineno, {"error": stmt.type.s})

    elif isinstance(stmt, ast.Expr):
        return _expression(stmt.value, state)

    elif isinstance(stmt, ast.Pass):
        return _form(state, stmt.lineno, {"doc": ""})

    raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(stmt), stmt.lineno))

def _ifchain(state, stmt, chain):
    test = _expression(stmt.test, _newscope(state))
    thenClause = _statements(stmt.body, _newscope(state))
    chain.append(OrderedDict([("if", test), ("then", thenClause)]))

    if len(stmt.orelse) == 0:
        return chain

    elif len(stmt.orelse) == 1 and isinstance(stmt.orelse[0], ast.If):
        return _ifchain(state, stmt.orelse[0], chain)

    else:
        chain.append(_statements(stmt.orelse, _newscope(state)))
        return chain

def _expression(expr, state):
    if isinstance(expr, ast.BoolOp):
        if isinstance(expr.op, ast.And):
            andor = "&&"
        else:
            andor = "||"

        items = list(reversed(expr.values))
        out = _form(state, expr.lineno, {andor: [_expression(items.pop(), _newscope(state)), _expression(items.pop(), _newscope(state))]})

        while len(items) > 0:
            out = _form(state, expr.lineno, {andor: [out, _expression(items.pop(), _newscope(state))]})

        return out

    elif isinstance(expr, ast.BinOp):
        if isinstance(expr.op, ast.Add):
            op = "+"
        elif isinstance(expr.op, ast.Sub):
            op = "-"
        elif isinstance(expr.op, ast.Mult):
            op = "*"
        elif isinstance(expr.op, ast.Div):
            op = "/"
        elif isinstance(expr.op, ast.Mod):
            op = "%"
        elif isinstance(expr.op, ast.Pow):
            op = "**"
        elif isinstance(expr.op, ast.LShift):
            raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(expr), expr.lineno))
        elif isinstance(expr.op, ast.RShift):
            raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(expr), expr.lineno))
        elif isinstance(expr.op, ast.BitOr):
            op = "|"
        elif isinstance(expr.op, ast.BitXor):
            op = "^"
        elif isinstance(expr.op, ast.BitAnd):
            op = "&"
        elif isinstance(expr.op, ast.FloorDiv):
            op = "//"
        return _form(state, expr.lineno, {op: [_expression(expr.left, _newscope(state)), _expression(expr.right, _newscope(state))]})

    elif isinstance(expr, ast.UnaryOp):
        if isinstance(expr.op, ast.Invert):
            op = "~"
        elif isinstance(expr.op, ast.Not):
            op = "!"
        elif isinstance(expr.op, ast.UAdd):
            raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(expr), expr.lineno))
        elif isinstance(expr.op, ast.USub):
            op = "u-"
        return _form(state, expr.lineno, {op: _expression(expr.operand, _newscope(state))})

    elif isinstance(expr, ast.IfExp):
        test = _expression(expr.test, _newscope(state))
        thenClause = _expression(expr.body, _newscope(state))
        elseClause = _expression(expr.orelse, _newscope(state))
        return _form(state, expr.lineno, OrderedDict([("if", test), ("then", thenClause), ("else", elseClause)]))

    elif isinstance(expr, ast.Dict):
        return _literal(expr, state)
    
    elif isinstance(expr, ast.Compare):
        def opname(x):
            if isinstance(x, ast.Eq): return "=="
            elif isinstance(x, ast.NotEq): return "!="
            elif isinstance(x, ast.Lt): return "<"
            elif isinstance(x, ast.LtE): return "<="
            elif isinstance(x, ast.Gt): return ">"
            elif isinstance(x, ast.GtE): return ">="
            else:
                raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(x), x.lineno))

        ops = list(reversed(expr.ops))
        cmps = list(reversed(expr.comparators))

        lastexpr = _expression(cmps.pop(), _newscope(state))
        out = _form(state, expr.lineno, {opname(ops.pop()): [_expression(expr.left, _newscope(state)), lastexpr]})

        while len(ops) > 0 and len(cmps) > 0:
            newexpr = _expression(cmps.pop(), _newscope(state))
            out = _form(state, expr.lineno, {"&&": [out, _form(state, expr.lineno, {opname(ops.pop()): [lastexpr, newexpr]})]})
            lastexpr = newexpr
                  
        return out

    elif isinstance(expr, ast.Call):
        def unfold(x):
            if isinstance(x, ast.Name):
                return x.id
            elif isinstance(x, ast.Attribute):
                return unfold(x.value) + "." + x.attr
            else:
                raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(x), x.lineno))

        specialForms = ["Int", "Long", "Float", "Double", "String", "Base64", "Type", "Value", "New", "Do", "Let", "Set", "Attr", "Path", "To", "Cell", "Pool", "Init", "If", "Then", "Else", "Cond", "While", "Until", "For", "Step", "Foreach", "In", "Forkey", "Forval", "Cast", "Cases", "Partial", "Upcast", "As", "Ifnotnull", "Doc", "Error", "Log", "Namespace", "Params", "Ret", "Fcn"]
        coreLibrary = {"Plus": "+", "Minus": "-", "Times": "*", "Divide": "/", "FloorDivide": "//", "Negative": "U-", "Modulo": "%", "Remainder": "%%", "Pow": "**", "Comparison": "cmp", "Equal": "==", "GreaterOrEqual": ">=", "GreaterThan": ">", "NotEqual": "!=", "LessThan": "<", "LessOrEqual": "<=", "Max": "max", "Min": "min", "And": "&&", "Or": "||", "XOr": "^^", "Not": "!", "BitwiseAnd": "&", "BitwiseOr": "|", "BitwiseXOr": "^", "BitwiseNot": "~"}

        name = unfold(expr.func)
        if name in specialForms:
            name = name.lower()
        name = coreLibrary.get(name, name)

        args = [_expression(x, _newscope(state)) for x in expr.args]
        if len(args) == 1:
            args = args[0]

        if name in ["string", "base64", "value", "attr", "cell", "pool", "foreach", "forkey", "forval", "doc", "error", "fcn"] and isinstance(args, dict) and len(args) == 1 and args.keys() == ["string"]:
            args, = args.values()

        out = OrderedDict([(name, args)])

        for kwd in expr.keywords:
            kwdarg = kwd.arg
            if kwdarg in specialForms:
                kwdarg = kwdarg.lower()
            out[kwdarg] = _expression(kwd.value, _newscope(state))

        return _form(state, expr.lineno, out)

    elif isinstance(expr, ast.Num):
        return expr.n

    elif isinstance(expr, ast.Str):
        return {"string": expr.s}

    elif isinstance(expr, ast.Attribute):
        return _unfold(expr, [], state)

    elif isinstance(expr, ast.Subscript):
        return _unfold(expr, [], state)

    elif isinstance(expr, ast.Name):
        if expr.id in state["subs"]:
            return state["subs"][expr.id]
        elif expr.id == "None":
            return None
        elif expr.id == "True":
            return True
        elif expr.id == "False":
            return False
        else:
            return expr.id

    elif isinstance(expr, ast.List):
        return _literal(expr, state)

    elif isinstance(expr, ast.Tuple):
        return [_expression(x, state) for x in expr.elts]

    raise PythonToPfaException("Python AST node {0} (source line {1}) does not have a PFA equivalent".format(ast.dump(expr), expr.lineno))

def _unfold(x, path, state):
    if isinstance(x, ast.Attribute):
        path.insert(0, {"string": x.attr})
        return _unfold(x.value, path, state)

    elif isinstance(x, ast.Subscript) and isinstance(x.slice, ast.Index):
        path.insert(0, _expression(x.slice.value, state))
        return _unfold(x.value, path, state)

    else:
        if isinstance(x, ast.Name) and x.id in state["cells"]:
            return _form(state, x.lineno, OrderedDict([("cell", x.id), ("path", path)]))
        elif isinstance(x, ast.Name) and x.id in state["pools"]:
            return _form(state, x.lineno, OrderedDict([("pool", x.id), ("path", path)]))
        else:
            return _form(state, x.lineno, OrderedDict([("attr", _expression(x, state)), ("path", path)]))

def _literal(expr, state):
    if isinstance(expr, ast.Dict):
        out = OrderedDict()
        for key, value in zip(expr.keys, expr.values):
            if isinstance(key, ast.Str) or (isinstance(key, ast.Name) and isinstance(state["subs"].get(key.id, None), basestring)):
                kkey = _literal(key, state)
                vvalue = _literal(value, state)
                out[kkey] = vvalue
            else:
                raise PythonToPfaException("literal JSON keys must be strings or subs identifiers, not {0} (source line {1})".format(ast.dump(expr), expr.lineno))
        return out

    elif isinstance(expr, ast.Num):
        return expr.n

    elif isinstance(expr, ast.Str):
        return expr.s

    elif isinstance(expr, ast.Name):
        if expr.id in state["subs"]:
            return state["subs"][expr.id]
        elif expr.id == "None":
            return None
        elif expr.id == "True":
            return True
        elif expr.id == "False":
            return False
        else:
            raise PythonToPfaException("identifiers ({0}) are not allowed in a literal expression, unless they are subs identifiers (source line {1})".format(ast.dump(expr), expr.lineno))

    elif isinstance(expr, ast.List):
        out = []
        for value in expr.elts:
            out.append(_literal(value, state))
        return out

    raise PythonToPfaException("Python AST node {0} (source line {1}) is not literal JSON".format(ast.dump(expr), expr.lineno))
