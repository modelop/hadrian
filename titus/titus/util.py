#!/usr/bin/env python

import inspect
import sys

uniqueEngineNameCounter = 0
def uniqueEngineName():
    sys.modules["titus.util"].uniqueEngineNameCounter += 1
    return "Engine_{}".format(sys.modules["titus.util"].uniqueEngineNameCounter)

uniqueRecordNameCounter = 0
def uniqueRecordName():
    sys.modules["titus.util"].uniqueRecordNameCounter += 1
    return "Record_{}".format(sys.modules["titus.util"].uniqueRecordNameCounter)

uniqueEnumNameCounter = 0
def uniqueEnumName():
    sys.modules["titus.util"].uniqueEnumNameCounter += 1
    return "Enum_{}".format(sys.modules["titus.util"].uniqueEnumNameCounter)

uniqueFixedNameCounter = 0
def uniqueFixedName():
    sys.modules["titus.util"].uniqueFixedNameCounter += 1
    return "Fixed_{}".format(sys.modules["titus.util"].uniqueFixedNameCounter)

def pos(dot, at):
    return "in{} object from {}".format("" if (dot == "") else " field " + dot + " of", at)

def flatten(x):
    return [item for sublist in x for item in sublist]

def div(numer, denom):
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
    def __init__(self, parent):
        self.parent = parent
        self.symbols = dict()

    def get(self, symbol):
        if symbol in self.symbols:
            return self.symbols[symbol]
        elif self.parent is not None:
            return self.parent.get(symbol)
        else:
            raise RuntimeError()

    def let(self, nameExpr):
        for symbol, init in nameExpr.items():
            self.symbols[symbol] = init

    def set(self, nameExpr):
        for symbol, value in nameExpr.items():
            if symbol in self.symbols:
                self.symbols[symbol] = value
            elif self.parent is not None:
                self.parent.set(nameExpr)
            else:
                raise RuntimeError()

def callfcn(state, scope, fcn, args):
    if hasattr(fcn, "paramNames"):
        callScope = DynamicScope(scope)
        if isinstance(args, (tuple, list)):
            args = dict(zip(fcn.paramNames, args))
        callScope.let(args)
        return fcn(state, callScope)
    else:
        return fcn(state, scope, None, *args)

def negativeIndex(length, index):
    if index >= 0:
        return index
    else:
        return length + index

def checkRange(length, index):
    if index < 0 or index >= length:
        raise PFARuntimeException("index out of range")

def startEnd(length, start, end):
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
    fields = [x for x in inspect.getargspec(clazz.__init__).args[1:] if x != "pos"]

    try:
        code = clazz.__init__.__func__
    except AttributeError:
        code = clazz.__init__.func_code

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
    return \"{}()\"
""".format(clazz.__name__), context, newMethods)

        exec("""
def __eq__(self, other):
    return isinstance(other, {})
""".format(clazz.__name__), context, newMethods)

    else:
        exec("""
def __repr__(self):
    return \"{}(\" + {} + \")\"
""".format(clazz.__name__, "+ \", \" + ".join(["repr(self." + x + ")" for x in fields])),
             context,
             newMethods)

        exec("""
def __eq__(self, other):
    if isinstance(other, {}):
        return {}
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

