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

import json

import titus.ast as ast
from titus.datatype import AvroArray
from titus.datatype import AvroDouble
from titus.datatype import AvroString
from titus.datatype import AvroTypeBuilder
from titus.signature import LabelData
from titus.util import uniqueEngineName, uniqueRecordName, uniqueEnumName

class Context(object):
    def copy(self, **butChange):
        out = Context()
        out.__dict__ = dict(self.__dict__)
        out.__dict__.update(butChange)
        return out

    def fieldRef(self, name):
        if name in self.scope:
            return ast.Ref(name)
        elif name in self.dataDictionary:
            return ast.AttrGet(ast.Ref("input"), [ast.LiteralString(name)])
        else:
            raise NameError("unknown field \"{}\"".format(name))

    def symbolTable(self):
        out = {"input": self.inputType}
        out.update(self.scope)
        return out

class PmmlBinding(object):
    def __init__(self):
        self.children = []
        self.text = ""
        self.pos = None

    @property
    def tag(self):
        return self.__class__.__name__

class ModelElement(object):
    def defineFields(self, options, context, transformations):
        action = []
        for derivedField in transformations.DerivedField:
            name, value = derivedField.toPFA(options, context)
            action.append(ast.Let({name: value}))
        return action

class Expression(object):
    pass

class Predicate(object):
    pass

class HasDataType(object):
    def pmmlTypeToAvro(self, dataType=None):
        if dataType is None:
            dataType = self.dataType
        if dataType == "string":
            return "string"
        elif dataType == "integer":
            return "int"
        elif dataType == "float":
            return "float"
        elif dataType == "double":
            return "double"
        else:
            raise NotImplementedError

class PMML(PmmlBinding):
    def toPFA(self, options, context):
        inputType = self.DataDictionary[0].toPFA(options, context)
        context.inputType = context.avroTypeBuilder.resolveOneType(json.dumps(inputType))

        models = self.models()
        if len(models) == 0:
            action = [ast.LiteralNull()]
            fcns = {}
            context.cells = {}
            context.pools = {}
            outputType = "null"
        else:
            action = []
            fcns = {}

            context.scope = {}
            context.fcns = {}
            context.cells = {}
            context.pools = {}
            context.storageType = "cell"
            context.storageName = "modelData"

            if len(self.TransformationDictionary) > 0:
                for defineFunction in self.TransformationDictionary[0].DefineFunction:
                    name, fcn = defineFunction.toPFA(options, context)
                    fcns[name] = fcn
                    context.fcns["u." + name] = ast.UserFcn.fromFcnDef("u." + name, fcn)

                action.extend(models[0].defineFields(options, context, self.TransformationDictionary[0]))

            if len(models[0].LocalTransformations) > 0:
                action.extend(models[0].defineFields(options, context, models[0].LocalTransformations[0]))

            action.extend(models[0].toPFA(options, context))
            outputType = context.outputType

        return ast.EngineConfig(
            name=options.get("engine.name", uniqueEngineName()),
            method=ast.Method.MAP,
            inputPlaceholder=context.avroTypeBuilder.makePlaceholder(json.dumps(inputType)),
            outputPlaceholder=context.avroTypeBuilder.makePlaceholder(json.dumps(outputType)),
            begin=[],
            action=action,
            end=[],
            fcns=fcns,
            zero=None,
            cells=context.cells,
            pools=context.pools,
            randseed=options.get("engine.randseed", None),
            doc=options.get("engine.doc", None),
            metadata=options.get("engine.metadata", None),
            options=options.get("engine.options", {}))

class ARIMA(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Aggregate(PmmlBinding, Expression):
    def toPFA(self, options, context):
        raise NotImplementedError

class Alternate(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class AlwaysFalse(PmmlBinding, Predicate):
    def toPFA(self, options, context):
        raise NotImplementedError

class AlwaysTrue(PmmlBinding, Predicate):
    def toPFA(self, options, context):
        raise NotImplementedError

class Annotation(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Anova(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class AnovaRow(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class AntecedentSequence(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class AnyDistribution(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class Application(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Apply(PmmlBinding, Expression):
    pmmlToPFA = {
        "+": "+",
        "-": "-",
        "*": "*",
        "/": "/",
        "log10": "log10",
        "ln": "ln",
        "sqrt": "sqrt",
        "abs": "abs",
        "exp": "exp",
        "pow": "pow",
        "floor": "floor",
        "ceil": "ceil",
        "round": "round",
        "equal": "==",
        "notEqual": "!=",
        "lessThan": "<",
        "lessOrEqual": "<=",
        "greaterThan": ">",
        "greaterOrEqual": ">=",
        "and": "and",
        "or": "or",
        "not": "not",
        "uppercase": "s.upper",
        "lowercase": "s.lower",
        }

    def argTypes(self, args, context):
        return [ast.inferType(x, context.symbolTable(), fcns=context.fcns) for x in args]

    def broadestType(self, types):
        return LabelData.broadestType(types)

    def toPFA(self, options, context):
        expressions = [x for x in self.children if isinstance(x, Expression)]
        args = [x.toPFA(options, context) for x in expressions]
        
        if "u." + self.function in context.fcns:
            return ast.Call("u." + self.function, args)

        elif self.function in self.pmmlToPFA:
            return ast.Call(self.pmmlToPFA[self.function], args)

        elif self.function == "min":
            if len(expressions) == 2:
                return ast.Call("min", args)
            else:
                return ast.Call("a.min", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "max":
            if len(expressions) == 2:
                return ast.Call("max", args)
            else:
                return ast.Call("a.max", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "sum":
            if len(expressions) == 2:
                return ast.Call("+", args)
            else:
                return ast.Call("a.sum", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "avg":
            return ast.Call("a.mean", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "median":
            return ast.Call("a.median", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "product":
            if len(expressions) == 2:
                return ast.Call("*", args)
            else:
                return ast.Call("a.product", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))), context.avroTypeBuilder)])

        elif self.function == "threshold":
            return ast.If(ast.Call(">", args), [ast.LiteralInt(1)], [ast.LiteralInt(0)])

        elif self.function == "isMissing" or self.function == "isNotMissing":
            raise NotImplementedError

        elif self.function == "isIn":
            return ast.Call("a.contains", [args(1), args(0)])

        elif self.function == "isNotIn":
            return ast.Call("not", [ast.Call("a.contains", [args(1), args(0)])])

        elif self.function == "if":
            if len(args) != 3:
                raise NotImplementedError
            else:
                return ast.If(args(0), [args(1)], [args(2)])

        elif self.function == "substring":
            return ast.Call("s.substr", [args(0), ast.Call("+", args)])

        elif self.function == "trimBlanks":
            return ast.Call("s.strip", [args(0), ast.LiteralString(" \t\n")])

        elif self.function == "concat":
            if len(expressions) == 2:
                return ast.Call("s.concat", args)
            else:
                return ast.Call("a.join", [ast.NewArray(args, AvroArray(AvroString()), context.avroTypeBuilder), ast.LiteralString("")])

        elif self.function == "replace" or self.function == "matches":
            raise NotImplementedError    # requires regular expressions

        elif self.function == "formatNumber":
            raise NotImplementedError    # requires printf-like formatting

        elif self.function == "formatDatetime":
            raise NotImplementedError

        elif self.function == "dateDaysSinceYear":
            raise NotImplementedError

        elif self.function == "dateSecondsSinceYear":
            raise NotImplementedError

        elif self.function == "dateSecondsSinceMidnight":
            raise NotImplementedError

        else:
            raise ValueError("not a PMML built-in function: " + self.function)
        
class Array(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class AssociationModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class AssociationRule(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Attribute(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BaseCumHazardTables(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Baseline(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BaselineCell(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BaselineModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        return [self.TestDistributions[0].toPFA(options, context)]

class BaselineStratum(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesInput(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesInputs(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesOutput(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BoundaryValueMeans(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class BoundaryValues(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CategoricalPredictor(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Categories(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Category(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Characteristic(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Characteristics(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ChildParent(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ClassLabels(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Cluster(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringModelQuality(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Coefficient(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Coefficients(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ComparisonMeasure(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Comparisons(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ComplexPartialScore(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CompoundPredicate(PmmlBinding, Predicate):
    def toPFA(self, options, context):
        raise NotImplementedError

class CompoundRule(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Con(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ConfusionMatrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ConsequentSequence(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Constant(PmmlBinding, Expression):
    def toPFA(self, options, context):
        if self.dataType is None:
            try:
                value = int(self.text)
            except ValueError:
                try:
                    value = float(self.text)
                except ValueError:
                    out = ast.LiteralString(self.text)
                else:
                    out = ast.LiteralDouble(value)
            else:
                out = ast.LiteralInt(value)

        elif self.dataType == "string":
            out = ast.LiteralString(self.text)

        elif self.dataType == "integer":
            out = ast.LiteralInt(int(self.text))

        elif self.dataType == "float":
            out = ast.LiteralFloat(float(self.text))

        elif self.dataType == "double":
            out = ast.LiteralDouble(float(self.text))

        else:
            raise NotImplementedError

        return out

class Constraints(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ContStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationFields(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationMethods(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationValues(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Correlations(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CountTable(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Counts(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Covariances(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class CovariateList(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class DataDictionary(PmmlBinding):
    def toPFA(self, options, context):
        context.dataDictionary = {}

        fields = []
        for dataField in self.DataField:
            fields.append(dataField.toPFA(options, context))

        return {"type": "record", "name": "DataDictionary", "fields": fields}

class DataField(PmmlBinding, HasDataType):
    def toPFA(self, options, context):
        context.dataDictionary[self.name] = {"type": self.pmmlTypeToAvro()}
        return {"name": self.name, "type": self.pmmlTypeToAvro()}

class Decision(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class DecisionTree(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Decisions(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class DefineFunction(PmmlBinding):
    def toPFA(self, options, context):
        expressions = [x for x in self.children if isinstance(x, Expression)]

        params = []
        for parameterField in self.ParameterField:
            if parameterField.dataType is None:
                raise TypeError("parameter field dataType needed for field \"{}\" of function \"{}\"".format(parameterField.name, self.name))
            params.append(parameterField.toPFA(options, context))

        symbolTable = {}
        for p in params:
            n = p.keys()[0]
            v = p.values()[0]
            symbolTable[n] = v

        expr = expressions[0].toPFA(options, context.copy(scope=symbolTable))

        inferred = ast.inferType(expr, symbolTable, fcns=context.fcns)

        if self.dataType is not None:
            declared = context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))

            if not declared.accepts(inferred):
                raise TypeError("DefineFunction {} has inferred type {} and declared type {}".format(self.name, repr(inferred), repr(declared)))

            ret = declared

            if not inferred.accepts(declared):
                expr = ast.Upcast(expr, context.avroTypeBuilder.makePlaceholder(json.dumps(self.pmmlTypeToAvro())))

        else:
            ret = inferred

        return self.name, ast.FcnDef(params, ret, [expr])

class Delimiter(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class DerivedField(PmmlBinding, HasDataType):
    def toPFA(self, options, context):
        for child in self.children:
            if isinstance(child, Expression):
                expr = child.toPFA(options, context)
                inferred = ast.inferType(expr, context.symbolTable(), fcns=context.fcns)

                if self.dataType is not None:
                    declared = context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))
                    if not declared.accepts(inferred):
                        raise TypeError("DerivedField {} has inferred type {} and declared type {}".format(self.name, repr(inferred), repr(declared)))

                    context.scope[self.name] = declared

                    if not inferred.accepts(declared):
                        expr = ast.Upcast(expr, context.avroTypeBuilder.makePlaceholder(json.dumps(self.pmmlTypeToAvro())))

                else:
                    context.scope[self.name] = inferred

                return self.name, expr

class DiscrStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Discretize(PmmlBinding, Expression):
    def toPFA(self, options, context):
        raise NotImplementedError

class DiscretizeBin(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class DocumentTermMatrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class EventValues(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ExponentialSmoothing(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Extension(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class FactorList(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldColumnPair(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldRef(PmmlBinding, Expression):
    def toPFA(self, options, context):
        return context.fieldRef(self.field)

class FieldValue(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldValueCount(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class GaussianDistribution(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        return ast.Call("/", [ast.Call("-", [fieldName, ast.LiteralDouble(float(self.mean))]), ast.Call("m.sqrt", [ast.LiteralDouble(float(self.variance))])])

class GeneralRegressionModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class Header(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class INT_Entries(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class INT_SparseArray(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Indices(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class InlineTable(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class InstanceField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class InstanceFields(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Interval(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Item(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ItemRef(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Itemset(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class KNNInput(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class KNNInputs(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class KohonenMap(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Level(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class LiftData(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class LiftGraph(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class LinearKernelType(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class LinearNorm(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class LocalTransformations(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MapValues(PmmlBinding, Expression):
    def toPFA(self, options, context):
        raise NotImplementedError

class MatCell(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Matrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningBuildTask(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningSchema(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MissingValueWeights(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelExplanation(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelLiftGraph(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelVerification(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MultivariateStat(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class MultivariateStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NaiveBayesModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class NearestNeighborModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralInput(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralInputs(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralLayer(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralNetwork(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralOutput(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralOutputs(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Neuron(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Node(PmmlBinding):
    def nodes(self):
        out = []
        for node in self.Node:
            out.append(node)
            out.extend(node.nodes())
        return out

    def predicate(self):
        return (self.SimplePredicate + self.CompoundPredicate + self.SimpleSetPredicate + self.AlwaysTrue + self.AlwaysFalse)[0]

    def simpleWalk(self, context, functionName, splitCharacteristic, predicateTypes):
        if len(self.Node) == 0:
            if functionName == "regression":
                return {"double": float(self.score)}
            else:
                return {"string": self.score}
        else:
            if splitCharacteristic == "binarySplit":
                left, right = self.Node
                if predicateTypes == set(["SimplePredicate"]):
                    fieldName = left.predicate().field
                    if right.predicate().field != fieldName:
                        raise NotImplementedError

                    valueType = context.dataDictionary[fieldName]["type"]

                    lop = left.predicate().operator
                    rop = right.predicate().operator
                    lval = left.predicate().value
                    rval = right.predicate().value

                    if valueType in ("int", "long", "float", "double"):
                        lval, rval = {"double": float(lval)}, {"double": float(rval)}
                    else:
                        lval, rval = {"string": lval}, {"string": rval}

                    if (lop, rop) == ("equal", "notEqual") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": "==",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    elif (lop, rop) == ("notEqual", "equal") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": "!=",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    elif (lop, rop) == ("lessThan", "greaterOrEqual") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": "<",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    elif (lop, rop) == ("lessOrEqual", "greaterThan") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": "<=",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    elif (lop, rop) == ("greaterThan", "lessOrEqual") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": ">",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    elif (lop, rop) == ("greaterOrEqual", "lessThan") and lval == rval:
                        return {"TreeNode": {
                            "field": fieldName,
                            "operator": ">=",
                            "value": lval,
                            "pass": left.simpleWalk(context, functionName, splitCharacteristic, predicateTypes),
                            "fail": right.simpleWalk(context, functionName, splitCharacteristic, predicateTypes)}}
                    else:
                        raise NotImplementedError

            else:
                raise NotImplementedError

    def toPFA(self, options, context):
        raise NotImplementedError

class NormContinuous(PmmlBinding, Expression):
    def toPFA(self, options, context):
        raise NotImplementedError

class NormDiscrete(PmmlBinding, Expression):
    def toPFA(self, options, context):
        raise NotImplementedError

class NormalizedCountTable(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NumericInfo(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class NumericPredictor(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class OptimumLiftGraph(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Output(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class OutputField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PCell(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PCovCell(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PCovMatrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PPCell(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PPMatrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PairCounts(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ParamMatrix(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Parameter(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ParameterField(PmmlBinding, HasDataType):
    def toPFA(self, options, context):
        return {self.name: context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))}

class ParameterList(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Partition(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PartitionFieldStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PoissonDistribution(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class PolynomialKernelType(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PredictiveModelQuality(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Predictor(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class PredictorTerm(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Quantile(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class REAL_Entries(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class REAL_SparseArray(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ROC(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ROCGraph(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RadialBasisKernelType(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RandomLiftGraph(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Regression(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RegressionModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class RegressionTable(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class ResultField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSelectionMethod(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSet(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSetModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class ScoreDistribution(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Scorecard(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class SeasonalTrendDecomposition(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Seasonality_ExpoSmooth(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Segment(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Segmentation(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SelectResult(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Sequence(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceReference(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceRule(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SetPredicate(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SetReference(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SigmoidKernelType(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SimplePredicate(PmmlBinding, Predicate):
    



    def toPFA(self, options, context):
        raise NotImplementedError

class SimpleRule(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SimpleSetPredicate(PmmlBinding, Predicate):
    def toPFA(self, options, context):
        raise NotImplementedError

class SpectralAnalysis(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVector(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectorMachine(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectorMachineModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectors(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TableLocator(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Target(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValue(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueCount(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueCounts(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueStat(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Targets(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Taxonomy(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TestDistributions(PmmlBinding):
    def toPFA(self, options, context):
        if self.testStatistic == "zValue":
            context.outputType = "double"
            return self.Baseline[0].distribution().zValue(context.fieldRef(self.field))
        else:
            raise NotImplementedError

class TextCorpus(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextDictionary(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextDocument(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextIndex(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextIndexNormalization(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModelNormalization(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModelSimiliarity(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Time(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeAnchor(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeCycle(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeException(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeSeries(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeSeriesModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeValue(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Timestamp(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TrainingInstances(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TransformationDictionary(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class TreeModel(PmmlBinding, ModelElement):
    def toPFA(self, options, context):
        if self.splitCharacteristic == "binarySplit":
            topNode = self.Node[0]
            otherNodes = topNode.nodes()

            if not isinstance(topNode.predicate(), AlwaysTrue):
                raise NotImplementedError

            predicateTypes = set(x.predicate().__class__.__name__ for x in otherNodes)

            if predicateTypes == set(["SimplePredicate"]):
                modelData = topNode.simpleWalk(context, self.functionName, self.splitCharacteristic, predicateTypes)

                inputTypes = sorted(set(x["type"] for x in context.dataDictionary.values()))
                if self.functionName == "regression":
                    outputTypes = ["TreeNode", "double"]
                    context.outputType = "double"

                else:
                    outputTypes = ["TreeNode", "string"]
                    context.outputType = "string"

                modelType = {"type": "record", "name": "TreeNode", "fields": [
                    {"name": "field", "type": {"type": "enum", "name": "TreeFields", "symbols": [x.name for x in context.inputType.fields]}},
                    {"name": "operator", "type": "string"},
                    {"name": "value", "type": inputTypes},
                    {"name": "pass", "type": outputTypes},
                    {"name": "fail", "type": outputTypes}
                    ]}

                if context.storageType == "cell":
                    context.cells[context.storageName] = ast.Cell(context.avroTypeBuilder.makePlaceholder(json.dumps(modelType)), json.dumps(modelData), False, False)
                    return [ast.Call("model.tree.simpleWalk", [
                        ast.Ref("input"),
                        ast.CellGet(context.storageName, []),
                        ast.FcnDef([{"d": context.avroTypeBuilder.makePlaceholder('"DataDictionary"')}, {"t": context.avroTypeBuilder.makePlaceholder('"TreeNode"')}],
                                   context.avroTypeBuilder.makePlaceholder('"boolean"'),
                                   [ast.Call("model.tree.simpleTest", [ast.Ref("d"), ast.Ref("t")])])
                        ])]

                elif context.storageType == "pool":
                    poolName, itemName, refName = context.storageName
                    if poolName not in context.pools:
                        context.pools[poolName] = ast.Pool(context.avroTypeBuilder.makePlaceholder(json.dumps(modelType)), {}, False)
                    context.pools[poolName].init[itemName] = json.dumps(modelData)
                    return [ast.Call("model.tree.simpleWalk", [
                        ast.Ref("input"),
                        ast.PoolGet(context.storageName, []),
                        ast.FcnDef([{"d": context.avroTypeBuilder.makePlaceholder('"DataDictionary"')}, {"t": context.avroTypeBuilder.makePlaceholder('"TreeNode"')}],
                                   context.avroTypeBuilder.makePlaceholder('"boolean"'),
                                   [ast.Call("model.tree.simpleTest", [ast.Ref("d"), ast.Ref("t")])])
                        ])]
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

class Trend(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Trend_ExpoSmooth(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class UniformDistribution(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class UnivariateStats(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class Value(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorDictionary(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorFields(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorInstance(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class VerificationField(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class VerificationFields(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class XCoordinates(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class YCoordinates(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class binarySimilarity(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class chebychev(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class cityBlock(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class euclidean(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class jaccard(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class minkowski(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class row(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class simpleMatching(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class squaredEuclidean(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

class tanimoto(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

