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

import json

import titus.pfaast as ast
from titus.datatype import AvroArray
from titus.datatype import AvroDouble
from titus.datatype import AvroString
from titus.datatype import AvroTypeBuilder
from titus.signature import LabelData
from titus.util import uniqueEngineName, uniqueRecordName, uniqueEnumName

class Context(object):
    """PMML-to-PFA conversion context."""

    def copy(self, **butChange):
        """Copy this context object with a few members changed."""
        out = Context()
        out.__dict__ = dict(self.__dict__)
        out.__dict__.update(butChange)
        return out

    def fieldRef(self, name):
        """Generate a PFA field reference, which may be a direct reference or a member of the input record.
        
        :type name: string
        :param name: name of the field
        :rtype: titus.pfaast.Ref or titus.pfaast.AttrGet
        :return: PFA expression that gets the field
        """
        if name in self.scope:
            return ast.Ref(name)
        elif name in self.dataDictionary:
            return ast.AttrGet(ast.Ref("input"), [ast.LiteralString(name)])
        else:
            raise NameError("unknown field \"{0}\"".format(name))

    def symbolTable(self):
        """Symbol table as a dict from names to type strings."""
        out = {"input": self.inputType}
        out.update(self.scope)
        return out

class PmmlBinding(object):
    """Base class for loaded PMML elements."""

    def __init__(self):
        self.children = []
        self.text = ""
        self.pos = None

    @property
    def tag(self):
        """PMML tag name (string)."""
        return self.__class__.__name__

class ModelElement(object):
    """Trait for PMML ModelElements."""
    def defineFields(self, options, context, transformations):
        """Create PFA ``let`` expressions to define fields.

        :type options: dict of string
        :param options: PMML-to-PFA conversion options
        :type context: titus.pmml.version_independent.Context
        :param context: PMML-to-PFA conversion context
        :type transformations: PMML node with ``<DerivedField>`` elements
        :param transformations: derived fields to convert to ``let`` expressions
        :rtype: list of titus.pfaast.Expression
        :return: PFA ``let`` expressions
        """

        action = []
        for derivedField in transformations.DerivedField:
            name, value = derivedField.toPFA(options, context)
            action.append(ast.Let({name: value}))
        return action

class Expression(object):
    """Trait for PMML Expressions."""
    pass

class Predicate(object):
    """Trait for PMML Predicates."""
    pass

class HasDataType(object):
    """Mixin for PMML nodes that handle data types."""

    def pmmlTypeToAvro(self, dataType=None):
        """Limited PMML type to PFA type converter.

        :type dataType: string or ``None``
        :param dataType: PMML data type name or ``None`` for ``self.dataType``
        :rtype: string
        :return: PFA data type name
        """

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
    """Represents a <PMML> tag and provides methods to convert to PFA."""
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
            merge=None,
            cells=context.cells,
            pools=context.pools,
            randseed=options.get("engine.randseed", None),
            doc=options.get("engine.doc", None),
            version=options.get("engine.version", None),
            metadata=options.get("engine.metadata", {}),
            options=options.get("engine.options", {}))

class ARIMA(PmmlBinding):
    """Represents a <ARIMA> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Aggregate(PmmlBinding, Expression):
    """Represents a <Aggregate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Alternate(PmmlBinding):
    """Represents a <Alternate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AlwaysFalse(PmmlBinding, Predicate):
    """Represents a <AlwaysFalse> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AlwaysTrue(PmmlBinding, Predicate):
    """Represents a <AlwaysTrue> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Annotation(PmmlBinding):
    """Represents a <Annotation> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Anova(PmmlBinding):
    """Represents a <Anova> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AnovaRow(PmmlBinding):
    """Represents a <AnovaRow> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AntecedentSequence(PmmlBinding):
    """Represents a <AntecedentSequence> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AnyDistribution(PmmlBinding):
    """Represents a <AnyDistribution> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class Application(PmmlBinding):
    """Represents a <Application> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Apply(PmmlBinding, Expression):
    """Represents a <Apply> tag and provides methods to convert to PFA."""
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
                return ast.Call("a.min", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

        elif self.function == "max":
            if len(expressions) == 2:
                return ast.Call("max", args)
            else:
                return ast.Call("a.max", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

        elif self.function == "sum":
            if len(expressions) == 2:
                return ast.Call("+", args)
            else:
                return ast.Call("a.sum", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

        elif self.function == "avg":
            return ast.Call("a.mean", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

        elif self.function == "median":
            return ast.Call("a.median", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

        elif self.function == "product":
            if len(expressions) == 2:
                return ast.Call("*", args)
            else:
                return ast.Call("a.product", [ast.NewArray(args, AvroArray(self.broadestType(self.argTypes(args, context))))])

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
                return ast.Call("a.join", [ast.NewArray(args, AvroArray(AvroString())), ast.LiteralString("")])

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
    """Represents a <Array> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AssociationModel(PmmlBinding, ModelElement):
    """Represents a <AssociationModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class AssociationRule(PmmlBinding):
    """Represents a <AssociationRule> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Attribute(PmmlBinding):
    """Represents a <Attribute> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BaseCumHazardTables(PmmlBinding):
    """Represents a <BaseCumHazardTables> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Baseline(PmmlBinding):
    """Represents a <Baseline> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BaselineCell(PmmlBinding):
    """Represents a <BaselineCell> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BaselineModel(PmmlBinding, ModelElement):
    """Represents a <BaselineModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        return [self.TestDistributions[0].toPFA(options, context)]

class BaselineStratum(PmmlBinding):
    """Represents a <BaselineStratum> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesInput(PmmlBinding):
    """Represents a <BayesInput> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesInputs(PmmlBinding):
    """Represents a <BayesInputs> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BayesOutput(PmmlBinding):
    """Represents a <BayesOutput> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BoundaryValueMeans(PmmlBinding):
    """Represents a <BoundaryValueMeans> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class BoundaryValues(PmmlBinding):
    """Represents a <BoundaryValues> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CategoricalPredictor(PmmlBinding):
    """Represents a <CategoricalPredictor> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Categories(PmmlBinding):
    """Represents a <Categories> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Category(PmmlBinding):
    """Represents a <Category> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Characteristic(PmmlBinding):
    """Represents a <Characteristic> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Characteristics(PmmlBinding):
    """Represents a <Characteristics> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ChildParent(PmmlBinding):
    """Represents a <ChildParent> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ClassLabels(PmmlBinding):
    """Represents a <ClassLabels> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Cluster(PmmlBinding):
    """Represents a <Cluster> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringField(PmmlBinding):
    """Represents a <ClusteringField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringModel(PmmlBinding, ModelElement):
    """Represents a <ClusteringModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ClusteringModelQuality(PmmlBinding):
    """Represents a <ClusteringModelQuality> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Coefficient(PmmlBinding):
    """Represents a <Coefficient> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Coefficients(PmmlBinding):
    """Represents a <Coefficients> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ComparisonMeasure(PmmlBinding):
    """Represents a <ComparisonMeasure> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Comparisons(PmmlBinding):
    """Represents a <Comparisons> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ComplexPartialScore(PmmlBinding):
    """Represents a <ComplexPartialScore> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CompoundPredicate(PmmlBinding, Predicate):
    """Represents a <CompoundPredicate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CompoundRule(PmmlBinding):
    """Represents a <CompoundRule> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Con(PmmlBinding):
    """Represents a <Con> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ConfusionMatrix(PmmlBinding):
    """Represents a <ConfusionMatrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ConsequentSequence(PmmlBinding):
    """Represents a <ConsequentSequence> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Constant(PmmlBinding, Expression):
    """Represents a <Constant> tag and provides methods to convert to PFA."""
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
    """Represents a <Constraints> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ContStats(PmmlBinding):
    """Represents a <ContStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationFields(PmmlBinding):
    """Represents a <CorrelationFields> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationMethods(PmmlBinding):
    """Represents a <CorrelationMethods> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CorrelationValues(PmmlBinding):
    """Represents a <CorrelationValues> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Correlations(PmmlBinding):
    """Represents a <Correlations> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CountTable(PmmlBinding):
    """Represents a <CountTable> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Counts(PmmlBinding):
    """Represents a <Counts> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Covariances(PmmlBinding):
    """Represents a <Covariances> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class CovariateList(PmmlBinding):
    """Represents a <CovariateList> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DataDictionary(PmmlBinding):
    """Represents a <DataDictionary> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        context.dataDictionary = {}

        fields = []
        for dataField in self.DataField:
            fields.append(dataField.toPFA(options, context))

        return {"type": "record", "name": "DataDictionary", "fields": fields}

class DataField(PmmlBinding, HasDataType):
    """Represents a <DataField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        context.dataDictionary[self.name] = {"type": self.pmmlTypeToAvro()}
        return {"name": self.name, "type": self.pmmlTypeToAvro()}

class Decision(PmmlBinding):
    """Represents a <Decision> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DecisionTree(PmmlBinding):
    """Represents a <DecisionTree> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Decisions(PmmlBinding):
    """Represents a <Decisions> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DefineFunction(PmmlBinding):
    """Represents a <DefineFunction> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        expressions = [x for x in self.children if isinstance(x, Expression)]

        params = []
        for parameterField in self.ParameterField:
            if parameterField.dataType is None:
                raise TypeError("parameter field dataType needed for field \"{0}\" of function \"{1}\"".format(parameterField.name, self.name))
            params.append(parameterField.toPFA(options, context))

        symbolTable = {}
        for p in params:
            n = list(p.keys())[0]
            v = list(p.values())[0]
            symbolTable[n] = v

        expr = expressions[0].toPFA(options, context.copy(scope=symbolTable))

        inferred = ast.inferType(expr, symbolTable, fcns=context.fcns)

        if self.dataType is not None:
            declared = context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))

            if not declared.accepts(inferred):
                raise TypeError("DefineFunction {0} has inferred type {1} and declared type {2}".format(self.name, repr(inferred), repr(declared)))

            ret = declared

            if not inferred.accepts(declared):
                expr = ast.Upcast(expr, context.avroTypeBuilder.makePlaceholder(json.dumps(self.pmmlTypeToAvro())))

        else:
            ret = inferred

        return self.name, ast.FcnDef(params, ret, [expr])

class Delimiter(PmmlBinding):
    """Represents a <Delimiter> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DerivedField(PmmlBinding, HasDataType):
    """Represents a <DerivedField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        for child in self.children:
            if isinstance(child, Expression):
                expr = child.toPFA(options, context)
                inferred = ast.inferType(expr, context.symbolTable(), fcns=context.fcns)

                if self.dataType is not None:
                    declared = context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))
                    if not declared.accepts(inferred):
                        raise TypeError("DerivedField {0} has inferred type {1} and declared type {2}".format(self.name, repr(inferred), repr(declared)))

                    context.scope[self.name] = declared

                    if not inferred.accepts(declared):
                        expr = ast.Upcast(expr, context.avroTypeBuilder.makePlaceholder(json.dumps(self.pmmlTypeToAvro())))

                else:
                    context.scope[self.name] = inferred

                return self.name, expr

class DiscrStats(PmmlBinding):
    """Represents a <DiscrStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Discretize(PmmlBinding, Expression):
    """Represents a <Discretize> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DiscretizeBin(PmmlBinding):
    """Represents a <DiscretizeBin> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class DocumentTermMatrix(PmmlBinding):
    """Represents a <DocumentTermMatrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class EventValues(PmmlBinding):
    """Represents a <EventValues> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ExponentialSmoothing(PmmlBinding):
    """Represents a <ExponentialSmoothing> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Extension(PmmlBinding):
    """Represents a <Extension> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class FactorList(PmmlBinding):
    """Represents a <FactorList> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldColumnPair(PmmlBinding):
    """Represents a <FieldColumnPair> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldRef(PmmlBinding, Expression):
    """Represents a <FieldRef> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        return context.fieldRef(self.field)

class FieldValue(PmmlBinding):
    """Represents a <FieldValue> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class FieldValueCount(PmmlBinding):
    """Represents a <FieldValueCount> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class GaussianDistribution(PmmlBinding):
    """Represents a <GaussianDistribution> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        return ast.Call("/", [ast.Call("-", [fieldName, ast.LiteralDouble(float(self.mean))]), ast.Call("m.sqrt", [ast.LiteralDouble(float(self.variance))])])

class GeneralRegressionModel(PmmlBinding, ModelElement):
    """Represents a <GeneralRegressionModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Header(PmmlBinding):
    """Represents a <Header> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class INT_Entries(PmmlBinding):
    """Represents a <INT_Entries> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class INT_SparseArray(PmmlBinding):
    """Represents a <INT_SparseArray> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Indices(PmmlBinding):
    """Represents a <Indices> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class InlineTable(PmmlBinding):
    """Represents a <InlineTable> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class InstanceField(PmmlBinding):
    """Represents a <InstanceField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class InstanceFields(PmmlBinding):
    """Represents a <InstanceFields> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Interval(PmmlBinding):
    """Represents a <Interval> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Item(PmmlBinding):
    """Represents a <Item> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ItemRef(PmmlBinding):
    """Represents a <ItemRef> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Itemset(PmmlBinding):
    """Represents a <Itemset> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class KNNInput(PmmlBinding):
    """Represents a <KNNInput> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class KNNInputs(PmmlBinding):
    """Represents a <KNNInputs> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class KohonenMap(PmmlBinding):
    """Represents a <KohonenMap> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Level(PmmlBinding):
    """Represents a <Level> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class LiftData(PmmlBinding):
    """Represents a <LiftData> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class LiftGraph(PmmlBinding):
    """Represents a <LiftGraph> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class LinearKernelType(PmmlBinding):
    """Represents a <LinearKernelType> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class LinearNorm(PmmlBinding):
    """Represents a <LinearNorm> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class LocalTransformations(PmmlBinding):
    """Represents a <LocalTransformations> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MapValues(PmmlBinding, Expression):
    """Represents a <MapValues> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MatCell(PmmlBinding):
    """Represents a <MatCell> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Matrix(PmmlBinding):
    """Represents a <Matrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningBuildTask(PmmlBinding):
    """Represents a <MiningBuildTask> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningField(PmmlBinding):
    """Represents a <MiningField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningModel(PmmlBinding, ModelElement):
    """Represents a <MiningModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MiningSchema(PmmlBinding):
    """Represents a <MiningSchema> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MissingValueWeights(PmmlBinding):
    """Represents a <MissingValueWeights> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelExplanation(PmmlBinding):
    """Represents a <ModelExplanation> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelLiftGraph(PmmlBinding):
    """Represents a <ModelLiftGraph> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelStats(PmmlBinding):
    """Represents a <ModelStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ModelVerification(PmmlBinding):
    """Represents a <ModelVerification> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MultivariateStat(PmmlBinding):
    """Represents a <MultivariateStat> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class MultivariateStats(PmmlBinding):
    """Represents a <MultivariateStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NaiveBayesModel(PmmlBinding, ModelElement):
    """Represents a <NaiveBayesModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NearestNeighborModel(PmmlBinding, ModelElement):
    """Represents a <NearestNeighborModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralInput(PmmlBinding):
    """Represents a <NeuralInput> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralInputs(PmmlBinding):
    """Represents a <NeuralInputs> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralLayer(PmmlBinding):
    """Represents a <NeuralLayer> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralNetwork(PmmlBinding, ModelElement):
    """Represents a <NeuralNetwork> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralOutput(PmmlBinding):
    """Represents a <NeuralOutput> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NeuralOutputs(PmmlBinding):
    """Represents a <NeuralOutputs> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Neuron(PmmlBinding):
    """Represents a <Neuron> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Node(PmmlBinding):
    """Represents a <Node> tag and provides methods to convert to PFA."""
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

            else:
                raise NotImplementedError

    def toPFA(self, options, context):
        raise NotImplementedError

class NormContinuous(PmmlBinding, Expression):
    """Represents a <NormContinuous> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NormDiscrete(PmmlBinding, Expression):
    """Represents a <NormDiscrete> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NormalizedCountTable(PmmlBinding):
    """Represents a <NormalizedCountTable> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NumericInfo(PmmlBinding):
    """Represents a <NumericInfo> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class NumericPredictor(PmmlBinding):
    """Represents a <NumericPredictor> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class OptimumLiftGraph(PmmlBinding):
    """Represents a <OptimumLiftGraph> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Output(PmmlBinding):
    """Represents a <Output> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class OutputField(PmmlBinding):
    """Represents a <OutputField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PCell(PmmlBinding):
    """Represents a <PCell> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PCovCell(PmmlBinding):
    """Represents a <PCovCell> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PCovMatrix(PmmlBinding):
    """Represents a <PCovMatrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PPCell(PmmlBinding):
    """Represents a <PPCell> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PPMatrix(PmmlBinding):
    """Represents a <PPMatrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PairCounts(PmmlBinding):
    """Represents a <PairCounts> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ParamMatrix(PmmlBinding):
    """Represents a <ParamMatrix> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Parameter(PmmlBinding):
    """Represents a <Parameter> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ParameterField(PmmlBinding, HasDataType):
    """Represents a <ParameterField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        return {self.name: context.avroTypeBuilder.resolveOneType(json.dumps(self.pmmlTypeToAvro()))}

class ParameterList(PmmlBinding):
    """Represents a <ParameterList> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Partition(PmmlBinding):
    """Represents a <Partition> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PartitionFieldStats(PmmlBinding):
    """Represents a <PartitionFieldStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PoissonDistribution(PmmlBinding):
    """Represents a <PoissonDistribution> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class PolynomialKernelType(PmmlBinding):
    """Represents a <PolynomialKernelType> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PredictiveModelQuality(PmmlBinding):
    """Represents a <PredictiveModelQuality> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Predictor(PmmlBinding):
    """Represents a <Predictor> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class PredictorTerm(PmmlBinding):
    """Represents a <PredictorTerm> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Quantile(PmmlBinding):
    """Represents a <Quantile> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class REAL_Entries(PmmlBinding):
    """Represents a <REAL_Entries> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class REAL_SparseArray(PmmlBinding):
    """Represents a <REAL_SparseArray> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ROC(PmmlBinding):
    """Represents a <ROC> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ROCGraph(PmmlBinding):
    """Represents a <ROCGraph> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RadialBasisKernelType(PmmlBinding):
    """Represents a <RadialBasisKernelType> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RandomLiftGraph(PmmlBinding):
    """Represents a <RandomLiftGraph> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Regression(PmmlBinding):
    """Represents a <Regression> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RegressionModel(PmmlBinding, ModelElement):
    """Represents a <RegressionModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RegressionTable(PmmlBinding):
    """Represents a <RegressionTable> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ResultField(PmmlBinding):
    """Represents a <ResultField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSelectionMethod(PmmlBinding):
    """Represents a <RuleSelectionMethod> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSet(PmmlBinding):
    """Represents a <RuleSet> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class RuleSetModel(PmmlBinding, ModelElement):
    """Represents a <RuleSetModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class ScoreDistribution(PmmlBinding):
    """Represents a <ScoreDistribution> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Scorecard(PmmlBinding, ModelElement):
    """Represents a <Scorecard> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SeasonalTrendDecomposition(PmmlBinding):
    """Represents a <SeasonalTrendDecomposition> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Seasonality_ExpoSmooth(PmmlBinding):
    """Represents a <Seasonality_ExpoSmooth> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Segment(PmmlBinding):
    """Represents a <Segment> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Segmentation(PmmlBinding):
    """Represents a <Segmentation> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SelectResult(PmmlBinding):
    """Represents a <SelectResult> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Sequence(PmmlBinding):
    """Represents a <Sequence> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceModel(PmmlBinding, ModelElement):
    """Represents a <SequenceModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceReference(PmmlBinding):
    """Represents a <SequenceReference> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SequenceRule(PmmlBinding):
    """Represents a <SequenceRule> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SetPredicate(PmmlBinding):
    """Represents a <SetPredicate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SetReference(PmmlBinding):
    """Represents a <SetReference> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SigmoidKernelType(PmmlBinding):
    """Represents a <SigmoidKernelType> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SimplePredicate(PmmlBinding, Predicate):
    """Represents a <SimplePredicate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SimpleRule(PmmlBinding):
    """Represents a <SimpleRule> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SimpleSetPredicate(PmmlBinding, Predicate):
    """Represents a <SimpleSetPredicate> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SpectralAnalysis(PmmlBinding):
    """Represents a <SpectralAnalysis> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVector(PmmlBinding):
    """Represents a <SupportVector> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectorMachine(PmmlBinding):
    """Represents a <SupportVectorMachine> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectorMachineModel(PmmlBinding, ModelElement):
    """Represents a <SupportVectorMachineModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class SupportVectors(PmmlBinding):
    """Represents a <SupportVectors> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TableLocator(PmmlBinding):
    """Represents a <TableLocator> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Target(PmmlBinding):
    """Represents a <Target> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValue(PmmlBinding):
    """Represents a <TargetValue> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueCount(PmmlBinding):
    """Represents a <TargetValueCount> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueCounts(PmmlBinding):
    """Represents a <TargetValueCounts> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueStat(PmmlBinding):
    """Represents a <TargetValueStat> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TargetValueStats(PmmlBinding):
    """Represents a <TargetValueStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Targets(PmmlBinding):
    """Represents a <Targets> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Taxonomy(PmmlBinding):
    """Represents a <Taxonomy> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TestDistributions(PmmlBinding):
    """Represents a <TestDistributions> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        if self.testStatistic == "zValue":
            context.outputType = "double"
            return self.Baseline[0].distribution().zValue(context.fieldRef(self.field))
        else:
            raise NotImplementedError

class TextCorpus(PmmlBinding):
    """Represents a <TextCorpus> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextDictionary(PmmlBinding):
    """Represents a <TextDictionary> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextDocument(PmmlBinding):
    """Represents a <TextDocument> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextIndex(PmmlBinding):
    """Represents a <TextIndex> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextIndexNormalization(PmmlBinding):
    """Represents a <TextIndexNormalization> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModel(PmmlBinding, ModelElement):
    """Represents a <TextModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModelNormalization(PmmlBinding):
    """Represents a <TextModelNormalization> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TextModelSimiliarity(PmmlBinding):
    """Represents a <TextModelSimiliarity> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Time(PmmlBinding):
    """Represents a <Time> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeAnchor(PmmlBinding):
    """Represents a <TimeAnchor> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeCycle(PmmlBinding):
    """Represents a <TimeCycle> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeException(PmmlBinding):
    """Represents a <TimeException> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeSeries(PmmlBinding):
    """Represents a <TimeSeries> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeSeriesModel(PmmlBinding, ModelElement):
    """Represents a <TimeSeriesModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TimeValue(PmmlBinding):
    """Represents a <TimeValue> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Timestamp(PmmlBinding):
    """Represents a <Timestamp> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TrainingInstances(PmmlBinding):
    """Represents a <TrainingInstances> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TransformationDictionary(PmmlBinding):
    """Represents a <TransformationDictionary> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class TreeModel(PmmlBinding, ModelElement):
    """Represents a <TreeModel> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        topNode = self.Node[0]
        otherNodes = topNode.nodes()

        splitCharacteristic = self.splitCharacteristic
        if self.splitCharacteristic is None:
            if all(len(node.Node) == 0 or len(node.Node) == 2 for node in otherNodes):
                splitCharacteristic = "binarySplit"

        if splitCharacteristic == "binarySplit":
            if not isinstance(topNode.predicate(), AlwaysTrue):
                raise NotImplementedError

            predicateTypes = set(x.predicate().__class__.__name__ for x in otherNodes)

            inputTypes = sorted(set(x["type"] for x in context.dataDictionary.values()))
            if self.functionName == "regression":
                outputTypes = ["TreeNode", "double"]
                context.outputType = "double"

            else:
                outputTypes = ["TreeNode", "string"]
                context.outputType = "string"

            if predicateTypes == set(["SimplePredicate"]):
                modelData = topNode.simpleWalk(context, self.functionName, splitCharacteristic, predicateTypes)["TreeNode"]

                modelType = {"type": "record", "name": "TreeNode", "fields": [
                    {"name": "field", "type": {"type": "enum", "name": "TreeFields", "symbols": [x.name for x in context.inputType.fields]}},
                    {"name": "operator", "type": "string"},
                    {"name": "value", "type": inputTypes},
                    {"name": "pass", "type": outputTypes},
                    {"name": "fail", "type": outputTypes}
                    ]}

                if context.storageType == "cell":
                    context.cells[context.storageName] = ast.Cell(context.avroTypeBuilder.makePlaceholder(json.dumps(modelType)), json.dumps(modelData), False, False, ast.CellPoolSource.EMBEDDED)
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
                        context.pools[poolName] = ast.Pool(context.avroTypeBuilder.makePlaceholder(json.dumps(modelType)), {}, False, ast.CellPoolSource.EMBEDDED)
                    context.pools[poolName].init[itemName] = json.dumps(modelData)
                    return [ast.Call("model.tree.simpleWalk", [
                        ast.Ref("input"),
                        ast.PoolGet(context.storageName, []),
                        ast.FcnDef([{"d": context.avroTypeBuilder.makePlaceholder('"DataDictionary"')}, {"t": context.avroTypeBuilder.makePlaceholder('"TreeNode"')}],
                                   context.avroTypeBuilder.makePlaceholder('"boolean"'),
                                   [ast.Call("model.tree.simpleTest", [ast.Ref("d"), ast.Ref("t")])])
                        ])]

            elif predicateTypes == set(["CompoundPredicate"]) or predicateTypes == set(["SimplePredicate", "CompoundPredicate"]):
                modelData = topNode.simpleWalk(context, self.functionName, splitCharacteristic, predicateTypes)["TreeNode"]

                modelType = {"type": "record", "name": "TreeNode", "fields": [
                    {"name": "operator", "type": "string"},
                    {"name": "comparisons", "type": {"type": "array", "items": {"type": "record", "name": "Comparison", "fields": [
                        {"name": "field", "type": {"type": "enum", "name": "TreeFields", "symbols": [x.name for x in context.inputType.fields]}},
                        {"name": "operator", "type": "string"},
                        {"name": "value", "type": inputTypes}
                    ]}}},
                    {"name": "pass", "type": outputTypes},
                    {"name": "fail", "type": outputTypes}
                    ]}

                if context.storageType == "cell":
                    context.cells[context.storageName] = ast.Cell(context.avroTypeBuilder.makePlaceholder(json.dumps(modelType)), json.dumps(modelData), False, False, ast.CellPoolSource.EMBEDDED)
                    return [ast.Call("model.tree.simpleWalk", [
                        ast.Ref("input"),
                        ast.CellGet(context.storageName, [LiteralString("operator")]),
                        ast.CellGet(context.storageName, [LiteralString("comparisions")]),
                        ast.FcnDef([{"d": context.avroTypeBuilder.makePlaceholder('"DataDictionary"')}, {"c": context.avroTypeBuilder.makePlaceholder('"Comparison"')}],
                                   context.avroTypeBuilder.makePlaceholder('"boolean"'),
                                   [ast.Call("model.tree.simpleTest", [ast.Ref("d"), ast.Ref("c")])])
                        ])]

            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

class Trend(PmmlBinding):
    """Represents a <Trend> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Trend_ExpoSmooth(PmmlBinding):
    """Represents a <Trend_ExpoSmooth> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class UniformDistribution(PmmlBinding):
    """Represents a <UniformDistribution> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

    def zValue(self, fieldName):
        raise NotImplementedError

class UnivariateStats(PmmlBinding):
    """Represents a <UnivariateStats> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class Value(PmmlBinding):
    """Represents a <Value> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorDictionary(PmmlBinding):
    """Represents a <VectorDictionary> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorFields(PmmlBinding):
    """Represents a <VectorFields> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class VectorInstance(PmmlBinding):
    """Represents a <VectorInstance> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class VerificationField(PmmlBinding):
    """Represents a <VerificationField> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class VerificationFields(PmmlBinding):
    """Represents a <VerificationFields> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class XCoordinates(PmmlBinding):
    """Represents a <XCoordinates> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class YCoordinates(PmmlBinding):
    """Represents a <YCoordinates> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class binarySimilarity(PmmlBinding):
    """Represents a <binarySimilarity> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class chebychev(PmmlBinding):
    """Represents a <chebychev> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class cityBlock(PmmlBinding):
    """Represents a <cityBlock> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class euclidean(PmmlBinding):
    """Represents a <euclidean> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class jaccard(PmmlBinding):
    """Represents a <jaccard> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class minkowski(PmmlBinding):
    """Represents a <minkowski> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class row(PmmlBinding):
    """Represents a <row> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class simpleMatching(PmmlBinding):
    """Represents a <simpleMatching> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class squaredEuclidean(PmmlBinding):
    """Represents a <squaredEuclidean> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

class tanimoto(PmmlBinding):
    """Represents a <tanimoto> tag and provides methods to convert to PFA."""
    def toPFA(self, options, context):
        raise NotImplementedError

