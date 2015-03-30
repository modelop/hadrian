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

import avro.io
import avro.schema

import titus.errors
import titus.util

######################################################### the most general types

class Type(object):
    @property
    def avroType(self): raise TypeError

class FcnType(Type):
    def __init__(self, params, ret):
        self._params = params
        self._ret = ret

    @property
    def params(self):
        return self._params

    @property
    def ret(self):
        return self._ret

    def accepts(self, other):
        if isinstance(self, FcnType):
            return len(self.params) == len(other.params) and \
                   all(y.accepts(x) for x, y in zip(self.params, other.params)) and \
                   self.ret.accepts(other.ret)
        else:
            return False

    def __eq__(self, other):
        if isinstance(other, FcnType):
            return self.params == other.params and self.ret == other.ret
        else:
            return False

    def __repr__(self):
        return '{{"type": "function", "params": [{params}], "ret": {ret}}}'.format(
            params=",".join(repr(x) for x in self.params),
            ret=repr(self.ret))

    def jsonNode(self, memo):
        return {"type": "function", "params": [x.jsonNode(memo) for x in self.params], "ret": self.ret.jsonNode(memo)}

######################################################### Avro types

def schemaToAvroType(schema):
    if schema.type == "null":
        return AvroNull()
    elif schema.type == "boolean":
        return AvroBoolean()
    elif schema.type == "int":
        return AvroInt()
    elif schema.type == "long":
        return AvroLong()
    elif schema.type == "float":
        return AvroFloat()
    elif schema.type == "double":
        return AvroDouble()
    elif schema.type == "bytes":
        return AvroBytes()
    elif schema.type == "fixed":
        out = AvroFixed.__new__(AvroFixed)
        out._schema = schema
        return out
    elif schema.type == "string":
        return AvroString()
    elif schema.type == "enum":
        out = AvroEnum.__new__(AvroEnum)
        out._schema = schema
        return out
    elif schema.type == "array":
        return AvroArray(schemaToAvroType(schema.items))
    elif schema.type == "map":
        return AvroMap(schemaToAvroType(schema.values))
    elif schema.type == "record":
        out = AvroRecord.__new__(AvroRecord)
        out._schema = schema
        return out
    elif schema.type == "union":
        out = AvroUnion.__new__(AvroUnion)
        out._schema = schema
        return out

def avroTypeToSchema(avroType):
    return avroType.schema

class AvroType(Type):
    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return None

    def __eq__(self, other):
        if isinstance(other, AvroType):
            return self.schema == other.schema
        elif isinstance(other, AvroPlaceholder):
            return self.schema == other.avroType.schema
        else:
            return False

    def __hash__(self):
        return hash(self.schema)

    def _recordFieldsOkay(self, other, memo, checkRecord):
        for xf in self.fields:
            if xf.default is None:
                if not any(xf.name == yf.name and xf.avroType.accepts(yf.avroType, memo, checkRecord) for yf in other.fields):
                    return False
            else:
                # not having a matching name in y is fine: x has a default
                # but having a matching name with a mismatched type is bad
                # (spec isn't clear, but org.apache.avro.SchemaCompatibility works that way)
                for yf in other.fields:
                    if xf.name == yf.name:
                        if not xf.avroType.accepts(yf.avroType, memo, checkRecord):
                            return False
        return True

    # self == "reader" (the anticipated signature, pattern to be matched),
    # other == "writer" (the given fact, argument to be accepted or rejected)
    # the special cases handle situations in which Python-Avro fails to be fully covariant
    def accepts(self, other, memo=None, checkRecord=True):
        if isinstance(other, ExceptionType):
            return False

        elif isinstance(self, AvroNull) and isinstance(other, AvroNull):
            return True
        elif isinstance(self, AvroBoolean) and isinstance(other, AvroBoolean):
            return True
        elif isinstance(self, AvroBytes) and isinstance(other, AvroBytes):
            return True
        elif isinstance(self, AvroString) and isinstance(other, AvroString):
            return True

        elif isinstance(self, AvroInt) and isinstance(other, AvroInt):
            return True
        elif isinstance(self, AvroLong) and (isinstance(other, AvroInt) or isinstance(other, AvroLong)):
            return True
        elif isinstance(self, AvroFloat) and (isinstance(other, AvroInt) or isinstance(other, AvroLong) or isinstance(other, AvroFloat)):
            return True
        elif isinstance(self, AvroDouble) and (isinstance(other, AvroInt) or isinstance(other, AvroLong) or isinstance(other, AvroFloat) or isinstance(other, AvroDouble)):
            return True

        elif isinstance(self, AvroArray) and isinstance(other, AvroArray):
            return self.items.accepts(other.items, memo, checkRecord)

        elif isinstance(self, AvroMap) and isinstance(other, AvroMap):
            return self.values.accepts(other.values, memo, checkRecord)

        elif isinstance(self, AvroFixed) and isinstance(other, AvroFixed):
            return self.size == other.size and self.fullName == other.fullName

        elif isinstance(self, AvroEnum) and isinstance(other, AvroEnum):
            return set(other.symbols).issubset(set(self.symbols)) and self.fullName == other.fullName

        elif isinstance(self, AvroRecord) and isinstance(other, AvroRecord):
            if memo is None:
                memo = set()
            else:
                memo = set(memo)

            if self.fullName != other.fullName:
                return False

            elif checkRecord and other.fullName not in memo:
                if not self._recordFieldsOkay(other, memo, checkRecord=False):
                    return False

                memo.add(self.fullName)

                if not self._recordFieldsOkay(other, memo, checkRecord):
                    return False

            return True

        elif isinstance(self, AvroUnion) and isinstance(other, AvroUnion):
            for yt in other.types:
                if not any(xt.accepts(yt, memo, checkRecord) for xt in self.types):
                    return False
            return True

        elif isinstance(self, AvroUnion):
            return any(xt.accepts(other, memo, checkRecord) for xt in self.types)

        elif isinstance(other, AvroUnion):
            return all(self.accepts(yt, memo, checkRecord) for yt in other.types)

        else:
            return False

    def toJson(self):
        return json.dumps(self.jsonNode(set()))

    def jsonNode(self, memo):
        return self.name

    @property
    def avroType(self): return self

    def __repr__(self):
        return json.dumps(self.schema.to_json())

class AvroCompiled(AvroType):
    @property
    def name(self):
        return self.schema.name
    @property
    def namespace(self):
        return self.schema.namespace
    @property
    def fullName(self):
        return self.schema.fullname

class AvroNumber(AvroType): pass
class AvroRaw(AvroType): pass
class AvroIdentifier(AvroType): pass
class AvroContainer(AvroType): pass
class AvroMapping(AvroType): pass

class ExceptionType(AvroType):
    def accepts(self, other):
        return isinstance(other, ExceptionType)
    def __repr__(self):
        return '{"type":"exception"}'
    def jsonNode(self, memo):
        return {"type": "exception"}
    @property
    def schema(self):
        return AvroNull().schema

######################################################### Avro type wrappers

class AvroNull(AvroType):
    _schema = avro.schema.PrimitiveSchema("null")
    @property
    def name(self):
        return "null"

class AvroBoolean(AvroType):
    _schema = avro.schema.PrimitiveSchema("boolean")
    @property
    def name(self):
        return "boolean"

class AvroInt(AvroNumber):
    _schema = avro.schema.PrimitiveSchema("int")
    @property
    def name(self):
        return "int"

class AvroLong(AvroNumber):
    _schema = avro.schema.PrimitiveSchema("long")
    @property
    def name(self):
        return "long"

class AvroFloat(AvroNumber):
    _schema = avro.schema.PrimitiveSchema("float")
    @property
    def name(self):
        return "float"

class AvroDouble(AvroNumber):
    _schema = avro.schema.PrimitiveSchema("double")
    @property
    def name(self):
        return "double"

class AvroBytes(AvroRaw):
    _schema = avro.schema.PrimitiveSchema("bytes")
    @property
    def name(self):
        return "bytes"

class AvroFixed(AvroRaw, AvroCompiled):
    def __init__(self, size, name=None, namespace=None):
        if name is None:
            name = titus.util.uniqueFixedName()
        self._schema = avro.schema.FixedSchema(name, namespace, size, avro.schema.Names())
    @property
    def size(self):
        return self.schema.size
    def jsonNode(self, memo):
        if self.fullName in memo:
            return self.fullName
        else:
            memo.add(self.fullName)
            out = {"type": "fixed", "size": self.size}
            if self.namespace is not None and self.namespace != "":
                out["name"] = self.name
                out["namespace"] = self.namespace
            else:
                out["name"] = self.name
            return out

class AvroString(AvroIdentifier): 
    _schema = avro.schema.PrimitiveSchema("string")
    @property
    def name(self):
        return "string"

class AvroEnum(AvroIdentifier, AvroCompiled):
    def __init__(self, symbols, name=None, namespace=None):
        if name is None:
            name = titus.util.uniqueEnumName()
        self._schema = avro.schema.EnumSchema(name, namespace, symbols, avro.schema.Names())
    @property
    def symbols(self):
        return self.schema.symbols
    def jsonNode(self, memo):
        if self.fullName in memo:
            return self.fullName
        else:
            memo.add(self.fullName)
            out = {"type": "enum", "symbols": self.symbols}
            if self.namespace is not None and self.namespace != "":
                out["name"] = self.name
                out["namespace"] = self.namespace
            else:
                out["name"] = self.name
            return out

class AvroArray(AvroContainer):
    def __init__(self, items):
        self._schema = avro.schema.ArraySchema("null", avro.schema.Names())
        self._schema.set_prop("items", items.schema)
    @property
    def items(self):
        return schemaToAvroType(self.schema.items)
    @property
    def name(self):
        return "array"
    def jsonNode(self, memo):
        return {"type": "array", "items": self.items.jsonNode(memo)}

class AvroMap(AvroContainer, AvroMapping):
    def __init__(self, values):
        self._schema = avro.schema.MapSchema("null", avro.schema.Names())
        self._schema.set_prop("values", values.schema)
    @property
    def values(self):
        return schemaToAvroType(self.schema.values)
    @property
    def name(self):
        return "map"
    def jsonNode(self, memo):
        return {"type": "map", "values": self.values.jsonNode(memo)}

class AvroRecord(AvroContainer, AvroMapping, AvroCompiled):
    def __init__(self, fields, name=None, namespace=None):
        if name is None:
            name = titus.util.uniqueRecordName()
        self._schema = avro.schema.RecordSchema(name, namespace, [], avro.schema.Names(), "record")
        self._schema.set_prop("fields", [x.schema for x in fields])
    @property
    def fields(self):
        return [AvroField.fromSchema(x) for x in self.schema.fields]
    @property
    def fieldsDict(self):
        return dict((x.name, x) for x in self.fields)
    def field(self, name):
        return self.fieldsDict[name]
    @property
    def fieldsDict(self):
        return dict((x.name, AvroField.fromSchema(x)) for x in self.schema.fields)
    def jsonNode(self, memo):
        if self.fullName in memo:
            return self.fullName
        else:
            memo.add(self.fullName)
            out = {"type": "record"}
            if self.namespace is not None and self.namespace != "":
                out["name"] = self.name
                out["namespace"] = self.namespace
            else:
                out["name"] = self.name
            out["fields"] = []
            for field in self.fields:
                out["fields"].append(field.jsonNode(memo))
            return out

class AvroUnion(AvroType):
    def __init__(self, types):
        names = set([x.name for x in types])
        if len(types) != len(names):
            raise titus.errors.AvroException("duplicate in union")
        if "union" in names:
            raise titus.errors.AvroException("nested union")
        self._schema = avro.schema.UnionSchema([], avro.schema.Names())
        self._schema._schemas = [x.schema for x in types]
    @property
    def types(self):
        return [schemaToAvroType(x) for x in self._schema._schemas]
    @property
    def name(self):
        return "union"
    def jsonNode(self, memo):
        return [x.jsonNode(memo) for x in self.types]

class AvroField(object):
    @staticmethod
    def fromSchema(schema):
        out = AvroField.__new__(AvroField)
        out._schema = schema
        return out
    def __init__(self, name, avroType, default=None, order=None):
        self._schema = avro.schema.Field(avroType.schema.to_json(), name, default is not None, default, order, avro.schema.Names())
    @property
    def schema(self):
        return self._schema
    def __repr__(self):
        return json.dumps(self.schema.to_json())
    @property
    def name(self):
        return self.schema.name
    @property
    def avroType(self):
        return schemaToAvroType(self.schema.type)
    @property
    def default(self):
        return self.schema.default
    @property
    def order(self):
        return self.schema.order
    def jsonNode(self, memo):
        out = {"name": self.name, "type": self.avroType.jsonNode(memo)}
        if self.default is not None:
            out["default"] = self.default
        if self.order is not None:
            out["order"] = self.order
        return out

########################### resolving types out of order in streaming input

class AvroPlaceholder(object):
    def __init__(self, original, forwardDeclarationParser):
        self.original = original
        self.forwardDeclarationParser = forwardDeclarationParser
        
    @property
    def avroType(self):
        return self.forwardDeclarationParser.lookup(self.original)

    def __eq__(self, other):
        if isinstance(other, AvroPlaceholder):
            return self.avroType == other.avroType
        elif isinstance(other, AvroType):
            return self.avroType == other
        else:
            return False

    def __hash__(self):
        return hash(self.avroType)

    def __repr__(self):
        if self.forwardDeclarationParser.contains(self.original):
            return repr(self.forwardDeclarationParser.lookup(self.original))
        else:
            return '{"type": "unknown"}'

    def toJson(self):
        return json.dumps(self.jsonNode())

    def jsonNode(self, memo=set()):
        return self.avroType.jsonNode(memo)

    @property
    def parser(self):
        return self.forwardDeclarationParser

class AvroFilledPlaceholder(AvroPlaceholder):
    def __init__(self, avroType):
        self._avroType = avroType

    @property
    def avroType(self):
        return self._avroType

    def __repr__(self):
        return repr(self.avroType)

def parseAvroType(obj):
    return schemaToAvroType(avro.schema.make_avsc_object(obj, avro.schema.Names()))

class ForwardDeclarationParser(object):
    def __init__(self):
        self.names = avro.schema.Names()
        self.lookupTable = {}

    def contains(self, original):
        return original in self.lookupTable

    def lookup(self, original):
        return self.lookupTable[original]

    @property
    def compiledTypes(self):
        return [x for x in self.lookupTable if isinstance(x, (AvroFixed, AvroRecord, AvroEnum))]

    def parse(self, jsonStrings):
        schemae = {}
        unresolvedSize = -1
        lastUnresolvedSize = -1
        errorMessages = {}

        while unresolvedSize != 0:
            for jsonString in jsonStrings:
                if jsonString not in schemae:
                    obj = json.loads(jsonString)

                    if isinstance(obj, basestring) and self.names.has_name(obj, None):
                        gotit = self.names.get_name(obj, None)
                        schemae[jsonString] = gotit
                    else:
                        oldnames = dict(self.names.names)

                        try:
                            gotit = avro.schema.make_avsc_object(obj, self.names)
                        except avro.schema.SchemaParseException as err:
                            self.names.names = oldnames
                            errorMessages[jsonString] = str(err)
                        else:
                            schemae[jsonString] = gotit

            unresolved = [x for x in jsonStrings if x not in schemae]
            unresolvedSize = len(unresolved)

            if unresolvedSize == lastUnresolvedSize:
                raise titus.errors.SchemaParseException("Could not resolve the following types:\n    " +
                    "\n    ".join(["{0} ({1})".format(x, errorMessages[x]) for x in jsonStrings if x not in schemae]))
            else:
                lastUnresolvedSize = unresolvedSize

        result = dict((x, schemaToAvroType(schemae[x])) for x in jsonStrings)
        self.lookupTable.update(result)
        return result

    def getSchema(self, description):
        result = self.getAvroType(description)
        if result is None:
            return None
        else:
            return result.avroType

    def getAvroType(self, description):
        if isinstance(description, basestring):
            if self.names.has_name(description, None):
                return schemaToAvroType(self.names.get_name(description, None))
            elif description == "null":
                return AvroNull()
            elif description == "boolean":
                return AvroBoolean()
            elif description == "int":
                return AvroInt()
            elif description == "long":
                return AvroLong()
            elif description == "float":
                return AvroFloat()
            elif description == "double":
                return AvroDouble()
            elif description == "bytes":
                return AvroBytes()
            elif description == "string":
                return AvroString()
            else:
                try:
                    obj = json.loads(description)
                except ValueError:
                    return None
                else:
                    return self.getSchema(obj)
        elif isinstance(description, dict):
            if description == {"type": "null"}:
                return AvroNull()
            elif description == {"type": "boolean"}:
                return AvroBoolean()
            elif description == {"type": "int"}:
                return AvroInt()
            elif description == {"type": "long"}:
                return AvroLong()
            elif description == {"type": "float"}:
                return AvroFloat()
            elif description == {"type": "double"}:
                return AvroDouble()
            elif description == {"type": "bytes"}:
                return AvroBytes()
            elif description == {"type": "string"}:
                return AvroString()
            elif description.get("type") == "array" and "items" in description:
                return AvroArray(self.getSchema(description["items"]))
            elif description.get("type") == "map" and "values" in description:
                return AvroArray(self.getSchema(description["values"]))
            elif description.get("type") in ("fixed", "enum", "record"):
                if self.names.has_name(description.get("name"), description.get("namespace")):
                    return schemaToAvroType(self.names.get_name(description.get("name"), description.get("namespace")))
                else:
                    raise titus.errors.AvroException("new types, like {0}, cannot be defined with the parser.getAvroType or parser.getSchema methods (use parse)".format(json.dumps(description)))
        elif isinstance(description, (tuple, list)):
            return AvroUnion([self.getSchema(x) for x in description])
        return None

class AvroTypeBuilder(object):
    def __init__(self):
        self.forwardDeclarationParser = ForwardDeclarationParser()
        self.originals = []

    def removeDuplicateNames(self, x, memo):
        if isinstance(x, dict) and "name" in x and "type" in x and x["type"] in ("enum", "fixed", "record"):
            if "namespace" in x:
                name = x["namespace"] + "." + x["name"]
            else:
                name = x["name"]
            if name in memo:
                if memo[name] != x:
                    raise titus.errors.AvroException("type name \"{0}\" previously defined as\n{1}\nnow defined as\n{2}", name, memo[name], x)
                return name
            else:
                memo[name] = x
                return dict((k, self.removeDuplicateNames(v, memo)) for k, v in x.items())

        elif isinstance(x, dict):
            return dict((k, self.removeDuplicateNames(v, memo)) for k, v in x.items())

        elif isinstance(x, (list, tuple)):
            return [self.removeDuplicateNames(v, memo) for v in x]

        else:
            return x

    def makePlaceholder(self, avroJsonString, memo=None):
        if memo is not None:
            avroJsonString = json.dumps(self.removeDuplicateNames(json.loads(avroJsonString), memo))
        self.originals.append(avroJsonString)
        return AvroPlaceholder(avroJsonString, self.forwardDeclarationParser)

    def resolveTypes(self):
        self.forwardDeclarationParser.parse(self.originals)
        self.originals = []

    def resolveOneType(self, avroJsonString):
        return ForwardDeclarationParser().parse([avroJsonString])[avroJsonString]

########################### Avro-Python is missing a JSON decoder

def jsonDecoder(avroType, value):
    if isinstance(avroType, AvroNull):
        if value is None:
            return value
    elif isinstance(avroType, AvroBoolean):
        if value is True or value is False:
            return value
    elif isinstance(avroType, AvroInt):
        try:
            return int(value)
        except ValueError:
            pass
    elif isinstance(avroType, AvroLong):
        try:
            return long(value)
        except ValueError:
            pass
    elif isinstance(avroType, AvroFloat):
        try:
            return float(value)
        except ValueError:
            pass
    elif isinstance(avroType, AvroDouble):
        try:
            return float(value)
        except ValueError:
            pass
    elif isinstance(avroType, AvroBytes):
        if isinstance(value, basestring):
            return bytes(value)
    elif isinstance(avroType, AvroFixed):
        if isinstance(value, basestring):
            out = bytes(value)
            if len(out) == avroType.size:
                return out
    elif isinstance(avroType, AvroString):
        if isinstance(value, basestring):
            return value
    elif isinstance(avroType, AvroEnum):
        if isinstance(value, basestring) and value in avroType.symbols:
            return value
    elif isinstance(avroType, AvroArray):
        if isinstance(value, (list, tuple)):
            return [jsonDecoder(avroType.items, x) for x in value]
    elif isinstance(avroType, AvroMap):
        if isinstance(value, dict):
            return dict((k, jsonDecoder(avroType.values, v)) for k, v in value.items())
    elif isinstance(avroType, AvroRecord):
        if isinstance(value, dict):
            out = {}
            for field in avroType.fields:
                if field.name in value:
                    out[field.name] = jsonDecoder(field.avroType, value[field.name])
                elif field.default is not None:
                    out[field.name] = jsonDecoder(field.avroType, field.default)
                elif isinstance(field.avroType, AvroNull):
                    out[field.name] = None
                else:
                    raise titus.errors.AvroException("{0} does not match schema {1}".format(json.dumps(value), avroType))
            return out
    elif isinstance(avroType, AvroUnion):
        if isinstance(value, dict) and len(value) == 1:
            tag, = value.keys()
            val, = value.values()
            types = dict((x.name, x) for x in avroType.types)
            if tag in types:
                return {tag: jsonDecoder(types[tag], val)}
        elif value is None and "null" in [x.name for x in avroType.types]:
            return None
    else:
        raise Exception
    raise titus.errors.AvroException("{0} does not match schema {1}".format(json.dumps(value), avroType))

########################### check data value against type

try:
    import numpy
    booleanTypes = (numpy.bool_,)
    integerTypes = (numpy.int_,
                    numpy.intc,
                    numpy.intp,
                    numpy.int8,
                    numpy.int16,
                    numpy.int32,
                    numpy.int64,
                    numpy.uint8,
                    numpy.uint16,
                    numpy.uint32,
                    numpy.uint64)
    floatTypes   = integerTypes + \
                   (numpy.float_,
                    numpy.float16,
                    numpy.float32)
except ImportError:
    booleanTypes = ()
    integerTypes = ()
    floatTypes   = ()

def checkData(data, avroType):
    if isinstance(avroType, AvroNull):
        if data == "null":
            data = None
        elif data is None:
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroBoolean):
        if data == "true":
            return True
        elif data == "false":
            return False
        elif isinstance(data, booleanTypes):
            return bool(data)
        elif data is True or data is False:
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroInt):
        if isinstance(data, basestring):
            try:
                data = int(data)
            except ValueError:
                raise TypeError("expecting {0}, found {1}".format(avroType, data))
        elif isinstance(data, integerTypes):
            data = int(data)
        elif isinstance(data, (int, long)):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroLong):
        if isinstance(data, basestring):
            try:
                data = int(data)
            except ValueError:
                raise TypeError("expecting {0}, found {1}".format(avroType, data))
        elif isinstance(data, integerTypes):
            data = int(data)
        elif isinstance(data, (int, long)):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroFloat):
        if isinstance(data, basestring):
            try:
                data = float(data)
            except ValueError:
                raise TypeError("expecting {0}, found {1}".format(avroType, data))
        elif isinstance(data, floatTypes):
            data = float(data)
        elif isinstance(data, (int, long)):
            data = float(data)
        elif isinstance(data, float):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroDouble):
        if isinstance(data, basestring):
            try:
                data = float(data)
            except ValueError:
                raise TypeError("expecting {0}, found {1}".format(avroType, data))
        elif isinstance(data, floatTypes):
            return float(data)
        elif isinstance(data, (int, long)):
            return float(data)
        elif isinstance(data, float):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, (AvroBytes, AvroFixed)):
        if isinstance(data, unicode):
            return data.encode("utf-8", "replace")
        elif isinstance(data, str):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, (AvroString, AvroEnum)):
        if isinstance(data, str):
            return data.decode("utf-8", "replace")
        elif isinstance(data, unicode):
            return data
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroArray):
        if hasattr(data, "__iter__"):
            return [checkData(x, avroType.items) for x in data]
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroMap):
        if hasattr(data, "__iter__") and hasattr(data, "__getitem__"):
            newData = {}
            for key in data:
                value = checkData(data[key], avroType.values)
                if isinstance(key, str):
                    newData[key.decode("utf-8", "replace")] = value
                elif isinstance(key, unicode):
                    newData[key] = value
                else:
                    raise TypeError("expecting {0}, found key {1}".format(avroType, key))
            return newData
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroRecord):
        if hasattr(data, "__iter__") and hasattr(data, "__getitem__"):
            newData = {}
            for field in avroType.fields:
                try:
                    value = data[field.name]
                except KeyError:
                    raise TypeError("expecting {0}, couldn't find key {1}".format(avroType, field.name))
                newData[field.name] = checkData(value, field.avroType)
            return newData
        else:
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

    elif isinstance(avroType, AvroUnion):
        if isinstance(data, dict) and len(data) == 1:
            tag, = data.keys()
            value, = data.values()
            for tpe in avroType.types:
                if tpe.name == tag:
                    if tag == "null":
                        return checkData(value, tpe)
                    else:
                        return {tag: checkData(value, tpe)}
            raise TypeError("expecting {0}, found {1}".format(avroType, data))

        for tpe in avroType.types:
            try:
                newData = checkData(data, tpe)
            except TypeError:
                pass
            else:
                if tpe.name == "null":
                    return newData
                else:
                    return {tpe.name: newData}
        raise TypeError("expecting {0}, found {1}".format(avroType, data))

    return data
