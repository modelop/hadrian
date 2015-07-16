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

import antinous.util

null = antinous.util.TypeSpecification("null")
boolean = antinous.util.TypeSpecification("boolean")
int = int
long = long
float = float
double = antinous.util.TypeSpecification("double")
bytes = bytes
string = antinous.util.TypeSpecification("string")

class array(antinous.util.TypeSpecification):
    def __init__(self, items):
        self.items = items
    def __repr__(self):
        return "array({})".format(repr(self.items))

class map(antinous.util.TypeSpecification):
    def __init__(self, values):
        self.values = values
    def __repr__(self):
        return "map({})".format(repr(self.values))

class fixed(antinous.util.TypeSpecification):
    def __init__(self, size, _fullName=None):
        if _fullName is None:
            _fullName = antinous.util.uniqueFixedName()
        self.namespace, self.name = antinous.util.splitNamespace(_fullName)
        self.size = size
    @property
    def fullName(self):
        if self.namespace is None:
            return self.name
        else:
            return self.namespace + "." + self.name
    def __repr__(self):
        return "fixed({}, {})".format(repr(self.size), repr(self.fullName))

class enum(antinous.util.TypeSpecification):
    def __init__(self, symbols, _fullName=None):
        if _fullName is None:
            _fullName = antinous.util.uniqueEnumName()
        self.namespace, self.name = antinous.util.splitNamespace(_fullName)
        self.symbols = symbols
    @property
    def fullName(self):
        if self.namespace is None:
            return self.name
        else:
            return self.namespace + "." + self.name
    def __repr__(self):
        return "enum({}, {})".format(repr(self.symbols), repr(self.fullName))

class record(antinous.util.TypeSpecification):
    def __init__(self, _fullName=None, **fields):
        if _fullName is None:
            _fullName = antinous.util.uniqueRecordName()
        self.namespace, self.name = antinous.util.splitNamespace(_fullName)
        self.fields = fields
    @property
    def fullName(self):
        if self.namespace is None:
            return self.name
        else:
            return self.namespace + "." + self.name
    def __repr__(self):
        return "record({}, {})".format(repr(self.fullName), ", ".join("{}={}".format(k, repr(v)) for k, v in self.fields.items()))

class union(antinous.util.TypeSpecification):
    def __init__(self, type, *types):
        self.types = [type] + list(types)
    def __repr__(self):
        return "union({})".format(", ".join(repr(x) for x in self.types))
