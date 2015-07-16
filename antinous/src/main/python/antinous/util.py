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

import sys

class TypeSpecification(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return self.name

uniqueFixedNameCounter = 0
def uniqueFixedName():
    sys.modules["antinous.util"].uniqueFixedNameCounter += 1
    return "Fixed_{0}".format(sys.modules["antinous.util"].uniqueFixedNameCounter)

uniqueEnumNameCounter = 0
def uniqueEnumName():
    sys.modules["antinous.util"].uniqueEnumNameCounter += 1
    return "Enum_{0}".format(sys.modules["antinous.util"].uniqueEnumNameCounter)

uniqueRecordNameCounter = 0
def uniqueRecordName():
    sys.modules["antinous.util"].uniqueRecordNameCounter += 1
    return "Record_{0}".format(sys.modules["antinous.util"].uniqueRecordNameCounter)

def splitNamespace(fullyQualified):
    splitted = fullyQualified.rsplit(".", 1)
    if len(splitted) == 1:
        return None, fullyQualified
    else:
        return splitted
