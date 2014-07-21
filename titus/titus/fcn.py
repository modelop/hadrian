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

class Fcn(object):
    def genpy(self, paramTypes, args):
        raise NotImplementedError

class LibFcn(Fcn):
    name = None
    def genpy(self, paramTypes, args):
        return "self.f[{}]({})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args))
    def __call__(self, *args):
        raise NotImplementedError
