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

import sys

class Fcn(object):
    """Trait for a function in PFA: could be a library function, user-defined function, or emit."""
    def genpy(self, paramTypes, args, pos):
        raise NotImplementedError
    def deprecationWarning(self, sig, version):
        pass

class LibFcn(Fcn):
    """Base class for a PFA library function."""
    name = None
    def genpy(self, paramTypes, args, pos):
        """Generate an executable Python string for this function; usually ```self.f["function name"](arguments...)```."""
        return "self.f[{0}]({1})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args))
    def __call__(self, *args):
        """Call this library function; the first two arguments are always ``state`` (titus.genpy.ExecutionState) and ``scope`` (titus.util.DynamicScope)."""
        raise NotImplementedError
    def deprecationWarning(self, sig, version):
        """Write a deprecation warning on standard error if a matched signature is in the deprecated interval of its lifespan, given the requested PFA version.

        :type sig: titus.signature.Sig
        :param sig: the signature (we assume that it has already been matched)
        :type version: titus.signature.PFAVersion
        :param version: the requested PFA version
        :rtype: ``None``
        :return: nothing; if the signature is deprecated, a warning will appear on standard error
        """
        if sig.lifespan.deprecated(version):
            if sig.lifespan.contingency is None:
                contingency = ""
            else:
                contingency = "; " + sig.lifespan.contingency
            sys.stderr.write("WARNING: {0}{1} is deprecated in PFA {2}, will be removed in PFA {3}{4}\n".format(self.name, repr(sig), repr(version), repr(sig.lifespan.death), contingency))
            sys.stderr.flush()
