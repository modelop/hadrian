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

from titus.errors import PFAInitializationException

class EngineOptions(object):
    recognizedKeys = set(["@", "timeout", "timeout.begin", "timeout.action", "timeout.end", "data.PFARecord.interface"])

    def __init__(self, requestedOptions, hostOptions):
        combinedOptions = {} if requestedOptions is None else dict(requestedOptions)
        if hostOptions is not None:
            combinedOptions.update(hostOptions)
        unrecognizedKeys = set(combinedOptions.keys()) - self.recognizedKeys

        if len(unrecognizedKeys) > 0:
            raise PFAInitializationException("unrecognized options: " + " ".join(sorted(unrecognizedKeys)))

        def longOpt(name, default):
            out = combinedOptions.get(name, default)
            try:
                return int(out)
            except ValueError:
                raise PFAInitializationException(name + " must be an integral number")

        self.timeout = longOpt("timeout", -1)
        self.timeout_begin = longOpt("timeout", self.timeout)
        self.timeout_action = longOpt("timeout", self.timeout)
        self.timeout_end = longOpt("timeout", self.timeout)

        # ...
