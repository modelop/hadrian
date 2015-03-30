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

class AvroException(RuntimeError): pass
class SchemaParseException(RuntimeError): pass

class PFAException(RuntimeError): pass

class PrettyPfaException(PFAException): pass

class PFASyntaxException(PFAException):
    def __init__(self, message, pos):
        self.pos = pos
        if pos is None or pos == "":
            super(PFASyntaxException, self).__init__("PFA syntax error: " + message)
        else:
            super(PFASyntaxException, self).__init__("PFA syntax error at " + pos + ": " + message)

class PFASemanticException(PFAException):
    def __init__(self, message, pos):
        self.pos = pos
        if pos is None or pos == "":
            super(PFASemanticException, self).__init__("PFA semantic error: " + message)
        else:
            super(PFASemanticException, self).__init__("PFA semantic error at " + pos + ": " + message)

class PFAInitializationException(PFAException):
    def __init__(self, message):
        super(PFAInitializationException, self).__init__("PFA initialization error: " + message)

class PFARuntimeException(PFAException):
    def __init__(self, message):
        self.message = message
        super(PFARuntimeException, self).__init__("PFA runtime error: " + message)

class PFAUserException(PFAException):
    def __init__(self, message, code):
        self.message = message
        self.code = code
        super(PFAUserException, self).__init__("PFA user-defined error: " + message + ("" if code is None else "(code {0})".format(code)))

class PFATimeoutException(PFAException):
    def __init__(self, message):
        super(PFATimeoutException, self).__init__("PFA timeout error: " + message)
