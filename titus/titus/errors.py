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

class AvroException(RuntimeError):
    """Exception for errors in deserializing or serializing Avro data."""
    pass

class SchemaParseException(RuntimeError):
    """Exception for errors in parsing an Avro schema."""
    pass

class PFAException(RuntimeError):
    """Base trait for any exceptions from PFA."""
    pass

class PrettyPfaException(PFAException):
    """Exceptions found in converting PrettyPFA into PFA."""
    pass

class PFASyntaxException(PFAException):
    """Exception for PFA syntax errors (JSON does not describe a PFA scoring engine)."""
    def __init__(self, message, pos):
        """:type message: string
        :param message: syntax error message
        :type pos: string or ``None``
        :param pos: source file position
        """
        self.pos = pos
        if pos is None or pos == "":
            super(PFASyntaxException, self).__init__("PFA syntax error: " + message)
        else:
            super(PFASyntaxException, self).__init__("PFA syntax error " + pos + ": " + message)

class PFASemanticException(PFAException):
    """Exception for PFA semantic errors (higher-level PFA rules are broken, such as type mismatches)."""
    def __init__(self, message, pos):
        """:type message: string
        :param message: semantic error message
        :type pos: string or ``None``
        :param pos: source file position
        """
        self.pos = pos
        if pos is None or pos == "":
            super(PFASemanticException, self).__init__("PFA semantic error: " + message)
        else:
            super(PFASemanticException, self).__init__("PFA semantic error " + pos + ": " + message)

class PFAInitializationException(PFAException):
    """Exception for PFA errors encountered during initialization (when cells/pools are being interpreted, almost ready to start evaluating)."""
    def __init__(self, message):
        """:type message: string
        :param message: initialization error message
        """
        super(PFAInitializationException, self).__init__("PFA initialization error: " + message)

class PFARuntimeException(PFAException):
    """Exception encountered at runtime in a PFA library call."""
    def __init__(self, message, code, fcnName, pos):
        """:type message: string
        :param message: library function error message
        """
        self.message = message
        self.code = code
        self.fcnName = fcnName
        self.pos = pos
        super(PFARuntimeException, self).__init__("{0} in {1} (#{2})".format(message, fcnName, code) + ("" if pos is None else " " + pos))

class PFAUserException(PFAException):
    """Exception deliberately invoked by the PFA author in a ``{"error": "xxx"}``` special form."""
    def __init__(self, message, code, pos):
        """:type message: string
        :param message: user-defined error message
        :type code: integer
        :param code: user-defined error code
        """
        self.message = message
        self.code = code
        self.pos = pos
        super(PFAUserException, self).__init__(message + ("" if code is None else " ({0})".format(code)) + ("" if pos is None else " " + pos))

class PFATimeoutException(PFAException):
    """Exception encountered at runtime from a PFA begin, action, end, or merge process taking too long (possible infinite loop)."""
    def __init__(self, message):
        """:type message: string
        :param message: timeout error message
        """
        super(PFATimeoutException, self).__init__("PFA timeout error: " + message)
