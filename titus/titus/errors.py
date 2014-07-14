#!/usr/bin/env python

class AvroException(RuntimeError): pass
class SchemaParseException(RuntimeError): pass

class PFAException(RuntimeError): pass

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
        super(PFARuntimeException, self).__init__("PFA runtime error: " + message)

class PFAUserException(PFAException):
    def __init__(self, message, code):
        self.code = code
        super(PFAUserException, self).__init__("PFA user-defined error: " + message + ("" if code is None else "(code {})".format(code)))

class PFATimeoutException(PFAException):
    def __init__(self, message):
        super(PFATimeoutException, self).__init__("PFA timeout error: " + message)
