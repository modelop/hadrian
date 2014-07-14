#!/usr/bin/env python

from titus.errors import PFAInitializationException

class EngineOptions(object):
    recognizedKeys = set(["@", "timeout", "timeout.begin", "timeout.action", "timeout.end"])

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
