#!/usr/bin/env python

class Fcn(object):
    def genpy(self, paramTypes, args):
        raise NotImplementedError

class LibFcn(Fcn):
    name = None
    def genpy(self, paramTypes, args):
        return "self.f[{}]({})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args))
    def __call__(self, *args):
        raise NotImplementedError
