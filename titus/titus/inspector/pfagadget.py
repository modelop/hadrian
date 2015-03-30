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

import copy
import json
import StringIO

import titus.producer.tools as t
from titus.inspector.defs import *
from titus.util import avscToPretty

class ValidCommand(Command):
    def __init__(self, mode):
        self.name = "valid"
        self.syntax = "valid <name>"
        self.help = "check the validity of a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help

        elif len(args) == 1 and isinstance(args[0], parser.Word):
            if args[0].text not in self.mode.pfaFiles:
                raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
            model = self.mode.pfaFiles[args[0].text]

        else:
            self.syntaxError()

        model.engine
        print "PFA document is syntactically and semantically valid"
        
class InputOutputCommand(Command):
    def __init__(self, mode, which):
        self.name = which
        self.syntax = which + " <name> [pretty=true]"
        self.help = "view the " + which + " schema of a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"pretty": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["pretty"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["pretty"], bool):
                raise InspectorError("pretty must be boolean")

            if len(args) == 1 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj

            else:
                self.syntaxError()

            try:
                node = node[self.name]  # "input" or "output"
            except KeyError:
                raise InspectorError("PFA document \"{0}\" is missing {1} section')".format(args[0].text, self.name))

            if options["pretty"]:
                print avscToPretty(node)
            else:
                t.look(node, inlineDepth=1)

class TypesCommand(Command):
    def __init__(self, mode):
        self.name = "types"
        self.syntax = "types <name> [pretty=true]"
        self.help = "list the named types defined in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"pretty": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["pretty"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["pretty"], bool):
                raise InspectorError("pretty must be boolean")

            if len(args) == 1 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text]

            else:
                self.syntaxError()

            names = sorted(model.engine.parser.names.names.keys())
            for index, name in enumerate(names):
                node = model.engine.parser.getAvroType(name).jsonNode(set())
                print name + ":"
                if options["pretty"]:
                    print avscToPretty(node, 4)
                else:
                    t.look(node, inlineDepth=1)
                if index != len(names) - 1:
                    print

            if len(names) == 0:
                print "PFA document contains no named types"
        
class UserFcnsCommand(Command):
    def __init__(self, mode):
        self.name = "userfcns"
        self.syntax = "userfcns <name> [pretty=true]"
        self.help = "list details about the user functions in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"pretty": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["pretty"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["pretty"], bool):
                raise InspectorError("pretty must be boolean")

            if len(args) == 1 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text]

            else:
                self.syntaxError()

            names = sorted(model.engineConfig.fcns)
            for index, name in enumerate(names):
                fcn = model.engineConfig.fcns[name]
                print "u." + name + ":"
                print "    parameters:"
                for pname in fcn.paramNames:
                    ptype = fcn.params[pname]
                    if options["pretty"]:
                        print "        " + pname + ": " + avscToPretty(ptype.jsonNode(set()), 10 + len(pname)).lstrip()
                    else:
                        print "        " + pname + ": " + ptype.toJson()

                if options["pretty"]:
                    print "    returns " + avscToPretty(fcn.ret.jsonNode(set()), 12).lstrip()
                else:
                    print "    returns " + fcn.ret.toJson()

                if model.engine.isRecursive("u." + name):
                    print "    recursive"
                elif model.engine.hasRecursive("u." + name):
                    print "    can call a recursive function"
                else:
                    print "    call depth: " + str(model.engine.callDepth("u." + name))
                if model.engine.hasSideEffects("u." + name):
                    print "    can modify a cell or pool"

                calledBy = sorted(model.engine.calledBy("u." + name))
                print "    called by: " + ", ".join(calledBy) if len(calledBy) > 0 else "(none)"

                if index != len(names) - 1:
                    print

            if len(names) == 0:
                print "PFA document contains no user functions"
        
class CallsCommand(Command):
    def __init__(self, mode):
        self.name = "calls"
        self.syntax = "calls <name>"
        self.help = "list PFA functions called by a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help

        elif len(args) == 1 and isinstance(args[0], parser.Word):
            if args[0].text not in self.mode.pfaFiles:
                raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
            model = self.mode.pfaFiles[args[0].text]

        else:
            self.syntaxError()

        for name in sorted(model.engine.callGraph):
            print name + ": " + ", ".join(sorted(model.engine.callGraph[name]))

class CellsPoolsCommand(Command):
    def __init__(self, mode, which):
        self.name = which
        self.syntax = which + " <name> [pretty=true]"
        self.help = "list details about " + which + " in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"pretty": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["pretty"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["pretty"], bool):
                raise InspectorError("pretty must be boolean")

            if len(args) == 1 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text]

            else:
                self.syntaxError()

            if self.name == "cells":
                node = model.engineConfig.cells
            elif self.name == "pools":
                node = model.engineConfig.pools

            names = sorted(node.keys())
            for index, name in enumerate(names):
                obj = node[name]
                if self.name == "cells":
                    preamble = "{0}: shared={1} rollback={2} type=".format(name, json.dumps(obj.shared), json.dumps(obj.rollback))
                elif self.name == "pools":
                    preamble = "{0}: shared={1} rollback={2} elements={3} type=".format(name, json.dumps(obj.shared), json.dumps(obj.rollback), len(json.loads(obj.init)))

                ptype = obj.avroType
                if options["pretty"]:
                    print preamble + avscToPretty(ptype.jsonNode(set()), len(preamble)).lstrip()
                else:
                    print preamble + ptype.toJson()

                if index != len(names) - 1:
                    print

            if len(names) == 0:
                print "PFA document contains no " + self.name

class PFAGadget(Gadget):
    def __init__(self, mode):
        self.commandGroup = CommandGroup("pfa", [
            ValidCommand(mode),
            InputOutputCommand(mode, "input"),
            InputOutputCommand(mode, "output"),
            TypesCommand(mode),
            UserFcnsCommand(mode),
            CallsCommand(mode),
            CellsPoolsCommand(mode, "cells"),
            CellsPoolsCommand(mode, "pools"),
            ])
