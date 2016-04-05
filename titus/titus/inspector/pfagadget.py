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

import copy
import json
import StringIO
import re
import urllib

import titus.producer.tools as t
from titus.inspector.defs import *
from titus.util import avscToPretty

class ValidCommand(Command):
    """The 'pfa valid' command in pfainspector."""

    def __init__(self, mode):
        self.name = "valid"
        self.syntax = "valid <name>"
        self.help = "check the validity of a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    """The 'pfa input' and 'pfa output' commands in pfainspector."""

    def __init__(self, mode, which):
        self.name = which
        self.syntax = which + " <name> [pretty=true]"
        self.help = "view the " + which + " schema of a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
                node = model.obj

            else:
                self.syntaxError()

            try:
                node = node[self.name]  # "input" or "output"
            except KeyError:
                raise InspectorError("PFA document \"{0}\" is missing {1} section')".format(args[0].text, self.name))

            if isinstance(node, basestring):
                names = model.engine.parser.names.names.keys()
                if node in names:
                    node = model.engine.parser.getAvroType(node).jsonNode(set())

            if options["pretty"]:
                print avscToPretty(node)
            else:
                t.look(node, inlineDepth=1)

class TypesCommand(Command):
    """The 'pfa types' command in pfainspector."""

    def __init__(self, mode):
        self.name = "types"
        self.syntax = "types <name> [pretty=true]"
        self.help = "list the named types defined in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    """The 'pfa userfcns' command in pfainspector."""

    def __init__(self, mode):
        self.name = "userfcns"
        self.syntax = "userfcns <name> [pretty=true]"
        self.help = "list details about the user functions in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    """The 'pfa calls' command in pfainspector."""

    def __init__(self, mode):
        self.name = "calls"
        self.syntax = "calls <name>"
        self.help = "list PFA functions called by a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    """The 'pfa cells' and 'pfa pools' commands in pfainspector."""

    def __init__(self, mode, which):
        self.name = which
        self.syntax = which + " <name> [pretty=true]"
        self.help = "list details about " + which + " in a named PFA document\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["pretty="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    
class ExternalizeCommand(Command):
    """The 'pfa externalize' command in pfainspector."""

    def __init__(self, mode):
        self.name = "externalize"
        self.syntax = "externalize <name> <cell-or-pool> <filename> [cell=true]"
        self.help = "turn an embedded cell or pool into an external one\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["cell="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif len(words) == 1:
            model = self.mode.pfaFiles.get(words[0].text if isinstance(words[0], parser.Word) else None)
            if model is not None:
                cells = model.obj.get("cells")
                pools = model.obj.get("pools")
                tmp = set()
                if isinstance(cells, dict):
                    tmp.update(cells.keys())
                if isinstance(pools, dict):
                    tmp.update(pools.keys())
                names = sorted(tmp)
            else:
                names = []
            return [x for x in names if x.startswith(active)]

        elif len(words) == 2:
            if isinstance(words[1], parser.Word) and (words[1].text + ".json").startswith(active):
                return [words[1].text + ".json"]
            else:
                return []

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"cell": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["cell"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["cell"], bool):
                raise InspectorError("cell must be boolean")

            if len(args) == 3 and all(isinstance(x, parser.Word) for x in args):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text].obj

                cells = model.get("cells")
                pools = model.get("pools")
                obj = None
                which = ""
                if options["cell"]:
                    if isinstance(cells, dict):
                        obj = cells.get(args[1].text)
                        which = "cell"
                    if obj is None and isinstance(pools, dict):
                        obj = pools.get(args[1].text)
                        which = "pool"
                else:
                    if isinstance(pools, dict):
                        obj = pools.get(args[1].text)
                        which = "pool"
                    if obj is None and isinstance(cells, dict):
                        obj = cells.get(args[1].text)
                        which = "cell"
                if obj is None:
                    raise InspectorError("no cell or pool named {0}".format(args[1].text))

                if obj.get("source", "embedded") != "embedded":
                    raise InspectorError("{0} \"{1}\" is not currently embedded".format(which, args[1].text))

                print "Externalizing {0} \"{1}\" to {2}".format(which, args[1].text, args[2].text)
                json.dump(obj.get("init"), open(args[2].text, "w"))

                obj["source"] = "json"
                obj["init"] = args[2].text

            else:
                self.syntaxError()

class InternalizeCommand(Command):
    """The 'pfa internalize' command in pfainspector."""

    def __init__(self, mode):
        self.name = "internalize"
        self.syntax = "internalize <name> <cell-or-pool> [cell=true]"
        self.help = "turn an external cell or pool into an embedded one\n    " + self.syntax
        self.mode = mode

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

        options = ["cell="]
        words = getcomplete(established)

        if len(words) == 0:
            return sorted(x + " " for x in self.mode.pfaFiles if x.startswith(active))

        elif len(words) == 1:
            model = self.mode.pfaFiles.get(words[0].text if isinstance(words[0], parser.Word) else None)
            if model is not None:
                cells = model.obj.get("cells")
                pools = model.obj.get("pools")
                tmp = set()
                if isinstance(cells, dict):
                    tmp.update(cells.keys())
                if isinstance(pools, dict):
                    tmp.update(pools.keys())
                names = sorted(tmp)
            else:
                names = []
            return [x for x in names if x.startswith(active)]

        elif not words[-1].partial:
            return [x for x in options if x.startswith(active)]

        else:
            return []

    def action(self, args):
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

        if len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            options = {"cell": True}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["cell"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["cell"], bool):
                raise InspectorError("cell must be boolean")

            if len(args) == 2 and all(isinstance(x, parser.Word) for x in args):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text].obj

                cells = model.get("cells")
                pools = model.get("pools")
                obj = None
                which = ""
                if options["cell"]:
                    if isinstance(cells, dict):
                        obj = cells.get(args[1].text)
                        which = "cell"
                    if obj is None and isinstance(pools, dict):
                        obj = pools.get(args[1].text)
                        which = "pool"
                else:
                    if isinstance(pools, dict):
                        obj = pools.get(args[1].text)
                        which = "pool"
                    if obj is None and isinstance(cells, dict):
                        obj = cells.get(args[1].text)
                        which = "cell"
                if obj is None:
                    raise InspectorError("no cell or pool named {0}".format(args[1].text))
        
                if obj.get("source", "embedded") != "json":
                    raise InspectorError("{0} \"{1}\" is not currently json".format(which, args[1].text))

                url = obj.get("init")
                if not isinstance(url, basestring):
                    raise InspectorError("{0} \"{1}\" does not have a string-valued init".format(which, args[1].text))

                print "Internalizing {0} \"{1}\" from {2}".format(which, args[1].text, url)
                if re.match("^[a-zA-Z][a-zA-Z0-9\+\-\.]*://", url) is not None:
                    init = json.load(urllib.urlopen(url))
                else:
                    init = json.load(open(url))

                del obj["source"]
                obj["init"] = init

            else:
                self.syntaxError()

class PFAGadget(Gadget):
    """The 'pfa' gadget in pfainspector."""

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
            ExternalizeCommand(mode),
            InternalizeCommand(mode),
            ])
