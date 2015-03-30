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

import json
import os
import readline
import sys

import titus.version
from titus.inspector.defs import *

class InspectorMode(Mode):
    historyFileName = "inspector.history"
    
    def __init__(self, initialCommands, gadgets):
        self.prompt = "PFA-Inspector> "
        self.dirStack = []
        self.pfaFiles = {}

        class LoadCommand(Command):
            def __init__(self, mode):
                self.name = "load"
                self.syntax = "load <file-path> as <name>"
                self.help = "read a PFA file into the current context, possibly naming it\n    " + self.syntax
                self.mode = mode
            def complete(self, established, active):
                words = getcomplete(established)
                if len(words) == 0:
                    return pathcomplete(established, active)
                elif len(words) == 1 and "as".startswith(active):
                    return ["as "]
                else:
                    return []
            def action(self, args):
                if len(args) == 1 and args[0] == parser.Word("help"):
                    print self.help
                elif len(args) == 3 and isinstance(args[0], parser.FilePath) and args[1] == parser.Word("as") and isinstance(args[2], parser.Word):
                    try:
                        data = json.load(open(args[0].text))
                    except IOError as err:
                        raise InspectorError(err)
                    self.mode.pfaFiles[args[2].text] = Model(data)
                else:
                    self.syntaxError()

        class RenameCommand(Command):
            def __init__(self, mode):
                self.name = "rename"
                self.syntax = "rename <name> as <name>"
                self.help = "change the name of a PFA document\n    " + self.syntax
                self.mode = mode
            def complete(self, established, active):
                words = getcomplete(established)
                if len(words) == 0:
                    return sorted(x + " " for x in self.mode.pfaFiles.keys() if x.startswith(active))
                elif len(words) == 1 and "as".startswith(active):
                    return ["as "]
                else:
                    return []
            def action(self, args):
                if len(args) == 1 and args[0] == parser.Word("help"):
                    print self.help
                elif len(args) == 3 and isinstance(args[0], parser.Word) and args[1] == parser.Word("as") and isinstance(args[2], parser.Word):
                    if args[0].text not in self.mode.pfaFiles:
                        raise InspectorError("no PFA document named \"{0}\" in memory".format(args[0].text))
                    self.mode.pfaFiles[args[2].text] = self.mode.pfaFiles[args[0].text]
                    del self.mode.pfaFiles[args[0].text]
                else:
                    self.syntaxError()

        class DropCommand(Command):
            def __init__(self, mode):
                self.name = "drop"
                self.syntax = "drop <name>"
                self.help = "delete a named PFA document from memory\n    " + self.syntax
                self.mode = mode
            def complete(self, established, active):
                words = getcomplete(established)
                if len(words) == 0:
                    return sorted(x for x in self.mode.pfaFiles.keys() if x.startswith(active))
                else:
                    return []
            def action(self, args):
                if len(args) == 1 and args[0] == parser.Word("help"):
                    print self.help
                elif len(args) == 1 and isinstance(args[0], parser.Word):
                    if args[0].text not in self.mode.pfaFiles:
                        raise InspectorError("no PFA document named \"{0}\" in memory".format(args[0].text))
                    del self.mode.pfaFiles[args[0].text]
                else:
                    self.syntaxError()

        class SaveCommand(Command):
            def __init__(self, mode):
                self.name = "save"
                self.syntax = "save <name> to <file-path>"
                self.help = "save a PFA document from the current context or a named document to a file\n    " + self.syntax
                self.mode = mode
            def complete(self, established, active):
                words = getcomplete(established)
                if len(words) == 0:
                    return sorted(x + " " for x in self.mode.pfaFiles.keys() + ["to"] if x.startswith(active))
                elif len(words) == 1 and "to".startswith(active):
                    return ["to "]
                elif words[-1] == parser.Word("to"):
                    return pathcomplete(established, active)
                else:
                    return []
            def action(self, args):
                if len(args) == 1 and args[0] == parser.Word("help"):
                    print self.help
                elif len(args) == 3 and isinstance(args[0], parser.Word) and args[1] == parser.Word("to") and isinstance(args[2], parser.FilePath):
                    name = args[0].text
                    if name not in self.mode.pfaFiles:
                        raise InspectorError("no PFA document named \"{0}\" in memory".format(name))
                    try:
                        json.dump(self.mode.pfaFiles[name].obj, open(args[2].text, "w"))
                    except IOError as err:
                        raise InspectorError(err)
                else:
                    self.syntaxError()

        commands = [
            SimpleCommand("pwd",
                          os.getcwd,
                          0, 0,
                          help="print name of current/working directory",
                          syntax="pwd"),
            SimpleCommand("cd",
                          lambda x: switch(
                              os.path.isdir(x),
                              lambda: do(self.dirStack.append(os.getcwd()), os.chdir(os.path.expanduser(x)), None),
                              lambda: exception(InspectorError("not a directory"))),
                          1, 1,
                          pathcomplete,
                          help="change the current/working directory to the specified path",
                          syntax="cd <dir>"),
            SimpleCommand("back",
                          lambda: maybe(lambda: os.chdir(self.dirStack.pop()), [(IndexError, "no directories on stack")]),
                          0, 0,
                          help="pop back to a previously visited directory",
                          syntax="back"),
            SimpleCommand("ls",
                          lambda *args: run("ls", *args).rstrip(),
                          0, None,
                          pathcomplete,
                          help="list directory contents",
                          syntax="same as command-line 'ls', including switches"),
            LoadCommand(self),
            SimpleCommand("list",
                          lambda: "\n".join(sorted(x for x in self.pfaFiles)) if len(self.pfaFiles) > 0 else None,
                          0, 0,
                          help="list the named PFA files in memory",
                          syntax="list"),
            RenameCommand(self),
            DropCommand(self),
            SaveCommand(self)]

        # go go gadgets!
        commands.extend([x(self).commandGroup for x in gadgets])

        commands.append(SimpleCommand("exit",
                                      lambda: sys.exit(0),
                                      0, 0,
                                      help="exit the PFA-Inspector (also control-D)",
                                      syntax="exit"))

        self.commandGroup = CommandGroup(None, commands)

        print """Titus PFA Inspector (version {0})
Type 'help' for a list of commands.""".format(titus.version.__version__)

        for initialCommand in initialCommands:
            print initialCommand
            self.action(initialCommand)

        super(InspectorMode, self).__init__()

    def complete(self, established, active):
        return self.commandGroup.complete(established, active)

    def action(self, text):
        try:
            args = parser.parser.parse(text)
        except parser.ParserError as err:
            raise InspectorError(str(err))
        for arg in args:
            try:
                arg.checkPartial(text)
            except parser.ParserError as err:
                raise InspectorError(str(err))
        self.commandGroup.action(args)
