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

import atexit
import os
import os.path
import readline
import traceback
import re
import sys
import subprocess
import glob
import math

import titus.inspector.parser as parser
from titus.reader import jsonToAst
from titus.genpy import PFAEngine
from titus.errors import AvroException, SchemaParseException, PFAException

CONFIG_DIRECTORY = "~/.pfa"
CONFIG_DIRECTORY_EXISTS = True

if not os.path.exists(os.path.expanduser(CONFIG_DIRECTORY)):
    if raw_input("Create {0} for configuration files? (Y/n): ".format(CONFIG_DIRECTORY)).upper().strip() in ("Y", ""):
        os.mkdir(os.path.expanduser(CONFIG_DIRECTORY))
    else:
        CONFIG_DIRECTORY_EXISTS = False

CONFIG_COMPLETER_DELIMS = " \t[],:="
readline.set_completer_delims(CONFIG_COMPLETER_DELIMS)

readline.parse_and_bind("tab: complete")
readline.parse_and_bind(r'"\eh": backward-kill-word')
readline.parse_and_bind(r'"\ep": history-search-backward')
readline.parse_and_bind(r'"\en": history-search-forward')

class TabCompleter(object):
    def __init__(self, mode):
        self.mode = mode
        self.candidates = []
    def complete(self, text, state):
        if state == 0:
            line = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            established = line[:begin]
            active = line[begin:end]
            self.candidates = self.mode.complete(established, active)
        try:
            return self.candidates[state]
        except IndexError:
            return None

class InspectorError(Exception): pass

class Gadget(object):
    def __init__(self, mode):
        self.commandGroup = None

def do(*args):
    return args[-1]

def maybe(action, exceptions):
    try:
        action()
    except Exception as err:
        for cls, message in exceptions:
            if isinstance(err, cls):
                raise InspectorError(message)
        raise

def switch(*pairs):
    if len(pairs) % 2 != 1 or len(pairs) < 3:
        raise TypeError
    for predicate, consequent in zip(pairs[:-1][::2], pairs[:-1][1::2]):
        if callable(predicate):
            predicate = predicate()
        if predicate:
            if callable(consequent):
                consequent = consequent()
            return consequent
    alterante = pairs[-1]
    if callable(alterante):
        alterante = alterante()
    return alterante

def exception(x):
    raise x

def getwords(text):
    try:
        return parser.parser.parse(text)
    except parser.ParserError as err:
        raise InspectorError(str(err))

def getcomplete(established):
    if established.strip() == "":
        return []
    else:
        return parser.parser.parse(established)  # let it fail; completer ignores exceptions

def pathcomplete(established, active):
    base, last = os.path.split(os.path.expanduser(active))
    if base == "":
        base = "."
    if os.path.isdir(base):
        if last == "":
            condition = lambda x: not x.startswith(".")
        else:
            condition = lambda x: x.startswith(last)
        def finish(x):
            if " " in x:
                quote = '"'
            else:
                quote = ""
            if os.path.isdir(x):
                return quote + os.path.join(x, "")
            elif os.path.exists(x):
                return quote + x + " "
        return [finish(os.path.join(base, x) if base != "." else x) for x in os.listdir(base) if condition(x)]

def extcomplete(node, items):
    for item in items:
        if isinstance(item, (parser.Word, parser.String)) and isinstance(node, dict):
            if item.text in node:
                node = node[item.text]
            else:
                return []
        elif isinstance(item, parser.Integer) and isinstance(node, (list, tuple)):
            if item.num < len(node):
                node = node[item.num]
            else:
                return []
        else:
            return []

    def quote(x):
        if " " in x:
            return '"' + x + '"'
        else:
            return x

    if isinstance(node, dict):
        return [quote(x) + (", " if isinstance(node[x], (list, tuple, dict)) else "]") for x in node]

    elif isinstance(node, (list, tuple)):
        formatter = "%%0%dd" % int(math.ceil(math.log10(len(node))))
        return [quote(formatter % x) + (", " if isinstance(node[x], (list, tuple, dict)) else "]") for x in xrange(len(node))]

    else:
        return []

def extaction(args0, node, items):
    for index, item in enumerate(items):
        if isinstance(item, (parser.Word, parser.String)):
            try:
                node = node[item.text]
            except (KeyError, TypeError):
                raise InspectorError("{0} has no key {1}".format(args0.strto(index), str(item)))
        elif isinstance(item, parser.Integer):
            try:
                node = node[item.num]
            except (IndexError, TypeError):
                raise InspectorError("{0} has no index {1}".format(args0.strto(index), str(item)))
        else:
            raise InspectorError("syntax error: {0} should not appear in extractor".format(item))

    return node

def run(command, *args):
    whole = [command]
    for arg in args:
        g = glob.glob(arg)
        if len(g) > 0:
            whole.extend(g)
        else:
            whole.append(arg)
    proc = subprocess.Popen(whole, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    returnCode = proc.wait()
    output = proc.stdout.read()
    if returnCode != 0:
        output += "\nsubprocesses failed with exit code {0}".format(returnCode)
    return output

def pipe(command):
    return subprocess.Popen(command, stdin=subprocess.PIPE)

def pipewait(proc):
    proc.stdin.close()
    returnCode = proc.wait()
    if returnCode != 0:
        print "\nsubprocesses failed with exit code {0}".format(returnCode)

class Model(object):
    def __init__(self, obj):
        self.obj = obj
        self._engineConfig = None
        self._engine = None

    def reset(self):
        self._engineConfig = None
        self._engine = None
        
    @property
    def engineConfig(self):
        if self._engineConfig is None:
            try:
                self._engineConfig = jsonToAst(self.obj)
            except (AvroException, SchemaParseException, PFAException) as err:
                raise InspectorError(str(err))
        return self._engineConfig

    @property
    def engine(self):
        if self._engine is None:
            try:
                self._engine, = PFAEngine.fromAst(self.engineConfig)
            except (AvroException, SchemaParseException, PFAException) as err:
                raise InspectorError(str(err))
        return self._engine
        
    def __repr__(self):
        return "Model(" + repr(self.obj) + ")"
    
class Mode(object):
    def __init__(self):
        if CONFIG_DIRECTORY_EXISTS:
            self.historyPath = os.path.join(os.path.expanduser(CONFIG_DIRECTORY), self.historyFileName)
            if not os.path.exists(self.historyPath):
                open(self.historyPath, "w").close()

            self.active = True
            self.tabCompleter = TabCompleter(self)
            readline.read_history_file(self.historyPath)
            readline.set_completer(self.tabCompleter.complete)

            def writehistory():
                if self.active:
                    readline.write_history_file(self.historyPath)
            atexit.register(writehistory)

    def loop(self):
        while True:
            try:
                line = raw_input(self.prompt)
                if line.strip() != "":
                    self.action(line)
            except EOFError:
                print
                sys.exit(0)
            except KeyboardInterrupt:
                print "(use control-D or exit to quit)"
            except InspectorError as err:
                print err
            except Exception as err:
                traceback.print_exc()

    def pause(self):
        readline.write_history_file(self.historyPath)
        readline.clear_history()
        readline.set_completer()
        self.active = False

    def resume(self):
        self.active = True
        readline.read_history_file(self.historyPath)
        readline.set_completer(self.tabCompleter.complete)

class Command(object):
    def __init__(self):
        pass

    def syntaxError(self):
        if self.syntax is None:
            raise InspectorError("syntax error in {0}".format(self.name))
        else:
            raise InspectorError("syntax error, should be: {0}".format(self.syntax))

class SimpleCommand(Command):
    def __init__(self, name, action, minargs=None, maxargs=None, completer=None, help=None, syntax=None):
        self.name = name
        self._action = action
        self.minargs = minargs
        self.maxargs = maxargs
        self.completer = completer
        self.syntax = syntax
        self.help = help + "\n    " + syntax

    def complete(self, established, active):
        out = []
        if self.completer is None:
            pass
        elif callable(self.completer):
            out.extend(self.completer(established, active))
        else:
            for completer in self.completers:
                out.extend(completer(established, active))
        return out

    def action(self, args):
        if self.help is not None and len(args) == 1 and args[0] == parser.Word("help"):
            print self.help
        else:
            for arg in args:
                if not isinstance(arg, parser.FilePath):
                    raise InspectorError("arguments must all be words, not {0}".format(arg))

            if self.minargs is not None and len(args) < self.minargs:
                raise InspectorError("too few arguments (at least {0} are required)".format(self.minargs))
            if self.maxargs is not None and len(args) < self.maxargs:
                raise InspectorError("too many arguments (at most {0} are required)".format(self.maxargs))

            out = self._action(*[x.text for x in args])
            if out is not None:
                print out

class CommandGroup(Command):
    def __init__(self, name, commands):
        self.name = name
        self.commands = dict((x.name, x) for x in commands)
        if name is None:
            self.help = "Commands:\n"
        else:
            self.help = "{0} gadget (type '{1} help' for details)\nSubcommands under {2}:\n".format(name, name, name)
        for x in commands:
            self.help += "    {0:<20s} {1}\n".format(x.name, x.help.split("\n")[0] if x.help is not None else "")
        self.help = self.help.strip()

    def complete(self, established, active):
        word = re.match(r'''\s*([A-Za-z][A-Za-z0-9_]*)\s*''', established)
        if word is None:
            return [x + " " for x in self.commands if x.startswith(active)]
        else:
            try:
                command = self.commands[word.groups()[0]]
            except KeyError:
                return []
            else:
                return command.complete(established[word.end():], active)

    def action(self, args):
        if len(args) == 0:
            if self.name is not None:
                raise InspectorError("command {0} requires a subcommand".format(self.name))

        elif len(args) == 1 and args[0] == parser.Word("help"):
            print self.help

        elif isinstance(args[0], parser.Word):
            try:
                command = self.commands[args[0].text]
            except KeyError:
                if args[0] == parser.Word("help"):
                    raise InspectorError("help commands should end with the word 'help'")
                if self.name is None:
                    raise InspectorError("no command named {0}".format(args[0].text))
                else:
                    raise InspectorError("command {0} has no subcommand {1}".format(self.name, args[0].text))
            else:
                command.action(args[1:])

        else:
            if self.name is None:
                raise InspectorError("command must be a word, not {0}".format(args[0]))
            else:
                raise InspectorError("subcommand of {0} must be a word, not {1}".format(self.name, args[0]))
