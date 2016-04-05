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
    """Handles tab-completion in pfainspector."""

    def __init__(self, mode):
        """:type mode: titus.inspector.defs.Mode
        :param mode: the pfainspector mode in which this tab completer is active
        """

        self.mode = mode
        self.candidates = []

    def complete(self, text, state):
        """:type text: string
        :param text: partial text to complete
        :type state: integer
        :param state: the number of times the user has pressed tab. If ``0``, generate a new list of candidates; otherwise, use the old one
        :rtype: list of strings
        :return: set of completions
        """

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

class InspectorError(Exception):
    """Exception encountered in pfainspector."""
    pass

class Gadget(object):
    """Trait for pfainspector gadgets."""
    def __init__(self, mode):
        self.commandGroup = None

def do(*args):
    """Helper function for chaining function calls in a Python lambda expression.

    Part of a poor-man's functional programming suite used to define several pfainspector commands as one-liners.

    :type args: anything
    :param args: functions that have already been evaluated
    :rtype: anything
    :return: the last argument
    """
    return args[-1]

def maybe(action, exceptions):
    """Helper function for try-catch logic in a Python lambda expression.

    Part of a poor-man's functional programming suite used to define several pfainspector commands as one-liners.

    :type action: callable
    :param action: function to call
    :type exceptions: list of (class, string)
    :param exceptions: exception classes to catch and their corresponding pfainspector messages
    :rtype: anything
    :return: result of calling ``action``
    """

    try:
        action()
    except Exception as err:
        for cls, message in exceptions:
            if isinstance(err, cls):
                raise InspectorError(message)
        raise

def switch(*pairs):
    """Helper function for cond-logic in a Python lambda expression.

    Part of a poor-man's functional programming suite used to define several pfainspector commands as one-liners.

    :type pairs: callables
    :param pairs: sequence of predicate1, consequent1, predicate2, consequent2, ..., alternate; the predicates will each be called in turn until one returns ``True``, and then the corresponding consequent will be called *or* the alternate will be called if none of the predicates return ``True``
    """

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
    """Helper function for raising an exception in a Python lambda expression.

    Part of a poor-man's functional programming suite used to define several pfainspector commands as one-liners.

    :type x: ``Exception``
    :param x: exception to raise (from a function call)
    """

    raise x

def getwords(text):
    """Parse a (partial?) command line into a syntax tree.

    :type text: string
    :param text: text to parse
    :rtype: list of titus.inspector.parser.Ast
    :return: abstract syntax tree for a pfainspector command line
    """

    try:
        return parser.parser.parse(text)
    except parser.ParserError as err:
        raise InspectorError(str(err))

def getcomplete(established):
    """Get the result of tab completion.

    :type established: string
    :param established: text that has been established and is not subject to completion (though it may influence completion as a context)
    :rtype: list of titus.inspector.parser.Ast
    :return: abstract syntax tree of the established part of a pfainspector command line
    """

    if established.strip() == "":
        return []
    else:
        return parser.parser.parse(established)  # let it fail; completer ignores exceptions

def pathcomplete(established, active):
    """Tab completion routine for filesystem paths.

    :type established: string
    :param established: text that has been established and is not subject to completion (everything up to the last ``/``)
    :type active: string
    :param active: text to be completed (anything after the last ``/``)
    :rtype: list of strings
    :return: list of possible completions
    """

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
    """Tab completion routine for JSON extraction (everything after an opening square bracket).

    :type established: string
    :param established: text that has been established and is not subject to completion (everything up to the last ``,``)
    :type active: string
    :param active: text to be completed (anything after the last ``,``)
    :rtype: list of strings
    :return: list of possible completions
    """

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
    """Action for pfainspector extensions (depend on specific commands).

    :type args0: titus.inspector.parser.Extract
    :param args0: abstract syntax tree representation of the object to extract (word before opening square bracket)
    :type node: Pythonized JSON
    :param node: JSON node from which to extract subobjects
    :type items: (integer, titus.inspector.parser.FilePath)
    :param items: extraction path (everything between square brakets, possibly still open)
    :rtype: Pythonized JSON
    :return: result of extracting subobjects
    """

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
    """Helper function to run a subprocess in a Python lambda expression.

    :type command: string
    :param command: external command to run
    :type args: strings
    :param args: arguments for the external command
    :rtype: string
    :return: result of running the command
    """

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
    """Helper function to create a subprocess.Popen as a pipe.

    :type command: list of strings
    :param command: external command with arguments
    :rtype: subprocess.Popen object
    :return: piped subprocess (for further processing)
    """
    return subprocess.Popen(command, stdin=subprocess.PIPE)

def pipewait(proc):
    """Helper function to wait for a subprocess.Popen to complete.

    :type proc: subprocess.Popen
    :param proc: process to wait for
    :rtype: ``None``
    :return: nothing; if the process's return code is not ``0``, a warning message is printed to standard output
    """

    proc.stdin.close()
    returnCode = proc.wait()
    if returnCode != 0:
        print "\nsubprocesses failed with exit code {0}".format(returnCode)

class Model(object):
    """A loaded JSON or PFA file.

    Always has an ``obj`` member, representing the Pythonized JSON that was loaded from the file.

    Lazy-evaluates an ``engineConfig`` member the first time it is requested. This is a titus.pfaast.EngineConfig representing the abstract syntax tree of the PFA file. If the PFA file contains an error, attempting to access the ``engineConfig`` property yields an exception (titus.errors.AvroException, titus.errors.SchemaParseException, or titus.errors.PFAException).

    Lazy-evaluates an ``engine`` member the first time it is requested. This is a titus.genpy.PFAEngine representing an executable scoring engine. If the PFA file contains an error, attempting to access the ``engineConfig`` property yields an exception (titus.errors.AvroException, titus.errors.SchemaParseException, or titus.errors.PFAException).
    """

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
    """A mode of operation for the pfainspector.

    This concrete base class is a read-eval-print loop for responding to the user's commands.
    """

    def __init__(self):
        """Create the main mode.

        If titus.inspector.defs.CONFIG_DIRECTORY_EXISTS is ``True``, get the readline history file from the user's titus.inspector.defs.CONFIG_DIRECTORY.
        """

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
        """Main loop: attempts to evaluate commands.

        An ``EOFError`` exception (user typed control-D on an empty line) quits the pfainspector.

        A ``KeyboardInterrupt`` exception (user typed control-C at any time) jumps to an empty command-line state.

        Any titus.inspector.defs.InspectorError results in the error's message being printed on the screen.

        Any other exception results in a full stack trace being printed on the screen.
        """

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
        """Write the current history to the history file and stop readline."""
        readline.write_history_file(self.historyPath)
        readline.clear_history()
        readline.set_completer()
        self.active = False

    def resume(self):
        """Read the history file and restart readline."""
        self.active = True
        readline.read_history_file(self.historyPath)
        readline.set_completer(self.tabCompleter.complete)

class Command(object):
    """Trait for pfainspector commands."""

    def __init__(self):
        pass

    def syntaxError(self):
        """General behavior for syntax errors in the command parser: raise a titus.inspector.defs.InspectorError."""

        if self.syntax is None:
            raise InspectorError("syntax error in {0}".format(self.name))
        else:
            raise InspectorError("syntax error, should be: {0}".format(self.syntax))

class SimpleCommand(Command):
    """A pfainspector command whose action can be expressed as a Python lambda expression."""

    def __init__(self, name, action, minargs=None, maxargs=None, completer=None, help=None, syntax=None):
        """Create a SimpleCommand.

        :type name: string
        :param name: name of the command
        :type action: callable
        :param action: action to perform, usually supplied as a Python lambda expression.
        :type minargs: non-negative integer or ``None``
        :param minargs: if provided, a minimum legal number of arguments for the command
        :type maxargs: non-negative integer or ``None``
        :param maxargs: if provided, a maximum legal number of arguments for the command
        :type completer: callable
        :param completer: function to call to complete the arguments of the command
        :type help: string
        :param help: message to the user describing the purpose of this command
        :type syntax: string
        :param syntax: message to the user specifying the syntax of this command
        """

        self.name = name
        self._action = action
        self.minargs = minargs
        self.maxargs = maxargs
        self.completer = completer
        self.syntax = syntax
        self.help = help + "\n    " + syntax

    def complete(self, established, active):
        """Handle tab-complete for this command's arguments.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

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
        """Perform the action associated with this command.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
    """A pfainspector command that defers to a group of subcommands."""

    def __init__(self, name, commands):
        """:type name: string
        :param name: name of the group
        :type commands: list of titus.inspector.defs.Command
        :param commands: commands in this group
        """

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
        """Handle tab-complete for the command group: either expanding the subcommand name or deferring to the subcommand's ``complete`` method.

        :type established: string
        :param established: part of the text that has been established
        :type active: string
        :param active: part of the text to be completed
        :rtype: list of strings
        :return: potential completions
        """

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
        """Perform the action associated associated with this command group: descend into the subcommand and call its ``action`` method.

        :type args: list of titus.inspector.parser.Ast
        :param args: arguments passed to the command
        :rtype: ``None``
        :return: nothing; results must be printed to the screen
        """

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
