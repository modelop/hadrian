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

def depthGreaterThan(obj, target):
    """Helper function for determining if an object's depth is greater than a given target.

    A string, number, boolean, or null has depth 0, a list or dict of such objects has depth 1, etc.

    :type obj: Pythonized JSON
    :param obj: object to inspect
    :type target: non-negative integer
    :param target: target depth
    :rtype: bool
    :return: ``True`` if the depth of ``obj`` is greater than ``target``; ``False`` otherwise.
    """

    if isinstance(obj, dict):
        return any(depthGreaterThan(x, target - 1) for x in obj.values()) or \
               (len(obj) == 0 and target <= 0)
    elif isinstance(obj, (list, tuple)):
        return any(depthGreaterThan(x, target - 1) for x in obj) or \
               (len(obj) == 0 and target <= 0)
    else:
        return target < 0
        
class LookCommand(Command):
    """The 'json look' command in pfainspector."""

    def __init__(self, mode):
        self.name = "look"
        self.syntax = "look <name> [maxDepth=8] [indexWidth=30]"
        self.help = "look at a named PFA document or subexpression in memory\n    " + self.syntax
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

        options = ["maxDepth=", "indexWidth="]
        words = getcomplete(established)

        if len(words) == 0:
            if active in self.mode.pfaFiles:
                return [active + "["]
            else:
                return sorted(x for x in self.mode.pfaFiles if x.startswith(active))

        elif len(words) == 1 and isinstance(words[0], parser.Extract) and words[0].partial:
            if words[0].text in self.mode.pfaFiles:
                return [x for x in extcomplete(self.mode.pfaFiles[words[0].text].obj, words[0].items) if x.startswith(active)]
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
            options = {"maxDepth": 8, "indexWidth": 30}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["maxDepth", "indexWidth"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {0} unrecognized".format(opt.word.text))

            if not isinstance(options["maxDepth"], (int, long)) or options["maxDepth"] <= 0:
                raise InspectorError("maxDepth must be a positive integer")

            if not isinstance(options["indexWidth"], (int, long)) or options["indexWidth"] <= 0:
                raise InspectorError("indexWidth must be a positive integer")

            if len(args) == 1 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj

            elif len(args) == 1 and isinstance(args[0], parser.Extract):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj
                items = args[0].items
                node = extaction(args[0], node, items)

            else:
                self.syntaxError()

            if not depthGreaterThan(node, 0):
                print json.dumps(node)

            else:
                content = StringIO.StringIO()
                if not depthGreaterThan(node, 1):
                    t.look(node, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=0, stream=content)
                elif not depthGreaterThan(node, 2):
                    t.look(node, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=1, stream=content)
                else:
                    t.look(node, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=2, stream=content)

                content = content.getvalue()
                if content.count("\n") <= 100:
                    print content
                else:
                    proc = pipe("less")
                    try:
                        proc.stdin.write(content)
                    except IOError as err:
                        if str(err) != "[Errno 32] Broken pipe":
                            raise
                    pipewait(proc)

class CountCommand(Command):
    """The 'json count' command in pfainspector."""

    def __init__(self, mode):
        self.name = "count"
        self.syntax = "count <name> <pattern>"
        self.help = "count instances in a PFA document or subexpression that match a regular expression\n    " + self.syntax
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
            if active in self.mode.pfaFiles:
                return [active + "["]
            else:
                return sorted(x for x in self.mode.pfaFiles.keys() if x.startswith(active))

        elif len(words) == 1 and isinstance(words[0], parser.Extract) and words[0].partial:
            if words[0].text in self.mode.pfaFiles:
                return [x for x in extcomplete(self.mode.pfaFiles[words[0].text].obj, words[0].items) if x.startswith(active)]
            else:
                return []

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
            if len(args) == 2 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj

            elif len(args) == 2 and isinstance(args[0], parser.Extract):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj
                items = args[0].items
                node = extaction(args[0], node, items)

            else:
                self.syntaxError()

            regex = args[-1].regex()
            print "{0} matches".format(t.count(regex, node))

class IndexCommand(Command):
    """The 'json index' command in pfainspector."""

    def __init__(self, mode):
        self.name = "index"
        self.syntax = "index <name> <pattern>"
        self.help = "list indexes of a PFA document or subexpression that match a regular expression\n    " + self.syntax
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
            if active in self.mode.pfaFiles:
                return [active + "["]
            else:
                return sorted(x for x in self.mode.pfaFiles.keys() if x.startswith(active))

        elif len(words) == 1 and isinstance(words[0], parser.Extract) and words[0].partial:
            if words[0].text in self.mode.pfaFiles:
                return [x for x in extcomplete(self.mode.pfaFiles[words[0].text].obj, words[0].items) if x.startswith(active)]
            else:
                return []

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
            if len(args) == 2 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj

            elif len(args) == 2 and isinstance(args[0], parser.Extract):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj
                items = args[0].items
                node = extaction(args[0], node, items)

            else:
                self.syntaxError()

            regex = args[-1].regex()
            def display(i):
                if isinstance(i, basestring):
                    if " " in i:
                        return json.dumps(i)
                    else:
                        return i
                else:
                    return str(i)

            print "Indexes that match the pattern:"
            count = 0
            for index in t.indexes(regex, node):
                print "    [" + ", ".join(display(i) for i in index) + "]"
                count += 1
            if count == 0:
                print "    (none)"

class FindCommand(Command):
    """The 'json find' command in pfainspector."""

    def __init__(self, mode):
        self.name = "find"
        self.syntax = "find <name> <pattern> [maxDepth=3] [indexWidth=30]"
        self.help = "show all matches of a regular expression in a PFA document or subexpression\n    " + self.syntax
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

        options = ["maxDepth=", "indexWidth="]
        words = getcomplete(established)

        if len(words) == 0:
            if active in self.mode.pfaFiles:
                return [active + "["]
            else:
                return sorted(x for x in self.mode.pfaFiles if x.startswith(active))

        elif len(words) == 1 and isinstance(words[0], parser.Extract) and words[0].partial:
            if words[0].text in self.mode.pfaFiles:
                return [x for x in extcomplete(self.mode.pfaFiles[words[0].text].obj, words[0].items) if x.startswith(active)]
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
            options = {"maxDepth": 3, "indexWidth": 30}
            while len(args) > 0 and isinstance(args[-1], parser.Option):
                opt = args.pop()
                if opt.word.text in ["maxDepth", "indexWidth"]:
                    try:
                        options[opt.word.text] = opt.value.value()
                    except TypeError:
                        raise InspectorError("illegal value for {0}".format(opt.word.text))
                else:
                    raise InspectorError("option {1} unrecognized".format(opt.word.text))

            if not isinstance(options["maxDepth"], (int, long)) or options["maxDepth"] <= 0:
                raise InspectorError("maxDepth must be a positive integer")

            if not isinstance(options["indexWidth"], (int, long)) or options["indexWidth"] <= 0:
                raise InspectorError("indexWidth must be a positive integer")

            if len(args) == 2 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj

            elif len(args) == 2 and isinstance(args[0], parser.Extract):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                node = self.mode.pfaFiles[args[0].text].obj
                items = args[0].items
                node = extaction(args[0], node, items)

            else:
                self.syntaxError()

            regex = args[-1].regex()
            def display(i):
                if isinstance(i, basestring):
                    if " " in i:
                        return json.dumps(i)
                    else:
                        return i
                else:
                    return str(i)

            content = StringIO.StringIO()
            count = 0
            for index in t.indexes(regex, node):
                content.write("At index [" + ", ".join(display(i) for i in index) + "]:\n")

                matched = t.get(node, index)

                if not depthGreaterThan(matched, 0):
                    content.write(json.dumps(matched) + "\n")
                elif not depthGreaterThan(matched, 1):
                    t.look(matched, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=0, stream=content)
                elif not depthGreaterThan(matched, 2):
                    t.look(matched, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=1, stream=content)
                else:
                    t.look(matched, maxDepth=options["maxDepth"], indexWidth=options["indexWidth"], inlineDepth=2, stream=content)

                content.write("\n")
                count += 1
            if count == 0:
                print "    (none)"

            content = content.getvalue()
            if content.count("\n") <= 100:
                print content
            else:
                proc = pipe("less")
                try:
                    proc.stdin.write(content)
                except IOError as err:
                    if str(err) != "[Errno 32] Broken pipe":
                        raise
                pipewait(proc)

class ChangeCommand(Command):
    """The 'json change' command in pfainspector."""

    def __init__(self, mode):
        self.name = "change"
        self.syntax = "change <name> <pattern> to <replacement>"
        self.help = "replace instances in a PFA document or subexpression that match a regular expression\n    " + self.syntax
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
            if active in self.mode.pfaFiles:
                return [active + "["]
            else:
                return sorted(x for x in self.mode.pfaFiles.keys() if x.startswith(active))

        elif len(words) == 1 and isinstance(words[0], parser.Extract) and words[0].partial:
            if words[0].text in self.mode.pfaFiles:
                return [x for x in extcomplete(self.mode.pfaFiles[words[0].text].obj, words[0].items) if x.startswith(active)]
            else:
                return []

        elif len(words) == 2 and isinstance(words[0], (parser.Word, parser.Extract)) and words[0].text in self.mode.pfaFiles:
            return ["to "]

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
            if len(args) == 4 and isinstance(args[0], parser.Word):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text]
                node = model.obj
                safecopy = copy.deepcopy(node)
                def rollback():
                    self.mode.pfaFiles[args[0].text].obj = safecopy
                regex = args[1].regex()
                replacement = args[3].replacement()

            elif len(args) == 4 and isinstance(args[0], parser.Extract):
                if args[0].text not in self.mode.pfaFiles:
                    raise InspectorError("no PFA document named \"{0}\" in memory (try 'load <file> as {1}')".format(args[0].text, args[0].text))
                model = self.mode.pfaFiles[args[0].text]
                node = model.obj
                safecopy = copy.deepcopy(node)
                def rollback():
                    self.mode.pfaFiles[args[0].text].obj = safecopy
                items = args[0].items
                node = extaction(args[0], node, items)
                regex = args[1].regex()
                replacement = args[3].replacement()

            else:
                self.syntaxError()

            def display(i):
                if isinstance(i, basestring):
                    if " " in i:
                        return json.dumps(i)
                    else:
                        return i
                else:
                    return str(i)

            def replace(value, groups):
                if isinstance(value, parser.Replacement):
                    try:
                        return groups[value.name]
                    except KeyError:
                        raise InspectorError("group ({0}) not found in regular expression".format(value.name))

                elif isinstance(value, dict):
                    return dict((k, replace(v, groups)) for k, v in value.items())

                elif isinstance(value, (list, tuple)):
                    return [replace(x, groups) for x in value]

                else:
                    return value

            def removeAts(obj):
                if isinstance(obj, dict):
                    return dict((k, removeAts(v)) for k, v in obj.items() if k != "@")
                elif isinstance(obj, (list, tuple)):
                    return [removeAts(x) for x in obj]
                else:
                    return obj

            ask = True
            self.mode.pause()
            try:
                for index, match in t.search(regex, node):
                    replacedReplacement = replace(replacement, match.groups)

                    if ask:
                        print "At index [" + ", ".join(display(i) for i in index) + "]:"
                        print "Original:  " + json.dumps(removeAts(t.get(node, index)))
                        print "Change to: " + json.dumps(replacedReplacement)
                        action = None
                        while action is None:
                            response = raw_input("(Y/n/all/stop/revert): ")
                            normalized = response.strip().lower()
                            if normalized in ("", "y", "yes"):
                                action = "yes"
                            elif normalized in ("n", "no"):
                                action = "no"
                            elif normalized == "all":
                                action = "all"
                            elif normalized == "stop":
                                action = "stop"
                            elif normalized == "revert":
                                action = "revert"
                        print

                    else:
                        action = "yes"

                    if action == "yes":
                        t.assign(node, index, replacedReplacement)
                    elif action == "all":
                        t.assign(node, index, replacedReplacement)
                        ask = False
                    elif action == "stop":
                        break
                    elif action == "revert":
                        rollback()
                        break

            except:
                rollback()
                self.mode.resume()
                raise

            else:
                model.reset()
                self.mode.resume()

class JsonGadget(Gadget):
    """The 'json' gadget in pfainspector."""

    def __init__(self, mode):
        self.commandGroup = CommandGroup("json", [
            LookCommand(mode),
            CountCommand(mode),
            IndexCommand(mode),
            FindCommand(mode),
            ChangeCommand(mode)
            ])
