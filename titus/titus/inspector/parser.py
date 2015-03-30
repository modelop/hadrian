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

import ast as pythonast
import re

import ply.lex as lex
import ply.yacc as yacc

import titus.producer.tools as t

class ParserError(Exception):
    def __init__(self, text, col):
        self.text = text
        self.col = col
        super(ParserError, self).__init__("syntax error:  {0}\n--------------{1}^".format(text, "-" * col))
    def __repr__(self):
        return "ParserError({0}, {1})".format(self.text, self.col)

class Ast(object):
    partial = False
    col = None
    def checkPartial(self, text):
        if self.partial:
            raise ParserError(text, self.col)
    def value(self):
        raise TypeError("not a pure value: {0}".format(self))
    def regex(self):
        return self.value()
    def replacement(self):
        return self.value()

class DotDotDot(Ast):
    def __init__(self, col=None):
        self.col = col
    def __repr__(self):
        return "DotDotDot()"
    def __str__(self):
        return "..."
    def __eq__(self, other):
        return isinstance(other, DotDotDot)

class Underscore(Ast):
    def __init__(self, col=None):
        self.col = col
    def __repr__(self):
        return "Underscore()"
    def __str__(self):
        return "_"
    def __eq__(self, other):
        return isinstance(other, Underscore)
    def regex(self):
        return t.Any()

class PlusOrMinus(Ast):
    def __init__(self, col=None):
        self.col = col
    def __repr__(self):
        return "PlusOrMinus()"
    def __str__(self):
        return "+-"
    def __eq__(self, other):
        return isinstance(other, PlusOrMinus)

class FilePath(Ast):
    def __init__(self, text, col=None):
        self.text = text
        self.col = col
    def __repr__(self):
        return "FilePath({0})".format(repr(self.text))
    def __str__(self):
        return self.text
    def __eq__(self, other):
        return isinstance(other, FilePath) and self.text == other.text

class Regex(Ast):
    def __init__(self, text, find, to, flags, col=None):
        self.text = text
        self.find = find
        self.to = to
        self.flags = flags
        self.col = col
    def __repr__(self):
        return "Regex({0}, {1}, {2}, {3})".format(repr(self.text), repr(self.find), repr(self.to), repr(self.flags))
    def __str__(self):
        return self.text
    def __eq__(self, other):
        return isinstance(other, Regex) and self.text == other.text
    def regex(self):
        return t.RegEx(self.find, self.to, self.flags)

class Word(FilePath):
    def __init__(self, text, col=None):
        self.text = text
        self.col = col
    def __repr__(self):
        return "Word({0})".format(repr(self.text))
    def __str__(self):
        return self.text
    def __eq__(self, other):
        return isinstance(other, Word) and self.text == other.text
    def value(self):
        if self.text == "null":
            return None
        elif self.text == "true":
            return True
        elif self.text == "false":
            return False
        else:
            return self.text

class StartExtract(Ast):
    def __init__(self, text, col=None):
        self.text = text
        self.col = col
    def __repr__(self):
        return "StartExtract({0})".format(repr(self.text))
    def __str__(self):
        return self.text
    def __eq__(self, other):
        return isinstance(other, StartExtract) and self.text == other.text

class String(FilePath):
    def __init__(self, text, partial, col=None):
        self.text = text
        self.partial = partial
        self.col = col
    def __repr__(self):
        return "String({0}{1})".format(repr(self.text), " partial" if self.partial else "")
    def __str__(self):
        return repr(self.text)
    def __eq__(self, other):
        return isinstance(other, String) and self.text == other.text
    def value(self):
        return self.text

class Switches(FilePath):
    def __init__(self, text, col=None):
        self.text = text
        self.col = col
    def __repr__(self):
        return "Switches({0})".format(repr(self.text))
    def __str__(self):
        return self.text
    def __eq__(self, other):
        return isinstance(other, Switches) and self.text == other.text

class Floating(Ast):
    def __init__(self, num, col=None):
        self.num = float(num)
        self.col = col
    def __repr__(self):
        return "Floating({0})".format(repr(self.num))
    def __str__(self):
        return str(self.num)
    def __eq__(self, other):
        return isinstance(other, Floating) and self.num == other.num
    def value(self):
        return self.num

class Integer(Floating):
    def __init__(self, num, col=None):
        self.num = int(num)
        self.col = col
    def __repr__(self):
        return "Integer({0})".format(repr(self.num))
    def __str__(self):
        return str(self.num)
    def __eq__(self, other):
        return isinstance(other, Integer) and self.num == other.num
    def value(self):
        return self.num

class JsonObject(Ast):
    def __init__(self, pairs, partial, ellipsis, col=None):
        self.pairs = pairs
        self.partial = partial
        self.ellipsis = ellipsis
        self.col = col
    def __repr__(self):
        return "JsonObject([{0}]{1}{2})".format(", ".join(map(repr, self.pairs)), " partial" if self.partial else "", " ellipsis" if self.ellipsis else "")
    def __str__(self):
        return "{" + ", ".join(map(str, self.pairs)) + (" ..." if self.ellipsis else "") + "}"
    def __eq__(self, other):
        return isinstance(other, JsonObject) and self.pairs == other.pairs
    def checkPartial(self, text):
        super(JsonObject, self).checkPartial(text)
        for pair in self.pairs:
            pair.checkPartial(text)
    def value(self):
        if self.ellipsis:
            super(JsonObject, self).value()
        else:
            return dict((p.key.value(), p.value.value()) for p in self.pairs)
    def regex(self):
        if self.ellipsis:
            return t.Min(**dict((p.key.value(), p.value.regex()) for p in self.pairs))
        else:
            return dict((p.key.value(), p.value.regex()) for p in self.pairs)
    def replacement(self):
        if self.ellipsis:
            super(JsonObject, self).replacement()
        else:
            return dict((p.key.replacement(), p.value.replacement()) for p in self.pairs)

class Pair(Ast):
    def __init__(self, key, value, partial, col=None):
        self.key = key
        self.value = value
        self.partial = partial
        self.col = col
    def __repr__(self):
        return "Pair({0}, {1}{2})".format(repr(self.key), repr(self.value), " partial" if self.partial else "")
    def __str__(self):
        return self.key + ": " + str(self.value)
    def __eq__(self, other):
        return isinstance(other, Pair) and self.key == other.key and self.value == other.value
    def checkPartial(self, text):
        super(Pair, self).checkPartial(text)
        self.key.checkPartial(text)
        self.value.checkPartial(text)

class Approx(Ast):
    def __init__(self, central, error, col=None):
        self.central = central
        self.error = error
        self.col = col
    def __repr__(self):
        return "Approx({0}, {1})".format(repr(self.central), repr(self.error))
    def __str__(self):
        return str(self.central) + " +- " + str(self.error)
    def __eq__(self):
        return isinstance(other, Approx) and self.central == other.central and self.error == other.error
    def regex(self):
        return t.Approx(self.central, self.error)

class Replacement(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return "Replacement(" + self.name + ")"
    def __str__(self):
        return "(" + self.name + ")"

class Group(Ast):
    def __init__(self, item, counter, col=None):
        self.item = item
        self.counter = counter
        self.col = col
    def __repr__(self):
        return "Group({0}, {1})".format(repr(self.item), self.counter)
    def __str__(self):
        return "(" + str(self.item) + ")"
    def __eq__(self):
        return isinstance(other, Group) and self.item == other.item
    def regex(self):
        return t.Group(**{str(self.counter): self.item.regex()})
    def replacement(self):
        if isinstance(self.item, AndChain) and len(self.item.chain) == 1 and isinstance(self.item.chain[0], Integer):
            return Replacement(str(self.item.chain[0].num))
        else:
            super(Group, self).replacement()

class Range(Ast):
    def __init__(self, operator, comparator, col=None):
        self.operator = operator
        self.comparator = comparator
        self.col = col
    def __repr__(self):
        return "Range({0} {1})".format(self.operator, self.comparator)
    def __str__(self):
        return "# " + self.operator + " " + str(self.comparator)
    def __eq__(self):
        return isinstance(other, Range) and self.operator == other.operator and self.comparator == other.comparator
    def regex(self):
        if self.operator is None:
            return t.Any(long, int, float)
        elif self.operator == "<":
            return t.LT(self.comparator)
        elif self.operator == "<=":
            return t.LE(self.comparator)
        elif self.operator == ">":
            return t.GT(self.comparator)
        elif self.operator == ">=":
            return t.GE(self.comparator)
        else:
            raise ValueError

class OrChain(Ast):
    def __init__(self, chain, col=None):
        self.chain = chain
        self.col = col
    def __repr__(self):
        return "OrChain([{0}])".format(", ".join(map(repr, self.chain)))
    def __str__(self):
        return " | ".join(map(str, self.chain))
    def __eq__(self):
        return isinstance(other, OrChain) and self.chain == other.chain
    def regex(self):
        return t.Or(*[x.regex() for x in self.chain])

class AndChain(Ast):
    def __init__(self, chain, col=None):
        self.chain = chain
        self.col = col
    def __repr__(self):
        return "AndChain([{0}])".format(", ".join(map(repr, self.chain)))
    def __str__(self):
        return " & ".join(map(str, self.chain))
    def __eq__(self):
        return isinstance(other, AndChain) and self.chain == other.chain
    def regex(self):
        return t.And(*[x.regex() for x in self.chain])

class JsonArray(Ast):
    def __init__(self, items, partial, startEllipsis, endEllipsis, col=None):
        self.items = items
        self.partial = partial
        self.startEllipsis = startEllipsis
        self.endEllipsis = endEllipsis
        self.col = col
    def __repr__(self):
        return "JsonArray([{0}]{1}{2}{3})".format(", ".join(map(repr, self.items)), " partial" if self.partial else "", " startEllipsis" if self.startEllipsis else "", " endEllipsis" if self.endEllipsis else "")
    def __str__(self):
        return "[" + ("... " if self.startEllipsis else "") + ", ".join(map(str, self.items)) + (" ..." if self.endEllipsis else "") + "]"
    def __eq__(self, other):
        return isinstance(other, JsonArray) and self.items == other.items
    def checkPartial(self, text):
        super(JsonArray, self).checkPartial(text)
        for item in self.items:
            item.checkPartial(text)
    def value(self):
        if self.startEllipsis or self.endEllipsis:
            super(JsonObject, self).value()
        else:
            return [x.value() for x in self.items]
    def value(self):
        if self.startEllipsis and self.endEllipsis:
            raise NotImplementedError
        elif self.startEllipsis and not self.endEllipsis:
            return t.End(*[x.regex() for x in self.items])
        elif not self.startEllipsis and self.endEllipsis:
            return t.Start(*[x.regex() for x in self.items])
        else:
            return [x.regex() for x in self.items]
    def replacement(self):
        if self.startEllipsis or self.endEllipsis:
            super(JsonObject, self).replacement()
        else:
            return [x.replacement() for x in self.items]

class Extract(Ast):
    def __init__(self, text, items, partial, col=None):
        self.text = text
        self.items = items
        self.partial = partial
        self.col = col
    def __repr__(self):
        return "Extract({0}, [{1}]{2})".format(repr(self.text), ", ".join(map(repr, self.items)), " partial" if self.partial else "")
    def __str__(self):
        return str(self.text) + "[" + ", ".join(map(str, self.items)) + "]"
    def strto(self, index):
        return str(self.text) + "[" + ", ".join(map(str, self.items[:index])) + "]"
    def __eq__(self, other):
        return isinstance(other, Extract) and self.text == other.text and self.items == other.items
    def checkPartial(self, text):
        super(Extract, self).checkPartial(text)
        for item in self.items:
            item.checkPartial(text)

class Option(Ast):
    def __init__(self, word, value, partial, col=None):
        self.word = word
        self.value = value
        self.partial = partial
        self.col = col
    def __repr__(self):
        return "Option({0}, {1}{2})".format(repr(self.word), repr(self.value), " partial" if self.partial else "")
    def __str__(self):
        return str(self.word) + "=" + str(self.value)
    def __eq__(self, other):
        return isinstance(other, Option) and self.word == other.word and self.value == other.value
    def checkPartial(self, text):
        super(Option, self).checkPartial(text)
        self.word.checkPartial(text)
        self.value.checkPartial(text)

class Parser(object):
    def __init__(self):
        tokens = ["LT", "LE", "GT", "GE", "DOTDOTDOT", "UNDERSCORE", "PLUSORMINUS", "REGEX", "REGEXSUBS", "FILEPATH", "WORD", "STARTEXTRACT", "STRING", "PARTSTRING", "INTEGER", "FLOATING", "SWITCHES"]
        literals = [",", ":", "[", "]", "{", "}", "=", "(", ")", "|", "&", "#"]

        def t_LE(t):
            r'''<='''
            return t

        def t_LT(t):
            r'''<'''
            return t

        def t_GE(t):
            r'''>='''
            return t

        def t_GT(t):
            r'''>'''
            return t

        def t_DOTDOTDOT(t):
            r'''\.\.\.'''
            t.value = DotDotDot(t.lexpos)
            return t

        def t_UNDERSCORE(t):
            r'''_'''
            t.value = Underscore(t.lexpos)
            return t

        def t_PLUSORMINUS(t):
            r'''\+\-'''
            t.value = PlusOrMinus(t.lexpos)
            return t

        def t_STARTEXTRACT(t):
            r'''[A-Za-z][A-Za-z0-9_\.]*\['''
            t.value = StartExtract(t.value.rstrip("["), t.lexpos)
            return t

        def t_REGEX(t):
            r'''\?(/(([^\\/]|\\.)*)/)([ilmsuxILMSUX]+)?'''
            m = re.match(t_REGEX.__doc__, t.value)
            find = m.groups()[1]
            flags = m.groups()[3]
            t.value = Regex(t.value, find, None, flags, t.lexpos)
            return t

        def t_REGEXSUBS(t):
            r'''@(/(([^\\/]|\\.)*)/)((([^\\/]|\\.)*)/)([ilmsuxILMSUX]+)?'''
            m = re.match(t_REGEXSUBS.__doc__, t.value)
            find = m.groups()[1]
            to = m.groups()[4]
            flags = m.groups()[6]
            t.value = Regex(t.value, find, to, flags, t.lexpos)
            return t

        def t_FILEPATH(t):
            r'''[A-Za-z0-9_\.~@$%\-\+\\]*(/[A-Za-z0-9_\.~@$%\-\+\\]*)+'''
            t.value = FilePath(t.value, t.lexpos)
            return t

        def t_WORD(t):
            r'''[A-Za-z][A-Za-z0-9_\.]*'''
            t.value = Word(t.value, t.lexpos)
            return t

        def t_STRING(t):
            r'''"([^\\"]|\\.)*"'''
            t.value = String(pythonast.literal_eval(t.value), False, t.lexpos)
            return t

        def t_PARTSTRING(t):
            r'''"([^\\"]|\\.)*$'''
            t.value = String(pythonast.literal_eval(t.value), True, t.lexpos)
            return t

        def t_FLOATING(t):
            r'''-?(([0-9]+\.[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?|([0-9]+(\.[0-9]*)?|\.[0-9]+)[eE][-+]?[0-9]+)'''
            t.value = Floating(t.value, t.lexpos)
            return t

        def t_INTEGER(t):
            r'''-?[0-9]+'''
            t.value = Integer(t.value, t.lexpos)
            return t

        def t_SWITCHES(t):
            r'''-[A-Za-z0-9]+'''
            t.value = Switches(t.value, t.lexpos)
            return t

        t_ignore = " \t"

        def t_error(t):
            raise ParserError(self.text, t.lexpos)

        self.lexer = lex.lex()

        def p_expressions(p):
            r'''expressions : expression
                            | expressions expression'''
            if len(p) == 2:
                p[0] = [p[1]]
            else:
                if p[1][-1].partial:
                    raise ParserError(self.text, self.lexer.lexpos)
                p[1].append(p[2])
                p[0] = p[1]

        def p_expression(p):
            r'''expression : value
                           | extract
                           | option
                           | FILEPATH
                           | SWITCHES'''
            p[0] = p[1]

        def p_option(p):
            r'''option : WORD "="
                       | WORD "=" value'''
            p[0] = Option(p[1], p[3] if len(p) == 4 else None, len(p) < 4, self.lexer.lexpos)

        def p_extract(p):
            r'''extract : STARTEXTRACT
                        | STARTEXTRACT indexes
                        | STARTEXTRACT indexes "]"'''
            p[0] = Extract(p[1].text, p[2] if len(p) > 2 else [], len(p) < 4, self.lexer.lexpos)

        def p_index(p):
            r'''index : WORD
                      | STRING
                      | PARTSTRING
                      | INTEGER'''
            p[0] = p[1]

        def p_indexes(p):
            r'''indexes : index
                        | indexes ","
                        | indexes "," index'''
            if len(p) == 2:
                p[0] = [p[1]]
            elif len(p) == 3:
                p[0] = p[1]
            else:
                p[1].append(p[3])
                p[0] = p[1]

        def p_value(p):
            r'''value : WORD
                      | STRING
                      | PARTSTRING
                      | INTEGER
                      | FLOATING
                      | UNDERSCORE
                      | REGEX
                      | REGEXSUBS
                      | jsonobject
                      | jsonarray
                      | range
                      | approx
                      | group'''
            p[0] = p[1]

        def p_values(p):
            r'''values : empty
                       | value
                       | values "," value'''
            if len(p) == 2:
                if p[1] is None:
                    p[0] = []
                else:
                    p[0] = [p[1]]
            else:
                if len(p[1]) > 0:
                    prev = p[1][-1]
                    if prev.partial:
                        raise ParserError(self.text, self.lexer.lexpos)
                p[1].append(p[3])
                p[0] = p[1]

        def p_approx(p):
            r'''approx : INTEGER PLUSORMINUS INTEGER
                       | INTEGER PLUSORMINUS FLOATING
                       | FLOATING PLUSORMINUS INTEGER
                       | FLOATING PLUSORMINUS FLOATING'''
            p[0] = Approx(p[1].num, p[3].num, self.lexer.lexpos)
            
        def p_group(p):
            r'''group : "(" orchain ")"
                      | "(" andchain ")"'''
            p[0] = Group(p[2], self.groupCounter, self.lexer.lexpos)
            self.groupCounter += 1

        def p_range1(p):
            r'''range : "#"'''
            p[0] = Range(None, None, self.lexer.lexpos)

        def p_range2(p):
            r'''range : "#" LT INTEGER
                      | "#" LT FLOATING
                      | "#" LE INTEGER
                      | "#" LE FLOATING
                      | "#" GT INTEGER
                      | "#" GT FLOATING
                      | "#" GE INTEGER
                      | "#" GE FLOATING'''
            p[0] = Range(p[2], p[3].num, self.lexer.lexpos)

        def p_range3(p):
            r'''range : INTEGER  LT "#" LT INTEGER
                      | INTEGER  LT "#" LT FLOATING
                      | FLOATING LT "#" LT INTEGER
                      | FLOATING LT "#" LT FLOATING
                      | INTEGER  LT "#" LE INTEGER
                      | INTEGER  LT "#" LE FLOATING
                      | FLOATING LT "#" LE INTEGER
                      | FLOATING LT "#" LE FLOATING
                      | INTEGER  LE "#" LT INTEGER
                      | INTEGER  LE "#" LT FLOATING
                      | FLOATING LE "#" LT INTEGER
                      | FLOATING LE "#" LT FLOATING
                      | INTEGER  LE "#" LE INTEGER
                      | INTEGER  LE "#" LE FLOATING
                      | FLOATING LE "#" LE INTEGER
                      | FLOATING LE "#" LE FLOATING'''
            reverse = {"<": ">=", "<=": ">"}
            p[0] = AndChain([Range(reverse[p[2]], p[1].num, self.lexer.lexpos),
                             Range(p[4], p[5].num, self.lexer.lexpos)])

        def p_orchain(p):
            r'''orchain : andchain
                        | orchain "|" andchain'''
            if len(p) == 2:
                p[0] = OrChain([p[1]], self.lexer.lexpos)
            else:
                p[1].chain.append(p[3])
                p[1].col = self.lexer.lexpos
                p[0] = p[1]

        def p_andchain(p):
            r'''andchain : value
                         | andchain "&" value'''
            if len(p) == 2:
                p[0] = AndChain([p[1]], self.lexer.lexpos)
            else:
                p[1].chain.append(p[3])
                p[1].col = self.lexer.lexpos
                p[0] = p[1]

        def p_jsonarray(p):
            r'''jsonarray : jsonarraypart "]"
                          | jsonarraypart DOTDOTDOT "]"'''
            p[1].partial = False
            if isinstance(p[2], DotDotDot):
                if p[1].startEllipsis:
                    raise ParserError(self.text, self.lexer.lexpos)
                p[1].endEllipsis = True
            p[0] = p[1]

        def p_jsonarraypart(p):
            r'''jsonarraypart : "[" values
                              | "[" DOTDOTDOT values
                              | "[" values ","
                              | "[" DOTDOTDOT values ","'''
            if isinstance(p[2], DotDotDot):
                p[0] = JsonArray(p[3], partial=True, startEllipsis=True, endEllipsis=False, col=self.lexer.lexpos)
            else:
                p[0] = JsonArray(p[2], partial=True, startEllipsis=False, endEllipsis=False, col=self.lexer.lexpos)

        def p_jsonobject(p):
            r'''jsonobject : jsonobjectpart "}"
                           | jsonobjectpart DOTDOTDOT "}"'''
            p[1].partial = False
            if isinstance(p[2], DotDotDot):
                if p[1].ellipsis:
                    raise ParserError(self.text, self.lexer.lexpos)
                p[1].ellipsis = True
            p[0] = p[1]

        def p_jsonobjectpart(p):
            r'''jsonobjectpart : "{" pairs
                               | "{" DOTDOTDOT pairs
                               | "{" pairs ","
                               | "{" DOTDOTDOT pairs ","'''
            if isinstance(p[2], DotDotDot):
                p[0] = JsonObject(p[3], partial=True, ellipsis=True, col=self.lexer.lexpos)
            else:
                p[0] = JsonObject(p[2], partial=True, ellipsis=False, col=self.lexer.lexpos)

        def p_pairs(p):
            r'''pairs : empty
                      | pair
                      | pairs "," pair'''
            if len(p) == 2:
                if p[1] is None:
                    p[0] = []
                else:
                    p[0] = [p[1]]
            else:
                if len(p[1]) > 0:
                    if p[1][-1].partial:
                        raise ParserError(self.text, self.lexer.lexpos)
                p[1].append(p[3])
                p[0] = p[1]

        def p_pair(p):
            r'''pair : WORD ":"
                     | STRING ":"
                     | WORD ":" value
                     | STRING ":" value'''
            if len(p) < 4:
                p[0] = Pair(p[1], None, True, self.lexer.lexpos)
            else:
                p[0] = Pair(p[1], p[3], False, self.lexer.lexpos)

        def p_empty(p):
            r'''empty : '''

        def p_error(p):
            raise ParserError(self.text, self.lexer.lexpos)

        self.yacc = yacc.yacc(debug=False, write_tables=False)

    def parse(self, text):
        self.text = text
        self.groupCounter = 1
        return self.yacc.parse(text, lexer=self.lexer)

parser = Parser()
