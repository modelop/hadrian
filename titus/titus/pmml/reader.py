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

import gzip
import io
import xml.sax
import xml.sax.handler

from titus.datatype import AvroTypeBuilder
import titus.pmml.version_independent
import titus.pmml.version_3_2
import titus.pmml.version_4_0
import titus.pmml.version_4_1
import titus.pmml.version_4_2

class PmmlContentHandler(xml.sax.handler.ContentHandler):
    """Streaming XML reader for loading PMML: methods handle SAX events (see xml.sax.handler.ContentHandler)."""

    def __init__(self):
        self.stack = []
        self.version = None
        self.namespace = None
        self.tagToClass = None
        self.result = None

    def startElement(self, name, attrib):
        if self.version is None:
            if name == "PMML":
                self.setVersion(attrib["version"])
            else:
                raise ValueError("top-level element must be named PMML")

        try:
            newElement = self.tagToClass[name](attrib)
        except KeyError:
            self.stack.append(None)
        else:
            if hasattr(self.parser, "getLineNumber"):
                newElement.pos = "PMML line " + repr(self.parser.getLineNumber())

            if len(self.stack) > 0:
                self.stack[-1].children.append(newElement)
                where = getattr(self.stack[-1], newElement.__class__.__name__, None)
                if where is not None:
                    where.append(newElement)
                else:
                    raise ValueError("cannot place {0} under {1} in PMML {2}".format(name, self.stack[-1].tag, self.version))

            self.stack.append(newElement)

    def startElementNS(self, name, qname, attrib):
        if self.version is None:
            if name[1] == "PMML":
                self.setVersion([v for k, v in attrib.items() if k[1] == "version"][0])
            else:
                raise ValueError("top-level element must be named PMML")

        if name[0] is not None and name[0].startswith(self.namespace):
            self.startElement(name[1], dict((k[1], v) for k, v in attrib.items()))

    def setVersion(self, version):
        self.version = version
        if version == "3.2":
            self.namespace = titus.pmml.version_3_2.namespace
            self.tagToClass = titus.pmml.version_3_2.tagToClass
        elif version == "4.0" or version == "4.0.1":
            self.namespace = titus.pmml.version_4_0.namespace
            self.tagToClass = titus.pmml.version_4_0.tagToClass
        elif version == "4.1":
            self.namespace = titus.pmml.version_4_1.namespace
            self.tagToClass = titus.pmml.version_4_1.tagToClass
        elif version == "4.2":
            self.namespace = titus.pmml.version_4_2.namespace
            self.tagToClass = titus.pmml.version_4_2.tagToClass
        else:
            raise NotImplementedError("PMML version {0} is not supported (only 3.2, 4.0, 4.0.1, 4.1, and 4.2)".format(version))

    def characters(self, text):
        if len(self.stack) > 0:
            self.stack[-1].text += text

    def endElement(self, name):
        self.result = self.stack.pop()

    def endElementNS(self, name, qname):
        if name[0] is not None and name[0].startswith(self.namespace):
            self.endElement(name[1])
        
def loadPMML(pmmlInput, processNamespaces=False):
    """Load a PMML document.

    :type pmmlInput: open XML file, gzip-compressed byte string, XML string, or file name string
    :param pmmlInput: input source for the PMML
    :type processNamespaces: bool
    :param processNamespaces: if ``True``, allow for namespaces other than just "http://www.dmg.org/PMML-*"
    :rtype: titus.pmml.version_independent.PmmlBinding
    :return: loaded PMML
    """

    if isinstance(pmmlInput, basestring):
        if len(pmmlInput) >= 2 and pmmlInput[0:2] == "\x1f\x8b":
            pmmlInput = gzip.GzipFile(fileobj=io.StringIO(pmmlInput))
        elif pmmlInput.find("<") != -1:
            if isinstance(pmmlInput, unicode):
                pmmlInput = io.StringIO(pmmlInput)
            else:
                pmmlInput = io.StringIO(unicode(pmmlInput))
        else:
            pmmlInput = open(pmmlInput)

    contentHandler = PmmlContentHandler()
    parser = xml.sax.make_parser()
    parser.setContentHandler(contentHandler)
    if processNamespaces:
        parser.setFeature(xml.sax.handler.feature_namespaces, True)

    contentHandler.parser = parser

    parser.parse(pmmlInput)

    return contentHandler.result

def pmmlToAst(pmmlInput, options=None):
    """Load a PMML document and convert it to a PFA abstract syntax tree.

    :type pmmlInput: open XML file, gzip-compressed byte string, XML string, or file name string
    :param pmmlInput: input source for the PMML
    :type options: dict of option strings
    :param options: PMML-to-PFA conversion options
    :rtype: titus.pfaast.EngineConfig
    :return: converted PFA
    """

    if options is None:
        options = {}

    context = titus.pmml.version_independent.Context()
    context.avroTypeBuilder = AvroTypeBuilder()
    
    obj = loadPMML(pmmlInput, options.get("reader.processNamespaces", False))
    result = obj.toPFA(options, context)

    context.avroTypeBuilder.resolveTypes()
    return result

def pmmlToNode(pmmlInput, options=None):
    """Load a PMML document and convert it to PFA as Pythonized JSON.

    :type pmmlInput: open XML file, gzip-compressed byte string, XML string, or file name string
    :param pmmlInput: input source for the PMML
    :type options: dict of option strings
    :param options: PMML-to-PFA conversion options
    :rtype: Pythonized JSON
    :return: converted PFA
    """

    if options is None:
        options = {}

    config = pmmlToAst(pmmlInput, options)

    lineNumbers = options.get("reader.lineNumbers", False)
    return config.jsonNode(lineNumbers, set())

def pmmlToJson(pmmlInput, options=None):
    """Load a PMML document and convert it to PFA as a serialized JSON string.

    :type pmmlInput: open XML file, gzip-compressed byte string, XML string, or file name string
    :param pmmlInput: input source for the PMML
    :type options: dict of option strings
    :param options: PMML-to-PFA conversion options
    :rtype: JSON string
    :return: converted PFA
    """

    if options is None:
        options = {}

    config = pmmlToAst(pmmlInput, options)

    lineNumbers = options.get("reader.lineNumbers", False)
    return config.toJson(lineNumbers)
    
