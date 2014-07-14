#!/usr/bin/env python

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
                    raise ValueError("cannot place {} under {} in PMML {}".format(name, self.stack[-1].tag, self.version))

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
            raise NotImplementedError("PMML version {} is not supported (only 3.2, 4.0, 4.0.1, 4.1, and 4.2)".format(version))

    def characters(self, text):
        if len(self.stack) > 0:
            self.stack[-1].text += text

    def endElement(self, name):
        self.result = self.stack.pop()

    def endElementNS(self, name, qname):
        if name[0] is not None and name[0].startswith(self.namespace):
            self.endElement(name[1])
        
def loadPMML(pmmlInput, processNamespaces=False):
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
    if options is None:
        options = {}

    context = titus.pmml.version_independent.Context()
    context.avroTypeBuilder = AvroTypeBuilder()
    
    obj = loadPMML(pmmlInput, options.get("reader.processNamespaces", False))
    result = obj.toPFA(options, context)

    context.avroTypeBuilder.resolveTypes()
    return result
