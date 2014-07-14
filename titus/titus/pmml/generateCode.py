#!/usr/bin/env python

import sys
import os
import xml.etree.ElementTree as etree

superset = set()

def fixName(name):
    out = name.replace("-", "_")
    if out == "True":
        out = "AlwaysTrue"
    elif out == "False":
        out = "AlwaysFalse"
    elif out == "from":
        out = "isfrom"
    return out

def convert(xsdFileName):
    pmmlXsd = etree.parse(xsdFileName)

    elements = pmmlXsd.getroot().getchildren()
    elements.sort(lambda a, b: cmp(a.attrib["name"], b.attrib["name"]))

    pyFileName = xsdFileName.replace("schemae/", "").replace("pmml-", "version_").replace("-", "_").replace(".xsd", ".py")
    if os.path.exists(pyFileName):
        if raw_input("overwrite {} (y/N)? ".format(pyFileName)).lower() != "y":
            sys.exit(0)
    output = open(pyFileName, "w")
    output.write("""#!/usr/bin/env python

import version_independent as ind    

namespace = "{}"

""".format(pmmlXsd.getroot().attrib["targetNamespace"]))

    groups = {}
    unresolvedCount = 0
    while True:
        for element in elements:
            if element.tag == "{http://www.w3.org/2001/XMLSchema}group":
                children = [x.attrib["ref"] for x in element.findall(".//{http://www.w3.org/2001/XMLSchema}element")]
                groups[element.attrib["name"]] = children

                groupsWithinGroups = [x.attrib["ref"] for x in element.findall(".//{http://www.w3.org/2001/XMLSchema}group")]
                for group in groupsWithinGroups:
                    if group in groups:
                        groups[element.attrib["name"]].extend(groups[group])
                    else:
                        unresolvedCount += 1

        if unresolvedCount == 0: break
        unresolvedCount = 0

    tagToClass = []
    for element in elements:
        if element.tag == "{http://www.w3.org/2001/XMLSchema}element":
            attributes = [x.attrib["name"] for x in element.findall(".//{http://www.w3.org/2001/XMLSchema}attribute")]
            children = [x.attrib["ref"] for x in element.findall(".//{http://www.w3.org/2001/XMLSchema}element")]
            for group in [x.attrib["ref"] for x in element.findall(".//{http://www.w3.org/2001/XMLSchema}group")]:
                children.extend(groups[group])

            children = sorted(list(set(children)))

            overlap = set(attributes).intersection(set(children))
            if len(overlap) > 0:
                raise Exception("overlap of subelements and attributes in {}: {}".format(element.attrib["name"], overlap))

            output.write("""class {cls}(ind.{cls}):
    def __init__(self, attribs):
        super({cls}, self).__init__()
{attributes}        for key, value in attribs.items():
            setattr(self, key, value)
{children}        
""".format(cls=fixName(element.attrib["name"]),
           attributes="".join("        self.{} = None\n".format(fixName(x)) for x in attributes),
           children="".join("        self.{} = []\n".format(fixName(x)) for x in children)))

            if element.attrib["name"] != fixName(element.attrib["name"]):
                output.write("""    @property
    def tag(self):
        return "{}"

""".format(element.attrib["name"]))

            tagToClass.append((element.attrib["name"], fixName(element.attrib["name"])))
            superset.add(fixName(element.attrib["name"]))

    output.write("tagToClass = {\n")
    for tag, cls in tagToClass:
        output.write("    \"{}\": {},\n".format(tag, cls))
    output.write("    }\n")

    output.close()

if __name__ == "__main__":
    convert("schemae/pmml-4-2.xsd")
    convert("schemae/pmml-4-1.xsd")
    convert("schemae/pmml-4-0.xsd")
    convert("schemae/pmml-3-2.xsd")

    pyFileName = "version_independent.py"
    if os.path.exists(pyFileName):
        if raw_input("overwrite {} (y/N)? ".format(pyFileName)).lower() != "y":
            sys.exit(0)
    output = open(pyFileName, "w")
    output.write("""#!/usr/bin/env python

import json

import titus.ast as ast
from titus.datatype import AvroTypeBuilder
from titus.util import uniqueEngineName, uniqueRecordName, uniqueEnumName

class Context(object): pass

class PmmlBinding(object):
    def __init__(self):
        self.children = []
        self.text = ""
        self.pos = None

    @property
    def tag(self):
        return self.__class__.__name__

""")
    for element in sorted(list(superset)):
        output.write("""class {cls}(PmmlBinding):
    def toPFA(self, options, context):
        raise NotImplementedError

""".format(cls=element))

    output.close()
