#!/usr/bin/env python

import glob
import re
import sys
import codecs

from plasTeX.TeX import TeX

# expects one argument from command-line
outputFileName, = sys.argv[1:]

documentation = {}
for fileName in glob.glob("man/*.Rd"):
    # turn the LaTeX into an XML DOM
    tex = TeX()
    tex.input(open(fileName))
    doc = tex.parse()

    # get name, usage, and description from the (weirdly formatted) XML DOM
    name = doc.allChildNodes[[i for i, x in enumerate(doc.allChildNodes) if x.nodeType == 1 and x.tagName == "name"][0] + 1].textContent.strip()
    usage = doc.allChildNodes[[i for i, x in enumerate(doc.allChildNodes) if x.nodeType == 1 and x.tagName == "usage"][0] + 1].textContent.strip()
    description = doc.allChildNodes[[i for i, x in enumerate(doc.allChildNodes) if x.nodeType == 1 and x.tagName == "description"][0] + 2].textContent.strip()

    # get examples directly from the file because that's the only way to save whitespace (multi-line examples)
    examplesMatch = re.search("""\\examples{
(.*)
}
""", open(fileName).read(), re.DOTALL)
    if examplesMatch is not None:
        examples = examplesMatch.group(1).strip()
    else:
        examples = None

    # get the arguments from a nested XML object
    argumentsXML = [x for x in doc.allChildNodes if x.nodeType == 1 and x.tagName == "bgroup" and x.firstChild.nodeType == 1 and x.firstChild.tagName == "par"]
    if len(argumentsXML) == 1:
        # ... and pair up name-description pairs with zip(arguments[::2], arguments[1::2])
        arguments = [x.textContent.strip() for x in argumentsXML[0].allChildNodes if x.nodeType == 1 and x.tagName == "bgroup"]
        argumentPairs = zip(arguments[::2], arguments[1::2])
    else:
        argumentPairs = []

    # create a documentation record
    documentation[name] = {"usage": usage, "description": description, "examples": examples, "arguments": argumentPairs}

# write a .md file for the GitHub wiki
outputFile = codecs.open(outputFileName, "w", "utf-8")

for name in sorted(documentation):   # sorted by function name
    outputFile.write("### " + name + "\n")
    outputFile.write("\n")
    outputFile.write(documentation[name]["description"] + "\n")
    outputFile.write("\n")
    outputFile.write("**Usage:** `" + documentation[name]["usage"] + "`\n")
    outputFile.write("\n")
    for argname, argdesc in documentation[name]["arguments"]:
        outputFile.write("   * **" + argname + ":** " + argdesc + "\n")
    outputFile.write("\n")
    if documentation[name]["examples"] is not None:
        outputFile.write("**Examples:**\n")
        outputFile.write("\n")
        outputFile.write("```R\n")
        outputFile.write(documentation[name]["examples"] + "\n")
        outputFile.write("```\n")
    outputFile.write("\n")

outputFile.close()
