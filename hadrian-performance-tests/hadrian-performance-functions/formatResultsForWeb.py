#!/usr/bin/env python

import json
import math
import os
from xml.sax.saxutils import escape as escapeXML

import numpy

from cassius import *

class FunctionTest(object):
    def __init__(self, functionName):
        self.functionName = functionName
        self.timeSeconds = []
        self.repetitions = []
        self.result = []
    def add(self, timeSeconds, repetitions, result):
        self.timeSeconds.append(timeSeconds)
        self.repetitions.append(repetitions)
        self.result.append(result)
    def mean(self, weighted=True, successes=True):
        timeSeconds = numpy.array(self.timeSeconds)
        repetitions = numpy.array(self.repetitions)
        result = numpy.array(self.result, dtype=numpy.int)
        if successes:
            timeSeconds = timeSeconds[result == 1]
            repetitions = repetitions[result == 1]
        else:
            timeSeconds = timeSeconds[result == 0]
            repetitions = repetitions[result == 0]
        if not weighted:
            return timeSeconds.mean()
        else:
            return (timeSeconds * repetitions).sum() / repetitions.sum()
    def stdev(self, weighted=True, successes=True):
        timeSeconds = numpy.array(self.timeSeconds)
        repetitions = numpy.array(self.repetitions)
        result = numpy.array(self.result, dtype=numpy.int)
        if successes:
            timeSeconds = timeSeconds[result == 1]
            repetitions = repetitions[result == 1]
        else:
            timeSeconds = timeSeconds[result == 0]
            repetitions = repetitions[result == 0]
        if not weighted:
            return timeSeconds.std()
        else:
            try:
                return math.sqrt((timeSeconds**2 * repetitions).sum()/repetitions.sum() - ((timeSeconds * repetitions).sum()/repetitions.sum())**2)
            except ValueError:
                return 0.0

functionTests = {}

for lineNumber, line in enumerate(open("timingResults.tsv")):
    if lineNumber % 10000 == 0:
        print lineNumber/1158159.0
    if not line.startswith("Java HotSpot(TM)"):
        functionName, signature, input, result, output, timeSeconds, repetitions = line.strip().split("\t")
        # signature = json.loads(signature)
        # input = json.loads(input)
        # if output != "?":
        #     output = json.loads(output)
        if result == "success":
            result = True
        elif result == "failure":
            result = False
        timeSeconds = float(timeSeconds)
        repetitions = int(repetitions)
        key = functionName
        if key not in functionTests:
            functionTests[key] = FunctionTest(functionName)
        functionTests[key].add(timeSeconds, repetitions, result)

for functionTest in sorted(functionTests.values(), key=lambda x: x.functionName):
    result = numpy.array(functionTest.result, dtype=numpy.int)
    x0 = numpy.array(functionTest.timeSeconds)[result == 0] * 1e6
    w0 = numpy.array(functionTest.repetitions)[result == 0]
    x1 = numpy.array(functionTest.timeSeconds) * 1e6
    w1 = numpy.array(functionTest.repetitions)
    h1 = Histogram(20, 0, 50, data=x1, weights=w1, fillcolor="DodgerBlue", xlabel="microseconds", toplabel=escapeXML(functionTest.functionName))
    if not all(functionTest.result):
        h0 = Histogram(20, 0, 50, data=x0, weights=w0, fillcolor="darkorange", xlabel="microseconds", toplabel=escapeXML(functionTest.functionName))
    else:
        h0 = None
    fileName = "www/PLOTS/dist-" + functionTest.functionName.replace(".", "_").replace("<=", "le").replace("<", "lt").replace(">=", "ge").replace(">", "gt").replace("==", "eq").replace("!=", "ne").replace("%", "mod").replace("&", "and").replace("|", "or").replace("^", "caret").replace("+", "plus").replace("-", "minus").replace("**", "pow").replace("*", "times").replace("/", "div").replace("~", "tilde").replace("!", "bang").replace("(nothing)", "nothing").replace("(passminusthrough)", "pass-through") + ".svg"
    if h0 is None:
        draw(h1, toplabeloffset=-0.02, fileName=fileName)
    else:
        draw(Overlay(h1, h0, xmin=0, xmax=50, ymin=0, xlabel="microseconds", toplabel=escapeXML(functionTest.functionName)), toplabeloffset=-0.02, fileName=fileName)
    littlepng = fileName.replace(".svg", "_50.png")
    bigpng = fileName.replace(".svg", "_500.png")
    os.system("inkscape %s --export-png=%s --export-width=50 --export-background=white" % (fileName, littlepng))
    os.system("inkscape %s --export-png=%s --export-width=500 --export-background=white" % (fileName, bigpng))

tableData = []
for functionTest in sorted(functionTests.values(), key=lambda x: " " if x.functionName == "(nothing)" else "  " if x.functionName == "(pass-through)" else x.functionName):
    fileName = "PLOTS/dist-" + functionTest.functionName.replace(".", "_").replace("<=", "le").replace("<", "lt").replace(">=", "ge").replace(">", "gt").replace("==", "eq").replace("!=", "ne").replace("%", "mod").replace("&", "and").replace("|", "or").replace("^", "caret").replace("+", "plus").replace("-", "minus").replace("**", "pow").replace("*", "times").replace("/", "div").replace("~", "tilde").replace("!", "bang").replace("(nothing)", "nothing").replace("(passminusthrough)", "pass-through") + "_50.png"

    successMean = functionTest.mean(successes=True) * 1e6
    successStdev = functionTest.stdev(successes=True) * 1e6
    if not all(functionTest.result):
        failureMean = functionTest.mean(successes=False) * 1e6
        failureStdev = functionTest.stdev(successes=False) * 1e6
    else:
        failureMean = "n/a"
        failureStdev = "n/a"

    functionNameSort = functionTest.functionName
    successMeanSort = successMean
    successStdevSort = successStdev
    failureMeanSort = failureMean if failureMean != "n/a" else 0.00002
    failureStdevSort = failureStdev if failureMean != "n/a" else 0.00002
    if functionTest.functionName == "(nothing)":
        functionNameSort = " "
        successMeanSort = 0.0
        successStdevSort = 0.0
        failureMeanSort = 0.0
        failureStdevSort = 0.0
    elif functionTest.functionName == "(pass-through)":
        functionNameSort = "  "
        successMeanSort = 0.00001
        successStdevSort = 0.00001
        failureMeanSort = 0.00001
        failureStdevSort = 0.00001
    
    if "(" in functionTest.functionName:
        tableData.append("""          <tr><td data-sort-value="%s">%s</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%s</td><td style="text-align: center;" data-sort-value="%011.5f">%s</td><td style="text-align: center;" data-sort-value="%s"><a href="%s"><img src="%s"></a></td></tr>""" % (functionNameSort, functionTest.functionName, successMeanSort, successMean, successStdevSort, successStdev, failureMeanSort, failureMean, failureStdevSort, failureStdev, functionNameSort, fileName.replace("_50.png", "_500.png"), fileName))
    elif failureMean != "n/a":
        tableData.append("""          <tr><td data-sort-value="%s"><a href="%s">%s</a></td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%s"><a href="%s"><img src="%s"></a></td></tr>""" % (functionNameSort, "http://dmg.org/pfa/docs/library/#fcn:" + functionTest.functionName, functionTest.functionName, successMeanSort, successMean, successStdevSort, successStdev, failureMeanSort, failureMean, failureStdevSort, failureStdev, functionNameSort, fileName.replace("_50.png", "_500.png"), fileName))
    else:
        tableData.append("""          <tr><td data-sort-value="%s"><a href="%s">%s</a></td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%.2f</td><td style="text-align: center;" data-sort-value="%011.5f">%s</td><td style="text-align: center;" data-sort-value="%011.5f">%s</td><td style="text-align: center;" data-sort-value="%s"><a href="%s"><img src="%s"></a></td></tr>""" % (functionNameSort, "http://dmg.org/pfa/docs/library/#fcn:" + functionTest.functionName, functionTest.functionName, successMeanSort, successMean, successStdevSort, successStdev, failureMeanSort, failureMean, failureStdevSort, failureStdev, functionNameSort, fileName.replace("_50.png", "_500.png"), fileName))

open("www/index.html", "w").write('''
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Hadrian performance</title>
    <script src="jquery-tablesort-0.0.6/jquery-2.1.4.min.js"></script>
    <script src="jquery-tablesort-0.0.6/jquery.tablesort.min.js"></script>
    <style>
th.sorted.ascending div:after {
    content: "  \\2191";
}
th.sorted.descending div:after {
    content: " \\2193";
}
    </style>
  </head>
  <body onload='$("#performanceTable").tablesort();'>
    <h2>Hadrian performance</h2>
    <p>The sortable table below presents mean and standard deviation (stdev) times spent computing each PFA function in isolation. The "(nothing)" special case is a PFA engine that simply returns null and the "(pass-through)" special case is a PFA engine that simply outputs what it is given. These represent the overhead of the PFA action itself, while the tests of each function should be understood as being relative to this baseline (within a standard deviation).</p>
    <p>Each mean and standard deviation is based on all the <a href="http://dmg.org/pfa/docs/conformance/">conformance tests</a>, which cover a broad spectrum of input types (for functions with wildcards) and input values, and each input value was tested hundreds of times. The effect of HotSpot just-in-time optimizations was avoided by instituting a burn-in period, and the effect of the Java garbage collector was avoided by invoking the garbage collector when the stopwatch is off (when the heap memory used reaches 5 GB in a JVM with a maximum of 8 GB).</p>
    <p>Means and standard deviations are split into two cases: ones which result in a value (no errors) and ones that raised an exception.</p>
    <p>All tests were performed on an AWS m3.xlarge instance (4 vCPU Intel Xeon E5-2670 v2 processors, 15 GB SSD-based RAM) running Ubuntu 14.04 (Trusty Tahr) and Oracle Java version 1.8.0_40 (64-bit build 25.40-b25) with <tt>-Xmx8g -Xmn8g</tt> to ensure the fixed memory allocation that allows us to control garbage collector calls.</p>
    <div style="position: absolute">
      <div style="height: 20px;">
        <div style="position: absolute; top: -4px; left: 322px; background-color: lightgrey; padding: 3px; font-weight: bold; width: 216px; text-align: center;">Returning value</div>
        <div style="position: absolute; top: -4px; left: 554px; background-color: lightgrey; padding: 3px; font-weight: bold; width: 216px; text-align: center;">Raising exception</div>
      </div>
      <table id="performanceTable">
        <thead>
          <tr>
            <th class="sorted ascending"><div style="width: 300px; background-color: lightgrey; padding: 3px; margin: 3px;">Function</div></td>
            <th><div style="width: 100px; background-color: lightgrey; padding: 3px; margin: 3px;">Mean (&mu;s)</div></td>
            <th><div style="width: 100px; background-color: lightgrey; padding: 3px; margin: 3px;">Stdev (&mu;s)</div></td>
            <th><div style="width: 100px; background-color: lightgrey; padding: 3px; margin: 3px;">Mean (&mu;s)</div></td>
            <th><div style="width: 100px; background-color: lightgrey; padding: 3px; margin: 3px;">Stdev (&mu;s)</div></td>
            <th><div style="width: 100px; background-color: lightgrey; padding: 3px; margin: 3px;">Distribution</div></td>
          </tr>
        </thead>
          <tbody>
  %s
          </tbody>
      </table>
    </div>
  </body>
</html>
''' % "\n".join(tableData))
