#!/usr/bin/env python

import csv
import json
import random
import math
import copy

from augustus.strict import *

spectra = set(x[-1] for x in csv.reader(open("hipparcos_numerical.csv")))

def splitter():
    splitField = ["ra", "dec", "dist", "mag", "absmag", "x", "y", "z", "vx", "vy", "vz"][random.randint(0, 10)]
    if splitField == "ra":
        splitValue = random.uniform(1, 23)
    elif splitField == "dec":
        splitValue = random.uniform(-87, 87)
    elif splitField == "dist":
        splitValue = math.exp(random.gauss(5.5, 1))
    elif splitField == "mag":
        splitValue = random.gauss(8, 1)
    elif splitField == "absmag":
        splitValue = random.gauss(2, 2)
    elif splitField == "x":
        splitValue = math.exp(random.gauss(5, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    elif splitField == "y":
        splitValue = math.exp(random.gauss(5, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    elif splitField == "z":
        splitValue = math.exp(random.gauss(5, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    elif splitField == "vx":
        splitValue = math.exp(random.gauss(-12, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    elif splitField == "vy":
        splitValue = math.exp(random.gauss(-12, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    elif splitField == "vz":
        splitValue = math.exp(random.gauss(-12, 1)) * (1 if random.randint(0, 1) == 1 else -1)
    return splitField, splitValue

random.seed(12345)
identifier = 0
def getIdentifier():
    global identifier
    identifier += 1
    return identifier - 1

def treeNodePFA(depth):
    if depth == 0:
        return {"int": getIdentifier()}
    splitField, splitValue = splitter()
    return {"T": {"field": splitField, "operator": "<", "value": splitValue, "pass": treeNodePFA(depth - 1), "fail": treeNodePFA(depth - 1)}}

skeleton = \
{"input": {"type": "record", "name": "Datum", "fields": [
    {"name": "ra", "type": "double", "doc": "0..24"},
    {"name": "dec", "type": "double", "doc": "0..90"},
    {"name": "dist", "type": "double", "doc": "ln(dist) is 5.5 +- 1"},
    {"name": "mag", "type": "double", "doc": "mag is 8 +- 1"},
    {"name": "absmag", "type": "double", "doc": "absmag is 2 +- 2"},
    {"name": "x", "type": "double", "doc": "ln(abs(x)) is 5 +- 1"},
    {"name": "y", "type": "double"},
    {"name": "z", "type": "double"},
    {"name": "vx", "type": "double", "doc": "ln(abs(vx)) is -12 +- 1"},
    {"name": "vy", "type": "double"},
    {"name": "vz", "type": "double"},
    {"name": "spectrum", "type": "string"}
    ]},
 "output": "int",
 "pools": {"trees":
           {"type": {"type": "record", "name": "T", "fields":
                     [{"name": "field", "type": {"type": "enum", "name": "F", "symbols": ["ra", "dec", "dist", "mag", "absmag", "x", "y", "z", "vx", "vy", "vz", "spectrum"]}},
                      {"name": "operator", "type": "string"},
                      {"name": "value", "type": "double"},
                      {"name": "pass", "type": ["int", "T"]},
                      {"name": "fail", "type": ["int", "T"]}
                      ]},
            "init": {}}},
 "action": {"model.tree.simpleWalk": [
    "input",
    {"pool": "trees", "path": ["input.spectrum"]},
    {"params": [{"d": "Datum"}, {"t": "T"}],
     "ret": "boolean",
     "do": {"model.tree.simpleTest": ["d", "t"]}}]}
 }

tofill = skeleton["pools"]["trees"]["init"]
for spectrum in spectra:
    tofill[spectrum] = treeNodePFA(10)["T"]

json.dump(skeleton, open("hipparcos_segmented_10.pfa", "w"))

random.seed(12345)
identifier = 0
def getIdentifier():
    global identifier
    identifier += 1
    return identifier - 1

pmml = modelLoader.loadXml("""
<PMML version="4.1" xmlns="http://www.dmg.org/PMML-4_1">
    <Header/>
    <DataDictionary>
        <DataField name="ra" dataType="double" optype="continuous" />
        <DataField name="dec" dataType="double" optype="continuous" />
        <DataField name="dist" dataType="double" optype="continuous" />
        <DataField name="mag" dataType="double" optype="continuous" />
        <DataField name="absmag" dataType="double" optype="continuous" />
        <DataField name="x" dataType="double" optype="continuous" />
        <DataField name="y" dataType="double" optype="continuous" />
        <DataField name="z" dataType="double" optype="continuous" />
        <DataField name="vx" dataType="double" optype="continuous" />
        <DataField name="vy" dataType="double" optype="continuous" />
        <DataField name="vz" dataType="double" optype="continuous" />
        <DataField name="spectrum" dataType="string" optype="continuous" />
    </DataDictionary>
    <MiningModel functionName="classification">
        <MiningSchema>
            <MiningField name="ra" />
            <MiningField name="dec" />
            <MiningField name="dist" />
            <MiningField name="mag" />
            <MiningField name="absmag" />
            <MiningField name="x" />
            <MiningField name="y" />
            <MiningField name="z" />
            <MiningField name="vx" />
            <MiningField name="vy" />
            <MiningField name="vz" />
            <MiningField name="spectrum" />
        </MiningSchema>
        <Segmentation multipleModelMethod="selectFirst">
<Segment><SimplePredicate field="spectrum" operator="equal" value=""/><TreeModel functionName="classification"><MiningSchema><MiningField name="ra"/><MiningField name="dec"/><MiningField name="dist"/><MiningField name="mag"/><MiningField name="absmag"/><MiningField name="x"/><MiningField name="y"/><MiningField name="z"/><MiningField name="vx"/><MiningField name="vy"/><MiningField name="vz"/><MiningField name="spectrum"/></MiningSchema><Node score=""><True/></Node></TreeModel></Segment>
        </Segmentation>
    </MiningModel>
</PMML>            
""")

E = modelLoader.elementMaker()

def treeNodePMML(depth, predicate):
    if depth == 0:
        return E.Node(predicate, score=str(getIdentifier()))
    splitField, splitValue = splitter()
    return E.Node(predicate,
                  treeNodePMML(depth - 1, SimplePredicate(field=splitField, operator="lessThan", value=str(splitValue))),
                  treeNodePMML(depth - 1, SimplePredicate(field=splitField, operator="greaterOrEqual", value=str(splitValue))),
                  score="")

segment = pmml[2, 1, 0]
del pmml[2, 1, 0]

for spectrum in spectra:
    newSegment = copy.deepcopy(segment)
    newSegment[1, 1] = treeNodePMML(10, E.True())
    newSegment[0]["value"] = spectrum
    pmml[2, 1].append(newSegment)

pmml.xmlFile("hipparcos_segmented_10.pmml")
