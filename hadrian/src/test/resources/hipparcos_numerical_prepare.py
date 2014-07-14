#!/usr/bin/env python

import csv
import json
import random
import math

from augustus.strict import *

hipparcos = csv.reader(open("/tmp/downloads/hygxyz.csv"))
fields = hipparcos.next()
out = open("hipparcos_numerical.csv", "w")

ra_index = fields.index("RA")
dec_index = fields.index("Dec")
dist_index = fields.index("Distance")
mag_index = fields.index("Mag")
absmag_index = fields.index("AbsMag")
spectrum_index = fields.index("Spectrum")
x_index = fields.index("X")
y_index = fields.index("Y")
z_index = fields.index("Z")
vx_index = fields.index("VX")
vy_index = fields.index("VY")
vz_index = fields.index("VZ")

for line in hipparcos:
    ra = float(line[ra_index])
    dec = float(line[dec_index])
    dist = float(line[dist_index])
    mag = float(line[mag_index])
    absmag = float(line[absmag_index])
    x = float(line[x_index])
    y = float(line[y_index])
    z = float(line[z_index])
    vx = float(line[vx_index])
    vy = float(line[vy_index])
    vz = float(line[vz_index])
    spectrum = line[spectrum_index]
    if spectrum == "":
        spectrum = "none"
    if dist < 10000000:
        out.write(",".join(map(str, [ra, dec, dist, mag, absmag, x, y, z, vx, vy, vz, spectrum.replace(" ", "-")])) + "\n")

out.close()

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
    {"name": "vz", "type": "double"}
    ]},
 "output": "int",
 "cells": {"tree":
           {"type": {"type": "record", "name": "T", "fields":
                     [{"name": "field", "type": {"type": "enum", "name": "F", "symbols": ["ra", "dec", "dist", "mag", "absmag", "x", "y", "z", "vx", "vy", "vz"]}},
                      {"name": "operator", "type": "string"},
                      {"name": "value", "type": "double"},
                      {"name": "pass", "type": ["int", "T"]},
                      {"name": "fail", "type": ["int", "T"]}
                      ]},
            "init": None}},
 "action": {"model.tree.simpleWalk": [
    "input",
    {"cell": "tree"},
    {"params": [{"d": "Datum"}, {"t": "T"}],
     "ret": "boolean",
     "do": {"model.tree.simpleTest": ["d", "t"]}}]}
 }

skeleton["cells"]["tree"]["init"] = treeNodePFA(10)["T"]
json.dump(skeleton, open("hipparcos_numerical_10.pfa", "w"))

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
    </DataDictionary>
    <TreeModel functionName="classification">
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
        </MiningSchema>
        <Node score="">
            <True />
        </Node>
    </TreeModel>
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

pmml[2, 1] = treeNodePMML(10, E.True())
pmml.xmlFile("hipparcos_numerical_10.pmml")
