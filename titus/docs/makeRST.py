#!/usr/bin/env python

import importlib
import inspect

modules = [
    "titus.datatype",
    "titus.errors",
    "titus.fcn",
    "titus.genpy",
    "titus.options",
    "titus.pfaast",
    "titus.P",
    "titus.prettypfa",
    "titus.reader",
    "titus.signature",
    "titus.util",
    "titus.version",
    "titus.producer.cart",
    "titus.producer.chain",
    "titus.producer.expression",
    "titus.producer.kmeans",
    "titus.producer.tools",
    "titus.producer.transformation",
    "titus.inspector.defs",
    "titus.inspector.jsongadget",
    "titus.inspector.main",
    "titus.inspector.parser",
    "titus.inspector.pfagadget",
    ]

modules = {name: importlib.import_module(name) for name in modules}

documented = []
for moduleName, module in modules.items():
    for objName in dir(module):
        obj = getattr(module, objName)
        if not objName.startswith("_") and callable(obj) and obj.__module__ == moduleName:
            print(objName, obj)
            documented.append(moduleName + "." + objName)
            if inspect.isclass(obj):
                open(moduleName + "." + objName + ".rst", "w").write('''
{0}
{1}

.. autoclass:: {0}
    :members:
    :undoc-members:
    :show-inheritance:
'''.format(moduleName + "." + objName, "=" * (len(moduleName) + len(objName) + 1)))
            else:
                open(moduleName + "." + objName + ".rst", "w").write('''
{0}
{1}

.. autofunction:: {0}
'''.format(moduleName + "." + objName, "=" * (len(moduleName) + len(objName) + 1)))

documented.sort()

open("index.rst", "w").write('''
Titus |version|
===============

:ref:`genindex`

.. toctree::
   :maxdepth: 2

''' + "\n".join("   " + x for x in documented) + "\n")
