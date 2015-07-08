Hadrian: implementations of the PFA specification
=======

**Version v.0.7.1**

This repository contains four libraries for working with PFA (see http://scoringengine.org for details).

  * Hadrian: complete implementation of PFA for the **JVM (Java, Scala, ...)**
    * loads, validates, byte-compiles, and executes PFA scoring engines within a JVM
    * tools for analyzing PFA statically and at runtime
  * Titus: complete implementation of PFA for **Python**
    * loads, validates, interprets, and executes PFA scoring engines in pure Python
    * translates PrettyPFA, Python code, and some PMML to PFA
    * produces models using k-means and CART
    * interactive analysis of PFA documents with the PFA Inspector script
    * PFA development tools, such as pfachain and pfaexternalize
  * Aurelius: tools for building PFA from **R**
    * converts a subset of the R language to PFA
    * converts glm, glmnet, randomForest, and gbm models to PFA
  * Antinous: model producer in **Jython**
    * encapsulates arbitrary Jython code in a PFA container
    * produces models using k-means

It also contains four envelopes for embedding PFA.

  * hadrian-standalone: runs multithreaded PFA on a command line
    * quick, simple testing in full Hadrian (as opposed to quicker testing in Titus)
    * may be used as a component of a shell-based workflow
  * hadrian-mr: runs PFA in Hadoop
    * schema-matching and sanity checks before job execution
    * built-in secondary sort semantics
    * batch scoring with or without snapshot-as-output
    * may be used with Antinous to produce models in Hadoop
  * hadrian-gae: runs Hadrian on Google App Engine
    * PFA-as-a-service
    * backend for tutorial examples on http://scoringengine.org
  * hadrian-actors: complex directed acyclic workflows for PFA
    * actor-based framework capable of complex, multi-engine workflow topologies
    * extensible configuration language

They are all licensed under the Hadrian Personal Use and Evaluation License (PUEL).
