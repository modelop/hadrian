Hadrian: implementations of the PFA specification
========

**Version v.0.7.1**

This repository contains four libraries for working with PFA (see http://scoringengine.org for details).

  * **[Hadrian](https://github.com/opendatagroup/hadrian/wiki#hadrian)** ([API](http://opendatagroup.github.io/hadrian/hadrian-0.7.1/index.html#com.opendatagroup.hadrian.jvmcompiler.PFAEngine)): complete implementation of PFA for the **JVM (Java, Scala, ...)**
    * loads, validates, byte-compiles, and executes PFA scoring engines within a JVM
    * tools for analyzing PFA statically and at runtime
  * **[Titus](https://github.com/opendatagroup/hadrian/wiki#titus)** ([API](http://opendatagroup.github.io/hadrian/titus-0.7.1/)): complete implementation of PFA for **Python**
    * loads, validates, interprets, and executes PFA scoring engines in pure Python
    * translates PrettyPFA, Python code, and some PMML to PFA
    * produces models using k-means and CART
    * interactive analysis of PFA documents with the PFA Inspector script
    * PFA development tools, such as pfachain and pfaexternalize
  * **[Aurelius](https://github.com/opendatagroup/hadrian/wiki#aurelius)**: tools for building PFA from **R**
    * converts a subset of the R language to PFA
    * converts glm, glmnet, randomForest, and gbm models to PFA
    * integrated with Titus for PFA validation and execution
  * **[Antinous](https://github.com/opendatagroup/hadrian/wiki#antinous)**: model producer in **Jython**
    * encapsulates arbitrary Jython code in a PFA container
    * produces models using k-means

It also contains four envelopes for embedding PFA.

  * **[hadrian-standalone](https://github.com/opendatagroup/hadrian/wiki/Hadrian-Standalone)**: runs multithreaded PFA on a command line
    * quick, simple testing in full Hadrian (as opposed to quicker testing in Titus)
    * may be used as a component of a shell-based workflow
  * **[hadrian-mr](https://github.com/opendatagroup/hadrian/wiki/Hadrian-MR)**: runs PFA in Hadoop
    * schema-matching and sanity checks before job execution
    * built-in secondary sort semantics
    * batch scoring with or without snapshot-as-output
    * may be used with Antinous to produce models in Hadoop
  * **[hadrian-gae](https://github.com/opendatagroup/hadrian/wiki/Hadrian-GAE)**: runs Hadrian on Google App Engine
    * PFA-as-a-service
    * backend for tutorial examples on http://scoringengine.org
  * **[hadrian-actors](https://github.com/opendatagroup/hadrian/wiki/Hadrian-Actors)**: complex directed acyclic workflows for PFA
    * actor-based framework capable of complex, multi-engine workflow topologies
    * extensible configuration language

See the [wiki](https://github.com/opendatagroup/hadrian/wiki) for details.

They are all licensed under the Hadrian Personal Use and Evaluation License (PUEL).
