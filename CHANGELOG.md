**After tag v0.1.0:**

  * Modified stat.sample.updateEWMA so that a variance is not required in the state record (Hadrian and Titus).

  * Loosened stat.sample.updateEWMA so that alpha=0 and alpha=1 are allowed (Hadrian and Titus).

  * Added stat.sample.updateHoltWinters, stat.sample.updateHoltWintersPeriodic, stat.sample.forecast1HoltWinters, and stat.sample.forecastHoltWinters to do double and triple exponential smoothing of a time-series (Hadrian and Titus).

  * Added a.rotate, a generally useful array function that is specifically useful as a stand-in for stat.sample.updateHoltWintersPeriodic with missing values.

  * Changed the return value of "cell-to" to be the new value of the cell and "pool-to" to be the new value of the pool so that change-and-get-new-value can be atomic (Hadrian, Titus, and PFA).  (If you do change and get new value in two statements, the second might have been modified by another scoring engine.)

  * Added PFAEngine.typeParser for access to the ForwardDeclarationParser that was used to interpret its types (Hadrian only).

  * Added AvroConversions for creating a new AvroType from a .avsc File, InputStream, or String (Hadrian only).

  * Fixed collections of scoring engines with shared cells/pools: they now share memory even if not given an explicit SharedMemory object (Hadrian only).

  * Added metric.simpleEuclidean not just as a convenience in clustering, but for linear algebra as well (Hadrian, Titus, and PFA).

  * Fixed a bug in the reader's identification of non-special forms (Hadrian and Titus).

  * Added stat.sample.updateCovariance to accumulate a covariance matrix as a list of lists or a map of maps (Hadrian, Titus, and PFA).

  * Removed null-handling from stat.sample functions that don't need it: update, updateCovariance.  The EWMA and HoltWinters functions do need it, however, because otherwise there wouldn't be a way to express an initial state that has no impact on subsequent values (Hadrian, Titus, and PFA).

  * Added a.mapIndex to provide indexes to map functions (Hadrian, Titus, and PFA).

  * Added call-args special form for calling (user-defined) functions identified at runtime (Hadrian, Titus, and PFA).

  * Fixed two type-casting issues in generated Java code.

**After tag v0.2.0:**

  * Fixed special cases in k-means producer (Titus).

**After tag v0.3.0:**

  * Added a lock to ensure that a scoring engine instance is only used in one thread at a time; different instances of the same engine class can be used concurrently (Hadrian).

  * Fixed an error in how pool-to is specified: the init value must have the type of the pool, not the end of the path (Hadrian, Titus, and PFA).

  * Fixed an error in how pool-to is specified: the init value must always be provided, even if the "to" field is a constant (Hadrian, Titus, and PFA).

**After tag v0.4.0:**

  * Added model.cluster.randomSeeds, kmeansIteration, and updateMean for online clustering (Hadrian, Titus, and PFA).

  * Renamed "fcnref" to just "fcn" and added an optional "fill" field for partially evaluated functions (Hadrian, Titus, and PFA).

  * Changed top-level field "metadata" to a string --> string map and added top-level field "version" as an optional int (Hadrian, Titus, and PFA).

  * Added access to name, version (if present), metadata, actionsStarted, and actionsFinished in begin, action, and end, where applicable (Hadrian, Titus, and PFA).

  * Changed user-defined error forms so that they do not return a real type so that if-then-else does not unexpectedly return a union when one branch raises an exception (Hadrian, Titus, and PFA).

  * Added string-to-number parsing functions in parse.* (Hadrian, Titus, and PFA).

  * Added number nan, inf checking in impute.* (Hadrian, Titus, and PFA).

  * Added la.* linear algebra module (Hadrian, Titus, and PFA).

  * Added m.special.erf (Hadrian, Titus, and PFA).

**After tag v0.5.0:**

  * Added ability to take snapshots of the scoring engine state (Hadrian and Titus).

  * Removed null option from stat.change.updateTrigger, stat.sample.updateEWMA, and stat.sample.updateHoltWintersPeriodic, since it is unnecessary and it complicates signature matching (Hadrian, Titus, and PFA).

  * Added map.* primitives (Hadrian, Titus, and PFA).

  * Changed some a.* names to be consistent with the corresponding m.* names (Hadrian, Titus, and PFA).

**After tag v0.5.2:**

  * Added s.number (Hadrian, Titus, and PFA).

  * Added enum.* fixed.* bytes.* (Hadrian, Titus, and PFA).

  * Added try special form for turning exceptions into missing values (Hadrian, Titus, and PFA).

**After tag v0.5.3:**

  * Fixed error message for missing top-level fields (Titus).

  * Fixed bug in Ast-to-Json: no longer writes out named types multiple times (Hadrian and Titus).

  * Added PrettyPFA with clustering and tutorial examples of its use (Titus).

  * Moved testSpeed into a suite of manually invoked tests, so that all standard tests take a reasonable amount of time (Titus).

  * Fixed NullPointerException in AvroFilledPlaceholder.toString (Hadrian).

**After tag v0.5.4:**

  * Added stat.histogram, stat.histogram2d, stat.topN (Hadrian, Titus, and PFA).

  * Fixed a namespace bug in P.py (Titus).

  * Added ability to read and write JSON data in Hadrian Standalone.

**After tag v0.5.5:**

  * Loosened type restrictions on the "new" special form: now it promotes values to the requested type, rather than requiring them to be exactly that type (Hadrian, Titus, and PFA).

  * Fixed bug in PrettyPFA "do" handling (Titus).

  * Fixed bugs in Hadrian-Standalone state output: special cases overlooked in development (Hadrian-Standalone).

  * Fixed line number bugs in PrettyPFA (Titus).

**After tag v0.5.6:**

  * Added casting functions as cast.* (Hadrian, Titus, PFA).

  * Added avscToPretty to turn Avro avsc files into PrettyPFA fragments (Titus).

  * Added update(recordExpr, field: newValue) syntax to PrettyPFA (Titus).

  * Fixed FcnRefFill special case for PrettyPFA (Titus).

  * Added merge as a required top-level field for fold-type engines (Hadrian, Titus, PFA).

  * Fixed random generator seeds so that a collection of engine instances will have different seeds, based on the master seed (Hadrian, Titus).

  * Added "types" section to PrettyPFA to centralize type declarations (Titus).

**After tag v0.5.7:**

  * Fixed bug: missing code in P.fromType that was already correctly implemented in Titus (Hadrian).

**After tag v0.5.8:**

  * Added ability to load one PFAEngine class's output into another PFAEngine class's input and create data that is not yet associated with any PFAEngine (Hadrian).

  * Added PFAEngine.factoryFrom* methods to make engine factories (Hadrian).

  * Added ability to load PFA data into type-safe Scala classes (Hadrian).

  * Added "instance" as an integer-valued predefined field everywhere that "name" is defined (Hadrian, Titus, and PFA).

  * Added jsonOutputDataFileWriter for JSON output with the same interface as avroOutputDataFileWriter (Hadrian).

  * Added a wrapper around fastavro to correct its behavior regarding Unicode strings (Titus).

**After tag v0.5.9:**

  * Added new implementation, hadrian-akka.

**After tag v0.5.10:**

  * Improved logging in hadrian-akka.

**After tag v0.5.11:**

  * Fixed bug in hadrian-akka: too many requests for work sent.

**After tag v0.5.12:**

  * Backported Titus to work in Python 2.6.

**After tag v0.5.13:**

  * Added additional debugging output to KMeans producer (Titus).

  * Fixed subtle bug in KMeans that only affected Numpy 1.6.2 (Titus).

  * Fixed sort order bug in KMeans that mislabels clusters with other clusters' populations (Titus).

  * Unified AVRO and JSON output, added option of going to an OutputStream rather than a file, and added option of writing schema as first line of JSON (Hadrian).

  * Simplified Hadrian-standalone to be a standard input to standard output UNIX tool.

  * Added ability for Hadrian-Akka to read schema types from files.

  * Added access to PFAEngine's inputClass and outputClass for reflection (Hadrian).

  * Fixed bug in implementation of PrettyPFA "types" section: type references must be expanded exactly once (Titus).

  * Fixed bug in implementation of try-catch (Hadrian).

  * Added utility functions for extracting and swapping PFA fragments (Titus).

  * Fixed a bug in Titus KMeans producer: AbsDiff was not taking the absolute value (never noticed until now because we always combined it with Euclidean).

  * Added preprocess option to KMeans producer (Titus).

  * Performance-tuned Hadrian-Akka and removed the Akka dependence, making it now Hadrian-Actors.

**After tag v0.5.14:**

  * Added literals lookup table to speed up common models by a factor of five (Hadrian).

  * Added thread to watch memory use in Hadrian-Actors and spill data from pipes if it gets too high.

**After tag v0.5.15:**

  * Added better logging of PFA-log messages and exceptions with stack traces in Hadrian-Actors.

  * Fixed a bug where both generic and engine-bound avroInputIterator would ignore mismatches between expected schema and file schema (Hadrian).

**After tag v0.5.16:**

  * Added many functions in prob.dist library, added regex.* and spec.* libraries (Hadrian, Titus, and PFA).

  * Added cast.fanout* functions to turn categorical variables into a suite of zeros and ones (Hadrian, Titus, and PFA).

  * Added model.reg.* module for regression (Hadrian, Titus, and PFA).

  * Added support for (non-standard) "NaN" when deserializing PFA from JSON; agrees with what Python outputs by default (Hadrian).

  * Fixed a bug in which the "in" operator was incorrectly scored when model.tree.simpleTest encountered a union of values that include both arrays and primitive types (Hadtrian and Titus).

  * Replaced hard-to-read type error strings with type comparisons rendered in indented PrettyPFA (Titus).

  * Added rand.* module for random value generation, including type-4 UUIDs (Hadrian, Titus, and PFA).

  * Added PFA-Inspector to scan PFA documents and get basic information about them.  Can be extended with gadgets.

  * Added Java-centric interface to build engines from JSON/YAML/etc. (Hadrian).

  * Added AvroRecord.fieldNames method (Hadrian).

**After tag v0.6.0:**

  * Added general Transformation class to titus.producer.expression, removed preprocess from KMeans (Titus).

  * Added ability to load input from files in Hadrian-Standalone.

  * Fixed large PFA file-reading bug in Hadrian-Actors.

  * Added replace(partialFunction) method to all Ast classes (Hadrian and Titus).

  * Added replacement string syntax and deep replacement methods to PrettyPFA (Titus).

  * Fixed Numpy-vs-Python output bug in model.reg.linear (Titus).

  * Added ability to revert() a PFAEngine to its initial state (Hadrian).

  * Added ability to load PFA models from the inspector commandline (Titus).

**After tag v0.6.1:**

  * Added optional check of datatype on entry to engine.action (Titus).

  * Added la.scale, la.add, and la.sub for common vector/matrix operations (Hadrian, Titus, and PFA).

  * Added simplified signature for model.cluster.closest and model.cluster.closestN (Hadrian, Titus, and PFA).

  * Added model.neighbor.nearestK, model.neighbor.ballR, and model.neighbor.mean (Hadrian, Titus, and PFA).

  * Added lib.time.* for date/time tests and queries (Hadrian, Titus, PFA).

  * Fixed bug in PrettyPFA in which JSON null, true, and false were converted into strings (Titus).

  * Fixed bug in type patterns in Titus: empty namespaces can sometimes be "" rather than None (Titus).
