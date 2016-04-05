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

**After tag v0.6.2:**

  * Fixed bug in implementations of time.isWeekend and expanded coverage of tests (Titus and Hadrian).

  * Added a --printTime option to Hadrian-Standalone.

  * Added pass-through begin and end routines to engines when none are defined (Titus).

  * Implemented kd-tree optimization for model.neighbor.nearestK with option lib.model.neighbor.nearestK.kdtree to toggle between them (Hadrian).

  * Fixed a bug in map.mapWithKey that prevented side-effect use, e.g. for logging (Hadrian).

  * Added a memory usage calculator for logging (Hadrian).

  * Added memory-calculation routines to Hadrian and optional reporting to Hadrian-Actors.

  * Added "unpack" special form to unpack binary data with corresponding syntax in PrettyPFA (Hadrian, Titus, and PFA).

  * Added "pack" special form to pack binary data with corresponding syntax in PrettyPFA (Hadrian, Titus, and PFA).

  * Added "bytes.len", "bytes.subseq", and "bytes.subseqto" basic access for byte arrays (Hadrian, Titus, and PFA).

**After tag v0.6.3:**

  * Added "s.hex" for hexidecimal numbers (Hadrian, Titus, and PFA).

  * Added "cast.signed" and "cast.unsigned" to make numbers of the appropriate widths (Hadrian, Titus, and PFA).

  * Fixed a bug in core min/max functions; they returned PFARuntimeException objects, rather than raising them (Titus).

  * Changed names of re.* function names for conformance with s.* (Hadrian, Titus, and PFA).

**After tag v0.6.4:**

  * Added "fixed.fromBytes" basic access for fixed-length byte arrays (Hadrian, Titus, and PFA).

**After tag v0.6.5:**

  * Fixed a bug in Hadrian that prevented simple library functions from being passed as arguments to functors (Hadrian).

**After tag v0.6.6:**

  * Fixed a bug in model.reg.linear that made it sensitive to the order of fields in the record definition (Titus).

  * Fixed a bug in signature resolution that made matching of unions unstable by depending on the order of a map/dict (Hadrian and Titus).

**After tag v0.6.7:**

  * Added titus.producer.chain to merge linear sequences of scoring engines (Titus).

  * Renamed inspector to pfainspector and added pfachain script (Titus).

  * Fixed a bug in PFADataTranslator for unions of enums, fixed, and/or null (Hadrian).

**After tag v0.6.8:**

  * Added rand.choice, rand.choices, rand.sample for picking elements from a bag (Hadrian, Titus, and PFA).

  * Added interp.nearest, interp.linear, interp.linearFlat, interp.linearMissing (Hadrian, Titus, and PFA).

**After tag v0.6.9:**

  * Fixed bug in signum (Titus only), added test coverage in Hadrian and Titus.

  * Added "source" field to cells and pools so that they can be directed to load model parameters from an external JSON or Avro file (Hadrian, Titus, and PFA).

  * Corrected interp.linear, interp.linearFlat, and interp.linearMissing algorithms (Hadrian, Titus, and PFA).

  * Fixed jsonEncoder implementation in Titus, which caused erroneous type tags in pfachain for boolean union types (Titus).

**After tag v0.6.10:**

  * Fixed compilation error in hadrian-gae.

  * Fixed ImportError on initialization if clib is not found (Titus).

**After tag v0.6.11:**

  * Added rand.histogram function to select items with a distribution (Hadrian, Titus, and PFA).

  * Added model.reg.linearVariance to propagate uncertainties through a linear fit (Hadrian, Titus, and PFA).

**After tag v0.7.0 (which is v0.6.12):**

  * Added Antinous project, which wraps Jython in the PFAEngine interface so that Python can substitute PFA in Hadrian.  The compile order is now: hadrian-core -> antinous -> hadrian-implementations/*

  * Fixed bugs in Titus's handling of externalized models (Titus).

  * Fixed bug in resolving unions of multiple record types when translating data (Hadrian).

  * Added Naive Bayes, Neural Nets (Hadrian, Titus, and PFA).

  * Moved link functions to a new library and added a few (Hadrian, Titus, and PFA).

  * Added a.ntile for arbitrary percentiles and a.logsumexp to avoid roundoff error (Hadrian, Titus, and PFA).

  * New JSON output function for R that writes JSON files without building a string in memory (Aurelius).

  * Added model.tree.missingWalk (Hadrian, Titus, and PFA).

  * Added gbm conversion library to Aurelius.

**After tag v0.7.1:**

  * Fixed a bug in Java byte array --> Jython string (Antinous).

  * Added CSV input and output to Hadrian products (Hadrian, Antinous, Hadrian-Standalone).

  * Fixed two bugs that made Antinous KMeans converge too slowly and added KMeans results options (Antinous).

  * Changed Antinous KMeans policy for too few unique points: now it creates a model with a cluster centered on every point and sets a tooFewUniquePoints() flag (Antinous).

**After tag v0.7.1-1:**

  * Added pfarandom script to Titus for generating PFA that makes random data for testing (Titus).

  * Added docstrings for PFAEngine (Hadrian and Titus).

  * Added pfasize script (Titus).

  * Changed time.* signatures to accept timezone string as a parameter (Hadrian, Titus, and PFA).

  * Changed default value of foreach's seq to true (Hadrian, Titus, and PFA).

  * Changed EngineOptions to allow (and ignore) unrecognized options (Hadrian, Titus, and PFA).

  * Added deprecation mechanism and tested with rand.uuid -> rand.uuid4 (Hadrian, Titus, and PFA).

  * Renamed (moved) model.reg.residual, pull, mahalanobis, updateChi2, reducedChi2, and chi2Prob to test.* (Hadrian, Titus, and PFA).

  * Added pool-del special form for removing an item from a pool (Hadrian, Titus, and PFA). Note: no PrettyPFA equivalent yet.

  * Renamed "lib1" directory to "lib" in Hadrian and Titus (finally!).

  * Added histogram-friendly signatures to model.naive.multinomial and model.naive.bernoulli (Hadrian, Titus, and PFA).

  * Added stat.sample.fillCounter (Hadrian, Titus, and PFA).

  * Added a simpler stat.change.zValue signature (Hadrian, Titus, and PFA).

  * Added a.zipmap, a.zipmapWithIndex, map.zipmap, map.zipmapWithKey (Hadrian, Titus, and PFA).

  * Added map.argmax* and map.argmin* (Hadrian, Titus, and PFA).

  * Added cast.avro, cast.json (Hadrian, Titus, and PFA).

  * Added model.tree.simpleTree (Hadrian, Titus, and PFA).

  * Added interp.bin (Hadrian, Titus, and PFA).

  * Added Kleene logic (Hadrian, Titus, and PFA). Want a Kleene xor?

  * Added SVN library (Hadrian, Titus, and PFA).

  * Added signatures to model.cluster.* that can accept a metric over strings and removed the array-based ones because the new signature is a generalization of the old, completely backward-compatible (Hadrian, Titus, and PFA).

  * Added 'pfa externalize' and 'pfa internalize' commands to pfainspector (Titus).

  * Added regular expression check to switch between URL and plain fileName in cell/pool init with source=json (Hadrian, Titus, and PFA).

  * Extended model.reg.linear and model.reg.linearVariance to interpret map-indexed matrices as sparse to be consistent with la.* (Hadrian, Titus, and PFA).

  * Pushed locator-mark derived line numbers through to library functions so that runtime errors can be annotated with source line numbers (Hadrian).

  * Added integer error codes to all library runtime errors (Hadrian, Titus, and PFA).

  * Added implementation-independent coverage tests for all functions in the library (PFA).

  * Added interp.gaussianProcess for interpolation (Hadrian, Titus, and PFA).

  * Added s.int and deprecated integer signature of s.number, so that s.number can be labeled unstable without affecting s.int (Hadrian, Titus, and PFA).

  * Added docstrings to all Titus functions except unit tests and titus.lib.* libraries (Titus).

**After tag v0.8.1:**

  * Fixed loading of libc library under Mac OS 10.11 El Capitan (Titus).

  * Fixed compiler in casting case to use W.wrapExpr.

  * Removed 1.8 java.util.Base64 from antinous.

  * Made Antinous tests insensitive to map order.

  * Added docstrings to all Scala functions except unit tests and com.opendatagroup.hadrian.lib._ libraries (Hadrian).

  * Check for 'long' everywhere we check for 'int' (Titus).

**After tag v0.8.2:**

  * Don't assume that nio.Encoder and nio.Decoder are thread-safe (Hadrian).

  * Fixed a bug in map.values that prevented the Janino Java compiler from recognizing a type-safe situation (Hadrian).

**After tag v.0.8.4:**

  * Fixed a bug in ifnotnull implementation that causes Python syntax errors for types with apostrophes in their doc strings (Titus).

  * Fixed a bug in Titus implementation that returns a malformed UUID4.  The last block (as a hex string) was too long.  Updated Titus' tests

  * Changed hadrian-servlet to pull hadrian dependency from the online repository rather than requiring that it be built and installed locally.

  * Changed from the PUEL to Open Source license.  Hadrian, Titus, and Aurelius are now Apache License v2.0

  * 
