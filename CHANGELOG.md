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

**Future:**

  * Added model.reg.* regression module (_____, _____, and  _____).    NOT STARTED!

