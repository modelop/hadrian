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
