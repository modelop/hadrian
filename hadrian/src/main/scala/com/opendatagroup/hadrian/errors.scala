// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian

package errors {
  /** Base class for all PFA exceptions (not used directly).
    * 
    * @param message human-readable message
    * @param cause chained exception or `null`
    */
  class PFAException(val message: String, val cause: Exception) extends RuntimeException(message, cause)

  /** Exception found in the syntax of the PFA document (valid JSON but invalid structure for PFA).
    * 
    * A `PFASyntaxException` is raised if the rules in the PFA specification are violated to the extent that Hadrian cannot build an abstract syntax tree.
    * 
    * @param message human-readable message
    * @param pos locator mark, PFA path index, or `None`
    */
  class PFASyntaxException(override val message: String, val pos: Option[String])
      extends PFAException("PFA syntax error%s: %s".format(pos match {
        case Some(position) => " " + position
        case None => ""
      }, message), null)

  /** Exception found in the semantics of the PFA document (type mismatches, unrecognized symbols, etc.).
    * 
    * A `PFASemanticException` is raised if the rules in the PFA specification are violated to the extent that Hadrian cannot build Java code from the abstract syntax tree.
    * 
    * @param message human-readable message
    * @param pos locator mark, PFA path index, or `None`
    */
  class PFASemanticException(override val message: String, val pos: Option[String])
      extends PFAException("PFA semantic error%s: %s".format(pos match {
        case Some(position) => " " + position
        case None => ""
      }, message), null)

  /** Exception found in the initialization of the PFA document (loading cell/pool data, initializing scoring engine state).
    * 
    * A `PFAInitializationException` is raised if the rules in the PFA specification are violated to the extent that Hadrian cannot provide a scoring engine instance to the calling program.
    * 
    * @param message human-readable message.
    */
  class PFAInitializationException(override val message: String)
      extends PFAException(message, null)

  /** Exception found at runtime in a PFA library function (attempt to get contents of an empty array, etc.).
    * 
    * A `PFARuntimeException` can arise if no PFA rules are violated, but runtime conditions are such that a library function cannot proceed.
    * 
    * @param message human-readable message that is strictly set in the PFA specification
    * @param code numeric code for the message, also set by the PFA specification
    * @param fcnName name of the library function that encountered the error
    * @param pos locator mark, PFA path index, or `None`
    */
  class PFARuntimeException(override val message: String, val code: Int, val fcnName: String, val pos: Option[String], cause: Exception = null)
      extends PFAException("%s in %s (#%d)%s".format(message, fcnName, code, pos match {
        case Some(position) => " " + position
        case None => ""
      }), cause)

  /** Exception intentionally raised by the user.
    * 
    * A `PFAUserException` can arise if no PFA rules are violated, even when nothing is wrong. The user simply wanted an exception to occur.
    * 
    * @param message human-readable message
    * @param optional numeric code
    * @param pos locator mark, PFA path index, or `None`
    */
  class PFAUserException(override val message: String, val code: Option[Int], val pos: Option[String])
      extends PFAException("%s%s%s".format(message, code match {
        case Some(c) => " (%d)".format(c)
        case None => ""
      }, pos match {
        case Some(position) => " " + position
        case None => ""
      }), null)

  /** Exception raised if a PFA method takes too long.
    * 
    * A `PFATimeoutException` can arise if no PFA rules are violated, even when nothing is wrong. The begin, action, or end method simply took too long to process (may be in infinite loop).
    * 
    * @param message human-readable message
    */
  class PFATimeoutException(override val message: String)
      extends PFAException(message, null)
}
