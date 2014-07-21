// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// 
// Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian

package errors {
  class PFAException(message: String, cause: Exception) extends RuntimeException(message, cause)

  class PFASyntaxException(message: String, pos: Option[String])
      extends PFAException("PFA syntax error%s: %s".format(pos match {
        case Some(position) => " " + position
        case None => ""
      }, message), null)

  class PFASemanticException(message: String, pos: Option[String])
      extends PFAException("PFA semantic error%s: %s".format(pos match {
        case Some(position) => " " + position
        case None => ""
      }, message), null)

  class PFAInitializationException(message: String)
      extends PFAException("PFA initialization error: " + message, null)

  class PFARuntimeException(val message: String, cause: Exception = null)
      extends PFAException("PFA runtime error: " + message, cause)

  class PFAUserException(val message: String, val code: Option[Int])
      extends PFAException("PFA user-defined error: " +
        (if (code == None) message else "%s (code %s)".format(message, code.get)), null)

  class PFATimeoutException(val message: String)
      extends PFAException("PFA timeout error: " + message, null)
}
