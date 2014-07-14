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
