package com.opendatagroup.hadrian

import org.codehaus.jackson.JsonNode

import com.opendatagroup.hadrian.errors.PFAInitializationException

package options {
  object EngineOptions {
    val recognizedKeys = Set("@", "timeout", "timeout.begin", "timeout.action", "timeout.end")
  }

  class EngineOptions(requestedOptions: Map[String, JsonNode], hostOptions: Map[String, JsonNode]) {
    val combinedOptions = requestedOptions ++ hostOptions
    val overridenKeys = hostOptions.keys.toSet intersect requestedOptions.keys.toSet
    val unrecognizedKeys = combinedOptions.keys.toSet diff EngineOptions.recognizedKeys

    if (!unrecognizedKeys.isEmpty)
      throw new PFAInitializationException("unrecognized options: " + unrecognizedKeys.toList.sorted.mkString(" "))

    private def longOpt(name: String, default: Long): Long = combinedOptions.get(name) match {
      case None => default
      case Some(jsonNode) if (jsonNode.isIntegralNumber) => jsonNode.getLongValue
      case _ => throw new PFAInitializationException(name + " must be an integral number")
    }

    val timeout = longOpt("timeout", -1)
    val timeout_begin = longOpt("timeout.begin", timeout)
    val timeout_action = longOpt("timeout.action", timeout)
    val timeout_end = longOpt("timeout.end", timeout)

    // ...

  }
}
