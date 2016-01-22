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

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.TextNode
import org.codehaus.jackson.node.BooleanNode

import com.opendatagroup.hadrian.errors.PFAInitializationException

package options {
  /** Represents the `options` section of a PFA document with host-specific overrides.
    * 
    * Unrecognized option names will be ignored.
    * 
    * Recognized option names whose option type is incorrect raise [[com.opendatagroup.hadrian.errors.PFAInitializationException PFAInitializationException]].
    * 
    * Options currently supported by Hadrian:
    * 
    *  - "timeout" (`Long`): number of milliseconds to allow a PFA method to run before raising [[com.opendatagroup.hadrian.errors.PFATimeoutException PFATimeoutException]].
    *  - "timeout.begin" (`Long`): overrides "timeout" for the "begin" method
    *  - "timeout.action" (`Long`): overrides "timeout" for the "action" method
    *  - "timeout.end" (`Long`): overrides "timeout" for the "end" method
    *  - "data.PFARecord.interface" (`String`): interface to inherit PFA records from (in addition to the ones it already inherits); this interface must exist in the classpath and must not require any methods that a `PFARecord` does not already have (including signatures)
    *  - "lib.model.neighbor.nearestK.kdtree" (`Boolean`): if `true`, build a kd-tree for neighbor sets sent to the `lib.model.neighbor.nearestK` function as a ''O(log(N))'' optimization; if `false`, do an ''O(N)'' lookup every time
    * 
    * @param requestedOptions options requested by PFA.
    * @param hostOptions options mandated by the host environment, which will override the `requestedOptions` if there is a conflict.
    */
  class EngineOptions(requestedOptions: Map[String, JsonNode], hostOptions: Map[String, JsonNode]) {
    val combinedOptions = requestedOptions ++ hostOptions
    val overridenKeys = hostOptions.keys.toSet intersect requestedOptions.keys.toSet

    private def longOpt(name: String, default: Long): Long = combinedOptions.get(name) match {
      case None => default
      case Some(jsonNode) if (jsonNode.isIntegralNumber) => jsonNode.getLongValue
      case _ => throw new PFAInitializationException(name + " must be an integral number")
    }

    val timeout = longOpt("timeout", -1)
    val timeout_begin = longOpt("timeout.begin", timeout)
    val timeout_action = longOpt("timeout.action", timeout)
    val timeout_end = longOpt("timeout.end", timeout)

    val data_pfarecord_interface = combinedOptions.get("data.PFARecord.interface") flatMap {_ match {
      case x: TextNode => Some(x.getTextValue)
      case _ => throw new PFAInitializationException("data.PFARecord.interface must be a string")
    }}

    val lib_model_neighbor_nearestK_kdtree: Boolean = combinedOptions.get("lib.model.neighbor.nearestK.kdtree") map {_ match {
      case x: BooleanNode => x == BooleanNode.TRUE
      case _ => throw new PFAInitializationException("lib.model.neighbor.nearestK.kdtree must be boolean")
    }} getOrElse false

    // ...

  }
}
