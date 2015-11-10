# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# functions to build cells and pools (not really necessary, just helps make the code self-documenting)

#' pfa.cell
#'
#' Creates a list-of-lists representing a PFA cell.
#' @param type cell type, which is an Avro schema as list-of-lists (created by avro.* functions)
#' @param init cell initial value, which is a list-of-lists, usually converted from a model
#' @param source if "embedded", the init is the data structure, if "json", the init is a URL string pointing to an external JSON file
#' @param shared if TRUE, the cell is shared across scoring engine instances
#' @param rollback if TRUE, the cell's value would be rolled back if an uncaught exception is encountered
#' @return a list-of-lists that can be inserted into pfa.config.
#' @export pfa.cell
#' @examples
#' pfa.cell(avro.double, 12)

pfa.cell <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)

#' pfa.pool
#'
#' Creates a list-of-lists representing a PFA pool.
#' @param type pool type, which is an Avro schema as list-of-lists (created by avro.* functions)
#' @param init pool initial value, which is a list-of-lists, usually converted from a model
#' @param source if "embedded", the init is the data structure, if "json", the init is a URL string pointing to an external JSON file
#' @param shared if TRUE, the pool is shared across scoring engine instances
#' @param rollback if TRUE, the pool's value would be rolled back if an uncaught exception is encountered
#' @return a list-of-lists that can be inserted into pfa.config.
#' @export pfa.pool
#' @examples
#' pfa.pool(avro.double, json.map(one = 1.1, two = 2.2, three = 3.3))

pfa.pool <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)
