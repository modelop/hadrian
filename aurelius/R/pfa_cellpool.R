# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' pfa_cell
#'
#' Creates a \code{list} of lists representing a PFA cell.
#' @param type cell type, which is an Avro schema as \code{list} of lists (created by avro_* functions)
#' @param init cell initial value, which is a \code{list} of lists, usually converted from a model
#' @param source if "embedded", the init is the data structure, if "json", the init is a URL string pointing to an external JSON file
#' @param shared if TRUE, the cell is shared across scoring engine instances
#' @param rollback if TRUE, the cell's value would be rolled back if an uncaught exception is encountered
#' @return a \code{list} of lists that can be inserted into pfa_config.
#' @examples
#' pfa_cell(avro_double, 12)
#' @export
pfa_cell <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)


#' pfa_pool
#'
#' Creates a \code{list} of lists representing a PFA pool.
#' @param type pool type, which is an Avro schema as \code{list} of lists (created by avro_* functions)
#' @param init pool initial value, which is a \code{list} of lists, usually converted from a model
#' @param source if "embedded", the init is the data structure, if "json", the init is a URL string pointing to an external JSON file
#' @param shared if TRUE, the pool is shared across scoring engine instances
#' @param rollback if TRUE, the pool's value would be rolled back if an uncaught exception is encountered
#' @return a \code{list} of lists that can be inserted into pfa_config.
#' @examples
#' pfa_pool(avro_double, json_map(one = 1.1, two = 2.2, three = 3.3))
#' @export
pfa_pool <- function(type, init, source = "embedded", shared = FALSE, rollback = FALSE)
    list(type = type, init = init, source = source, shared = shared, rollback = rollback)

