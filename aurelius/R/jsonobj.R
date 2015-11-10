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

#' json.map
#'
#' Convenience function for making a (possibly empty) named list, which converts to a JSON object.
#' @param args optional contents of the named list (as key-value pairs)
#' @return a named list
#' @export json.map
#' @examples
#' json.map()
#' json.map(one = 1, two = TRUE, three = "THREE")

json.map <- function(...) {
    args <- list(...)
    if (length(args) == 0)
        setNames(list(), list())
    else if (is.null(names(args)))
        stop("json.map must have named parameters")
    else
        args
}

#' json.array
#'
#' Convenience function for making a (possibly empty) unnamed list, which converts to a JSON array.
#' @param args optional contents of the unnamed list
#' @return an unnamed list
#' @export json.array
#' @examples
#' json.array()
#' json.array(1, TRUE, "THREE")

json.array <- function(...) {
    args <- list(...)
    if (!is.null(names(args)))
        stop("json.array must not have named parameters")
    else
        args
}
