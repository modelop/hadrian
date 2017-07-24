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


#' json_map
#'
#' Convenience function for making a (possibly empty) named list, which converts to a JSON object.
#' @param ... optional contents of the named list (as key-value pairs)
#' @return a named list
#' @examples
#' json_map()
#' json_map(one = 1, two = TRUE, three = "THREE")
#' @export
json_map <- function(...) {
    args <- list(...)
    if (length(args) == 0)
        setNames(list(), list())
    else if (is.null(names(args)))
        stop("json_map must have named parameters")
    else
        args
}


#' json_array
#'
#' Convenience function for making a (possibly empty) unnamed list, which 
#' converts to a JSON array.
#' 
#' @importFrom stats setNames
#' @param ... optional contents of the unnamed list
#' @return an unnamed list
#' @examples
#' json_array()
#' json_array(1, TRUE, "THREE")
#' @export
json_array <- function(...) {
    args <- list(...)
    if (!is.null(names(args))){
      stop("json_array must not have named parameters")
    }
    else {
      args
    }
}