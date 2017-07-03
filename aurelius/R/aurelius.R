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

#' \code{aurelius} package
#'
#' Generates PFA Documents from R Code and Optionally Runs Them
#'
#' Converts R syntax into PFA and provides tools for assembling a PFA
#' document within R. Tests validity and runtime behavior of PFA by offloading
#' PFA and data to Titus (through rPython). Facilitates conversion of common R
#' model output to PFA using aurelius.* libraries.  Aurelius is part of 
#' Hadrian and is on Github at \url{https://github.com/opendatagroup/hadrian}.
#'
#' @docType package
#' @name aurelius
#' @examples
#' \dontrun{
#' library("aurelius")
#' 
#' # build a model
#' lm_model <- lm(mpg ~ hp, data = mtcars)
#'   
#' # convert the lm object to a list of lists PFA representation
#' lm_model_as_pfa <- pfa(lm_model)
#'   
#' # save as plain-text JSON
#' write_pfa(lm_model_as_pfa, file = "my-model.pfa")
#' 
#' # read the model back in
#' read_pfa(file("my-model.pfa"))
#' }
NULL