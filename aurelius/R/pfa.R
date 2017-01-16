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

#' Generate PFA Document from Object 
#' 
#' pfa is a generic function for generating valid PFA documents from the results 
#' of various model fitting functions. The function invokes particular methods 
#' which depend on the class of the first argument.
#'
#' @param object a model object for which a PFA document is desired
#' @param name a character which is an optional name for the scoring engine
#' @param version	an integer which is sequential version number for the model
#' @param doc	a character which is documentation string for archival purposes
#' @param metadata a \code{list} of strings that is computer-readable documentation for 
#' archival purposes
#' @param randseed a integer which is a global seed used to generate all random 
#' numbers. Multiple scoring engines derived from the same PFA file have 
#' different seeds generated from the global one
#' @param options	a \code{list} with value types depending on option name
#' Initialization or runtime options to customize implementation 
#' (e.g. optimization switches). May be overridden or ignored by PFA consumer
#' @param ...	additional arguments affecting the PFA produced
#' @return a \code{list} of lists that compose a valid PFA document
#' @seealso \code{\link{pfa.lm}} \code{\link{pfa.glm}}
#' @examples
#' \dontrun{
#' require(utils)
#' 
#' # all the "pfa" methods found
#' methods("pfa")
#' }
#' @export
pfa <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, ...) UseMethod("pfa")


#' extract_tree
#'
#' Extracts a tree from a tree model object
#' 
#' @param object a tree model object
#' @return a \code{list} that is extracted from the tree model object
#' @examples
#' \dontrun{
#' require(utils)
#' 
#' # all the "extract_tree" methods found
#' methods("extract_tree")
#' }
#' @export

extract_tree <- function(object, ...) UseMethod("extract_tree")


#' build_tree
#'
#' Builds an entire PFA list of lists based on a single tree model object
#' 
#' @param object a tree model object
#' @return a \code{list} of lists representation of the tree that can be 
#' inserted into a cell or pool
#' @examples
#' \dontrun{
#' require(utils)
#' 
#' # all the "build_tree" methods found
#' methods("build_tree")
#' }
#' @export

build_tree <- function(object, ...) UseMethod("build_tree")
