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


#' extract_params.kmeans
#'
#' Extract K-means model parameters from a kmeans object
#' 
#' @source kcca.R
#' @param object an object of class "kmeans"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- kmeans(x=iris[, 1:2], centers=3)
#' extracted_params <- extract_params(model)
#' @export
extract_params.kmeans <- extract_params.kcca


#' PFA Formatting of Fitted K-means Models
#'
#' This function takes a K-means model fit using kmeans  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R kcca.R
#' @param object an object of class "kmeans"
#' @param cluster_names a character vector of length k to name the values relating
#' to each cluster instead of just an integer. If not specified, then the predicted 
#' cluster will be the string representation of the cluster index.
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
#' @return a \code{list} of lists that compose valid PFA document
#' @seealso \code{\link[stats]{kmeans}} \code{\link{pfa.kcca}}
#' @examples
#' model <- kmeans(x=iris[, 1:2], centers=3)
#' model_as_pfa <- pfa(model)
#' @export
pfa.kmeans <- pfa.kcca

