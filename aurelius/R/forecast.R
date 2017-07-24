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

#' extract_params.forecast
#'
#' Extract model parameters from a model with class "forecast" created using 
#' the forecast package
#' 
#' @param object an object of class "forecast"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- forecast::holt(airmiles)
#' extracted_model <- extract_params(model)
#' @export
extract_params.forecast <- function(object, ...) {
  
  extract_params(object$model, ...)
}
  

#' PFA Formatting of Time Series Models Fit using Forecast Package
#'
#' This function takes model with class "forecast" created using the forecast 
#' package and returns a list-of-lists representing in valid PFA document that 
#' could be used for scoring.
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "forecast"
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
#' @seealso \code{\link[forecast]{holt}} \code{\link[forecast]{ses}} \code{\link[forecast]{hw}} \code{\link{pfa.ets}}
#' @examples
#' model1 <- forecast::holt(airmiles)
#' model1_as_pfa <- pfa(model1)
#' 
#' model2 <- forecast::hw(USAccDeaths,h=48)
#' model2_as_pfa <- pfa(model2)
#' 
#' model3 <- forecast::ses(LakeHuron)
#' model3_as_pfa <- pfa(model3)
#' @export
pfa.forecast <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, ...){
  
  pfa(object$model, ...)
}