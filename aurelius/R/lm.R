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


#' PFA Formatting of Fitted Linear models
#'
#' This function takes a linear model fit using lm and 
#' returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @importFrom stats gaussian
#' @source pfa.config.R avro.typemap.R avro.R
#' @param object an object of class "lm"
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
#' @seealso \code{\link[stats]{lm}} \code{\link{pfa.glm}}
#' @examples
#' X1 <- rnorm(100)
#' X2 <- runif(100)
#' Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
#' 
#' model <- lm(Y ~ X1 + X2)
#' model_as_pfa <- pfa(model)
#' @export
pfa.lm <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, ...){
  
  if (!("lm" %in% class(object)))
    stop("pfa.lm requires an object of class \"lm\"")
  
  # add family function and add class to 
  # become glm and inherit its pfa function (pfa.glm)
  object$family <- gaussian('identity')
  class(object) <- c("glm", object$class)
  
  return(pfa(object, 
             pred_type = 'response', 
             cutoffs = NULL,
             name=name, 
             version=version, 
             doc=doc, 
             metadata=metadata, 
             randseed=randseed, 
             options=options, ...))
}