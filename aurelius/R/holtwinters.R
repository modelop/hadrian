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


#' extract_params.HoltWinters
#'
#' Extract Holt Winters model parameters from the stats library
#' 
#' @param object an object of class "HoltWinters"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- HoltWinters(co2)
#' extracted_model <- extract_params(model)
#' @export
extract_params.HoltWinters <- function(object, ...) {
  
  if(object$gamma == FALSE){
    list(alpha = if(is.numeric(object$alpha)) unname(object$alpha) else 0, 
         beta = if(is.numeric(object$beta)) unname(object$beta) else 0,
         state = list(level = if(is.finite(object$coefficients['a'])) unname(object$coefficients['a']) else 0, 
                      trend = if(is.finite(object$coefficients['b'])) unname(object$coefficients['b']) else 0)
    ) 
  } else {
    
    # you need to shift the cycle to match preds from predict.HoltWinters
    x <- unname(object$coefficients[grepl('s[0-9]+', names(object$coefficients))])
    n <- 1
    coeffs <- c(tail(x, n), head(x, -n))
    
    list(alpha = if(is.numeric(object$alpha)) unname(object$alpha) else 0, 
         beta = if(is.numeric(object$beta)) unname(object$beta) else 0,  
         gamma = if(is.numeric(object$gamma)) unname(object$gamma) else 0, 
         state = list(level = if(is.finite(object$coefficients['a'])) unname(object$coefficients['a']) else 0, 
                      trend = if(is.finite(object$coefficients['b'])) unname(object$coefficients['b']) else 0,
                      cycle = as.list(coeffs), 
                      multiplicative = object$seasonal == 'multiplicative')
    )
  }
}

#' PFA Formatting of Fitted Holt Winters Models
#'
#' This function takes a Holt Winters model fit using HoltWinters()  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "HoltWinters"
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
#' @seealso \code{\link[stats]{HoltWinters}} \code{\link{extract_params.HoltWinters}}
#' @examples
#' model <- HoltWinters(co2)
#' model_as_pfa <- pfa(model)
#' @export
pfa.HoltWinters <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, ...){
  
  # extract model parameters
  fit <- extract_params(object)
  
  is_periodic <- !is.null(fit$gamma)
  
  if(is_periodic){
    tm <- avro_typemap(HWModel = avro_record(fields = list(alpha = avro_double, 
                                                           beta = avro_double, 
                                                           gamma = avro_double,
                                                           state = avro_record(fields = list(level = avro_double, 
                                                                                             trend = avro_double,
                                                                                             cycle = avro_array(avro_double), 
                                                                                             multiplicative = avro_boolean)))))
    update_action_string <- 'if (!is.null(y <- input["y"])) {
                               new <- stat.sample.updateHoltWintersPeriodic(y, hw_model["alpha"], hw_model["beta"], hw_model["gamma"], hw_model["state"])    
                               hw_model["state"] <<- new
                             }'
  } else {
    tm <- avro_typemap(HWModel = avro_record(fields = list(alpha = avro_double, 
                                                           beta = avro_double, 
                                                           state = avro_record(fields = list(level = avro_double, 
                                                                                             trend = avro_double)))))
    update_action_string <- 'if (!is.null(y <- input["y"])) {
                               new <- stat.sample.updateHoltWinters(y, hw_model["alpha"], hw_model["beta"], hw_model["state"])    
                               hw_model["state"] <<- new
                             }'
  }

  this_cells <- list(hw_model = pfa_cell(type = tm("HWModel"), init = fit))
  
  forecast_action_string <- 'if (!is.null(h <- input["h"])) {
                               emit(stat.sample.forecastHoltWinters(h, hw_model["state"]))
                             }'
  this_action <- parse(text=paste(update_action_string,
                                  forecast_action_string,
                                  sep='\n '))
  
  # construct the pfa_document
  doc <- pfa_document(input = avro_record(fields = list(h = avro_union(avro_null, avro_int), 
                                                        y = avro_union(avro_null, avro_double))),
                      output = avro_array(avro_double),
                      cells = this_cells,
                      action = this_action,
                      fcns = NULL,
                      method = 'emit',
                      name=name, 
                      version=version, 
                      doc=doc, 
                      metadata=metadata, 
                      randseed=randseed, 
                      options=options,
                      ...
  )
  
  return(doc)
}