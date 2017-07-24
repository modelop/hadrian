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

#' extract_params.ets
#'
#' Extract model parameters from an Exponential smoothing state space model created 
#' using the ets() function from the forecast package.
#' 
#' @param object an object of class "ets"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- forecast::ets(USAccDeaths, model="ZZZ")
#' extracted_model <- extract_params(model)
#' @export
extract_params.ets <- function(object, ...) {
  
  n <- length(object$x)
  last_state <- object$states[n + 1, ]
  trendtype <- object$components[2]
  seasontype <- object$components[3]
  damped <- as.logical(object$components[4])
  m <- object$m
  par <- object$par  
  
  if(trendtype == 'M'){
    stop("Currently not supporting models with multiplicative trend")
  }
  if(seasontype == 'M'){
    stop("Currently not supporting models with multiplicative seasonality")
  }
  
  # create H, indicator matrix of parameters
  # with zeros for the seasonal components because 
  # they will be rotated through using another vector
  p <- length(last_state)
  model_H <- matrix(c(1, rep(0, p - 1)), nrow = 1)
  if (seasontype == "A") 
    model_H[1, p] <- 1
  if (trendtype == "A") {
    if (damped) 
      model_H[1, 2] <- par["phi"]
    else model_H[1, 2] <- 1
  }
  
  # create F, diagonal matrix for cycling through 
  # seasonal components
  model_F <- matrix(0, p, p)
  model_F[1, 1] <- 1
  if (trendtype == "A") {
    if (damped) 
      model_F[1, 2] <- model_F[2, 2] <- par["phi"]
    else model_F[1, 2] <- model_F[2, 2] <- 1
  }
  if (seasontype == "A") {
    model_F[p - m + 1, p] <- 1
    model_F[(p - m + 2):p, (p - m + 1):(p - 1)] <- diag(m - 1)
  }
  
  model_Fj <- diag(p)
  
  list(last_state = last_state, 
       H = model_H, 
       F = model_F, 
       Fj = model_Fj)
}

#' PFA Formatting of Fitted Exponential Smoothing State Space Models
#'
#' This function takes an Exponential smoothing state space model created 
#' using the ets() function from the forecast package and returns a 
#' list-of-lists representing in valid PFA document that could be used for scoring.
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "ets"
#' @param cycle_reset a logical indicating whether to reset the state back to the 
#' last point of the trained model before forecasting or to continue cycling forward 
#' through trend and seasonality with every new call to the engine. The default is 
#' TRUE so that repeated calls yield the same forecast as repeated calls to 
#' \code{\link[forecast]{forecast}}.
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
#' @seealso \code{\link[forecast]{ets}} \code{\link{extract_params.ets}}
#' @examples
#' model <- forecast::ets(USAccDeaths, model="ZZZ")
#' model_as_pfa <- pfa(model)
#' @export
pfa.ets <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                    cycle_reset = TRUE, ...){
  
  # extract model parameters
  fit <- extract_params(object)

  this_cells <- list(ets_last_state = pfa_cell(type = avro_array(avro_array(avro_double)), 
                                               init = matrix_to_arr_of_arr(matrix(fit$last_state))),
                     ets_H = pfa_cell(type = avro_array(avro_array(avro_double)), 
                                      init = list(as.list(fit$H))),
                     ets_F = pfa_cell(type = avro_array(avro_array(avro_double)),
                                      init = matrix_to_arr_of_arr(fit$F)),
                     ets_Fj = pfa_cell(type = avro_array(avro_array(avro_double)),
                                       init = matrix_to_arr_of_arr(fit$Fj)))
  
  blank_arr <- gen_blank_array(avro_double)
  this_action <- parse(text=paste('if(input["h"] <= 0) {
                                     stop("Forecast horizon out of bounds")  
                                   }',
                                  if(cycle_reset) 'original_FJ <- ets_Fj' else '',
                                  'preds <- blank_arr',
                                  'for (i in 1:input["h"]){
                                     preds <- a.append(preds, la.dot(la.dot(ets_H, ets_Fj), ets_last_state)[0][0])
                                     ets_Fj <<- la.dot(ets_Fj, ets_F)
                                   }',
                                  if(cycle_reset) 'ets_Fj <<- original_FJ' else '',
                                  'preds', sep='\n'))
  
  # construct the pfa_document
  doc <- pfa_document(input = avro_record(fields = list(h = avro_int)),
                      output = avro_array(avro_double),
                      cells = this_cells,
                      action = this_action,
                      fcns = NULL,
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