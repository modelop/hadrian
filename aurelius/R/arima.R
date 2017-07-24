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

#' extract_params.Arima
#'
#' Extract model parameters from an ARIMA model created 
#' using the arima(), Arima(), or auto.arima() functions
#' 
#' @param object an object of class "Arima"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- stats::arima(presidents, c(3, 0, 0))
#' extracted_model <- extract_params(model)
#' 
#' model <- forecast::Arima(USAccDeaths, order=c(2,2,2), seasonal=c(0,2,2))
#' extracted_model <- extract_params(model)
#'
#' model <- forecast::auto.arima(WWWusage)
#' extracted_model <- extract_params(model)
#' @export
extract_params.Arima <- function(object, ...) {
  
  this_intercept <- as.numeric(object$coef["intercept"]) 
  this_intercept <- if(!is.finite(this_intercept)) 0 else this_intercept

  xr <- object$call$xreg
  
  if(is.null(xr)){
    xreg <- NULL 
  } else {
    # find the object in some environment
    this_obj <- tryCatch({
      eval.parent(xr, n=1)
    }, error=function(e){
      return(NULL)
    })
    
    if(is.null(this_obj)){
      this_obj <- tryCatch({
        eval.parent(xr, n=2)
      }, error=function(e){
        return(NULL)
      })      
    }
    
    # finally try the global environment
    if(is.null(this_obj)){
      this_obj <- tryCatch({
        eval(xr, .GlobalEnv)
      }, error=function(e){
        return(NULL)
      })      
    }
    
    xreg_names <- names(this_obj)  
    if(is.null(xreg_names)){
      if(ncol(as.data.frame(this_obj)) == 1){
        xreg_names <- as.character(xr)
      } else {
        stop(paste('Could not determine the names of each external regressor to match up coefficients.', 
                   'Consider passing the xreg argument as a data.frame.'))
      }
    }
    xreg <- as.list(object$coef[xreg_names])
    names(xreg) <- validate_names(names(xreg))  
  }
  
  list(trans_matrix = object$model$T, 
       current_state = object$model$a, 
       obs_coeffs = object$model$Z,
       xreg_coeffs = xreg,
       intercept = this_intercept)
}

#' PFA Formatting of ARIMA Models
#'
#' This function takes an ARIMA model created using the arima(), Arima(), or auto.arima() functions 
#' and returns a list-of-lists representing in valid PFA document that could be used 
#' for scoring.
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "Arima"
#' @param cycle_reset a logical indicating whether to reset the state back to the 
#' last point of the trained model before forecasting or to continue cycling forward 
#' through trend and seasonality with every new call to the engine. The default is 
#' TRUE so that repeated calls yield the same forecast as repeated calls to 
#' \code{\link[stats]{predict}} or \code{\link[forecast]{forecast}}.
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
#' @seealso \code{\link[forecast]{Arima}} \code{\link[forecast]{auto.arima}} \code{\link[stats]{arima}} \code{\link{extract_params.Arima}}
#' @examples
#' model <- forecast::Arima(USAccDeaths, order=c(2,2,2), seasonal=c(0,2,2))
#' model_as_pfa <- pfa(model)
#' 
#' # with regressors
#' n <- 100
#' ext_dat <- data.frame(x1=rnorm(n), x2=rnorm(n))
#' x <- stats::arima.sim(n=n, model=list(ar=0.4)) + 2 + 0.8*ext_dat[,1] + 1.5*ext_dat[,2]
#' model <- stats::arima(x, order=c(1,0,0), xreg = ext_dat)
#' model_as_pfa <- pfa(model)
#' @export
pfa.Arima <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                      cycle_reset = TRUE, ...){
  
  # extract model parameters
  fit <- extract_params(object)
  has_xreg <- !is.null(fit$xreg_coeffs)

  this_cells <- list(arima_trans_matrix = pfa_cell(type = avro_array(avro_array(avro_double)),
                                                   init = matrix_to_arr_of_arr(fit$trans_matrix)),
                     arima_current_state = pfa_cell(type = avro_array(avro_array(avro_double)),
                                                    init = matrix_to_arr_of_arr(as.matrix(fit$current_state))),
                     arima_obs_coeffs = pfa_cell(type = avro_array(avro_array(avro_double)),
                                                 init = list(as.list(fit$obs_coeffs))),
                     arima_intercept = pfa_cell(type = avro_double,
                                                init = fit$intercept))
  
  if(has_xreg){
    
    field_names <- names(fit$xreg_coeffs)
    field_types <- lapply(seq.int(length(field_names)), FUN=function(x){avro_array(avro_double)})
    names(field_types) <- field_names
    xreg_type <- avro_record(field_types)
    
    input_type <- avro_record(fields = list(h = avro_int, 
                                            xreg = xreg_type))
    
    this_cells$arima_xreg_coeffs <- pfa_cell(type = avro_array(avro_array(avro_double)),
                                             init = matrix_to_arr_of_arr(as.matrix(fit$xreg_coeffs)))
    
    xreg_input_list <- list(type = avro_array(avro_array(avro_double)),
                            new = lapply(field_names, function(n) {
                              paste('input.xreg.', n, sep = "")
                            }))
    
    cast_input_string <- 'xreg_input <- xreg_input_list'
  } else {
    input_type <- avro_record(fields = list(h = avro_int))
    cast_input_string <- ''
  }
  
  blank_arr <- gen_blank_array(avro_double)
  this_action <- parse(text=paste('h <- input["h"]', 
                                  'if(h <= 0) {
                                     stop("Forecast horizon out of bounds")  
                                   }',
                                  cast_input_string,
                                  if(cycle_reset) 'original_state <- arima_current_state' else '',
                                  'preds <- blank_arr',
                                  if(has_xreg) 'xreg_constants <- la.dot(la.transpose(xreg_input), arima_xreg_coeffs)',
                                  if(has_xreg) 'h <- a.len(xreg_constants)' else '', #mimic behavior of predict() which makes h=max(h,nrow(xreg))
                                  'for (i in 1:h){
                                     new_state <- la.dot(arima_trans_matrix, arima_current_state)
                                     this_pred <- la.dot(arima_obs_coeffs, new_state)[0][0] + arima_intercept',
                                     if(has_xreg) 'this_pred <- this_pred + xreg_constants[i-1][0]' else '',
                                    'preds <- a.append(preds, this_pred)
                                     arima_current_state <<- new_state
                                   }',
                                  if(cycle_reset) 'arima_current_state <<- original_state' else '',
                                  'preds', sep='\n'))
  
  # construct the pfa_document
  doc <- pfa_document(input = input_type,
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