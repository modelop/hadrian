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


#' extract_params.glm
#'
#' Extract generalized linear model parameters from the glm library
#' 
#' @param object an object of class "glm"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' X1 <- rnorm(100)
#' X2 <- runif(100)
#' Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
#' Y <- Y > 0
#' 
#' glm_model <- glm(Y ~ X1 + X2, family = binomial(logit))
#' model_params <- extract_params(glm_model)
#' @export
extract_params.glm <- function(object, ...) {
  
    coeff <- as.list(object$coefficients)
    
    const <- coeff[["(Intercept)"]]
    
    # handle the no intercept model
    if(is.null(const))
      const <- 0
    
    coeff[["(Intercept)"]] <- NULL
    
    regressors <- names(coeff)

    list(coeff = coeff,
         const = const,
         regressors = lapply(regressors, function(x) gsub("\\.", "_", x)),
         family = object$family$family, 
         link = object$family$link)
}


#' PFA Formatting of Fitted GLMs
#'
#' This function takes a generalized linear model fit using glm  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "glm"
#' @param pred_type a string with value "response" for returning a prediction on the 
#' same scale as what was provided during modeling, or value "prob", which for classification 
#' problems returns the probability of each class.
#' @param cutoffs (Classification only) A named numeric vector of length equal to 
#' number of classes. The "winning" class for an observation is the one with the 
#' maximum ratio of predicted probability to its cutoff. The default cutoffs assume the 
#' same cutoff for each class that is 1/k where k is the number of classes
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
#' @seealso \code{\link[stats]{glm}} \code{\link{extract_params.glm}}
#' @examples
#' X1 <- rnorm(100)
#' X2 <- runif(100)
#' Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
#' Y <- Y > 0
#' 
#' glm_model <- glm(Y ~ X1 + X2, family = binomial(logit))
#' model_as_pfa <- pfa(glm_model)
#' @export
pfa.glm <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                    pred_type = c('response', 'prob'), 
                    cutoffs = NULL, ...){
  
  # extract model parameters
  fit <- extract_params(object)
  
  # define the input schema
  field_names <- fit$regressors
  field_types <- rep(avro_double, length(field_names))
  names(field_types) <- field_names
  input_type <- avro_record(field_types, "Input")
  
  # create list defining the first action of constructing input
  glm_input_list <- list(type = avro_array(avro_double),
                         new = lapply(field_names, function(n) {
                           paste("input.", n, sep = "")
                           }))
  
  # determine the output based on pred_type
  this_cells <- list()
  cast_input_string <- 'glm_input <- glm_input_list'
  which_pred_type <- match.arg(pred_type)
  if(fit$family == 'binomial'){
    if(which_pred_type == 'response'){
      cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = c('0', '1'))
      output_type <- avro_string
      pred_type_expression <- 'map.argmax(u.cutoff_ratio_cmp(probs, cutoffs))'
      this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
      this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
    } else if(which_pred_type == 'prob'){
      output_type <- avro_map(avro_double)
      pred_type_expression <- 'probs'
      this_fcns <- NULL
    } else {
      stop('Only "response" and "prob" values are accepted for pred_type')
    }
    this_action <- parse(text=paste(cast_input_string, 
                                    paste0('pred <- ', glm_link_func_mapper(fit$link, 
                                                                            input_name='glm_input', 
                                                                            model_name='glm_model')),
                                    'probs <- new(avro_map(avro_double), `0` = 1 - pred, `1` = pred)',
                                    pred_type_expression,
                                    sep='\n '))
  } else {
    output_type <- avro_double
    pred_type_expression <- 'pred'
    this_action <- parse(text=paste(cast_input_string, 
                                    paste0('pred <- ', glm_link_func_mapper(fit$link, 
                                                                            input_name='glm_input', 
                                                                            model_name='glm_model')),
                                    pred_type_expression,
                                    sep='\n '))
    this_fcns <- NULL
  }
  
  # define the pfa_document framework (inputs, outputs, cells)
  tm <- avro_typemap(Input = input_type,
                     Output = output_type,
                     Regression = avro_record(list(const = avro_double, 
                                                   coeff = avro_array(avro_double)),
                                              paste0(fit$family, "Regression")))
  
  this_cells[['glm_model']] <- pfa_cell(tm("Regression"), 
                                        list(const = fit$const,
                                             coeff = unname(fit$coeff)))
  
  # construct the pfa_document
  doc <- pfa_document(input = tm("Input"),
                      output = tm("Output"),
                      cells = this_cells,
                      action = this_action,
                      fcns = this_fcns,
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


#' @keywords internal
glm_link_func_mapper <- function(link, input_name, model_name) {

  model <- sprintf('model.reg.linear(%s, %s)', input_name, model_name)
  
  switch(link,
         identity = model,
         log = paste0('m.exp(', model, ')'),
         inverse = paste0('1 / ', model),
         logit = paste0('m.link.logit(', model, ')'), 
         probit = paste0('m.link.probit(', model, ')'), 
         cauchit = paste0('m.link.cauchit(', model, ')'), 
         cloglog = paste0('m.link.cloglog(', model, ')'), 
         sqrt = paste0('(', model, ') ** 2'),
         `1/mu^2` = paste0('1 / m.sqrt(', model, ')'), 
         stop(sprintf('supplied link function not supported: %s', link))) 
}