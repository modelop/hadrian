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
#' @param input_name a character for the name of the input as it should appear 
#' in the PFA document
#' @param model_name a character for the name of the model as it should appear 
#' in the PFA document
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' X1 <- rnorm(100)
#' X2 <- runif(100)
#' Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
#' Y <- Y > 0
#' 
#' z <- glm(Y ~ X1 + X2, family = binomial(logit))
#' z2 <- extract_params(z)
#' @export

extract_params.glm <- function(object, input_name='glm_input', model_name='glm_model') {
  
    coeff <- as.list(fit$coefficients)
    
    const <- coeff[["(Intercept)"]]
    
    # handle the no intercept model
    if(is.null(const))
      const <- 0
    
    coeff[["(Intercept)"]] <- NULL
    
    covmatrix <- vcov(object)
    covar <- vector("list", nrow(covmatrix))
    for (i in 1:nrow(covmatrix)) {
        row <- vector("list", ncol(covmatrix))
        for (j in 1:ncol(covmatrix)) {
            if (j == 1)
                jj = ncol(covmatrix)  # intercept goes last, not first
            else
                jj = j - 1
            row[[jj]] = covmatrix[i, j]
        }
        if (i == 1)
            ii = nrow(covmatrix)  # intercept goes last, not first
        else
            ii = i - 1
        covar[[ii]] = row
    }
    
    regressors <- names(coeff)

    list(coeff = coeff,
         const = const,
         covar = covar,
         regressors = lapply(regressors, function(x) gsub("\\.", "_", x)),
         family = object$family$family, 
         link = glm_link_func_mapper(object$family$link, input_name, model_name),
         input_name = input_name,
         model_name = model_name)
}

#' PFA Formatting of Fitted GLMs
#'
#' This function takes a generalized linear model fit using glm  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R
#' @param object an object of class "glm"
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
#' model <- glm(Y ~ X1 + X2, family = binomial(logit))
#' model_as_pfa <- pfa(model)
#' 
#' @export
pfa.glm <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, ...){
  
  # extract model parameters
  fit <- extract_params(object)
  
  # define the input schema
  field_names <- fit$regressors
  field_types <- rep(avro_double, length(field_names))
  names(field_types) <- field_names
  input_schema <- avro_record(field_types, "Input")
  
  # define the pfa_document framework (inputs, outputs, cells)
  tm <- avro_typemap(Input = input_schema,
                     Output = avro_double,
                     Regression = avro_record(list(coeff = avro_array(avro_double),
                                                   const = avro_double),
                                              paste0(fit$family, "Regression")))
  
  # create list defining the first action of constructing input
  glm_input_list <- list(type = avro_array(avro_double),
                         new = lapply(field_names, function(n) {
                           paste("input.", n, sep = "")
                           }))
  
  # construct the pfa_document
  doc <- pfa_document(input = tm("Input"),
                      output = tm("Output"),
                      cells = list(glm_model = pfa_cell(tm("Regression"),
                                                        list(const = fit$const,
                                                             coeff = unname(fit$coeff)))),
                      action = parse(text=paste(paste(fit$input_name, '<-', 'glm_input_list'), 
                                                fit$link, 
                                                sep='\n ')),
                      name=name, 
                      version=version, 
                      doc=doc, 
                      metadata=metadata, 
                      randseed=randseed, 
                      options=options
  )
  
  return(doc)
}


glm_link_func_mapper <- function(link, input_name='glm_input', model_name='glm_model') {

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
