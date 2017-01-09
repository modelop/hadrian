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

#' pfa.glm.extractParams
#'
#' Extract generalized linear model parameters from the glm library
#' @param fit an object of class "glm"
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' X1 <- rnorm(100)
#' X2 <- runif(100)
#' Y <- 3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3)
#' Y <- Y > 0
#' 
#' z <- glm(Y ~ X1 + X2, family = binomial(logit))
#' z2 <- pfa.glm.extractParams(z)
#' @export pfa.glm.extractParams
pfa.glm.extractParams <- function(fit, input_name='glm_input', model_name='glm_model') {
  
    if (!("glm" %in% class(fit)))
        stop("pfa.glm.extractParams requires an object of class \"glm\"")

    coeff <- as.list(fit$coefficients)
    
    const <- coeff[["(Intercept)"]]
    
    # handle the no intercept model
    if(is.null(const))
      const <- 0
    
    coeff[["(Intercept)"]] <- NULL
    
    covmatrix <- vcov(fit)
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
         family = fit$family$family, 
         link = glm_link_func_mapper(fit$family$link, input_name, model_name),
         input_name = input_name,
         model_name = model_name)
}

#' PFA Formatting of Fitted GLMs
#'
#' This function takes a generalized linear model fit using glm  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @usage pfa(model)
#' @source pfa.config.R avro.typemap.R avro.R
#' @param object an object of class "glm"
#' @return a \code{list} of lists that compose valid PFA document
#' @seealso \code{\link[stats]{glm}} \code{\link{pfa.glm.extractParams}}
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
pfa.glm <- function(object){
  
  # extract model parameters
  fit <- pfa.glm.extractParams(object)
  
  # define the input schema
  field_names <- fit$regressors
  field_types <- rep(avro.double, length(field_names))
  names(field_types) <- field_names
  input_schema <- avro.record(field_types, "Input")
  
  # define the pfa_document framework (inputs, outputs, cells)
  tm <- avro.typemap(Input = input_schema,
                     Output = avro.double,
                     Regression = pfa.glm.regressionType(fit))
  
  # create list defining the first action of constructing input
  glm_input_list <- create_glm_input_list(field_names)
  
  # construct the pfa_document
  pfa_document <- pfa.config(input = tm("Input"),
                             output = tm("Output"),
                             cells = list(glm_model = pfa.cell(tm("Regression"),
                                                               list(const = fit$const,
                                                                    coeff = unname(fit$coeff)))),
                             action = parse(text=paste(paste(fit$input_name, '<-', 'glm_input_list'), 
                                                       fit$link, 
                                                       sep='\n '))
  )
  
  return(pfa_document)
}

pfa.glm.inputType <- function(params, name = NULL, namespace = NULL) {
  fields = list()
  for (x in params$regressors)
      fields[[x]] <- avro.double
  avro.record(fields, name, namespace)
}

pfa.glm.regressionType <- function(params) {

  avro.record(list(coeff = avro.array(avro.double),
                   const = avro.double),
              paste0(params$family, "Regression"))
}

pfa.glm.predictProb <- function(params, input) {
  
    symbolsEnv <- new.env()
    symbolsEnv[[params$input_name]] <- list(avro.array(avro.double),
                                            lapply(params$regressors, function (x)
                                              list(attr = input, path = list(list(string = x)))))
    
    cellsEnv <- new.env()
    cellsEnv[[params$model_name]] <- pfa.cell(create_glm_reg_record(),
                                              list(const = params$const,
                                                   coeff = unname(params$coeff)))   
    
    out <- pfa.expr(parse(text = params$link), 
                    symbols = symbolsEnv, 
                    cells = cellsEnv)$do[[1]]
    return(out)
}

pfa.glm.modelParams <- function(params) {
    list(coeff = params$coeff, const = params$const)
}

create_glm_input_list <- function(field_names) {
  list(type = avro.array(avro.double),
       new = lapply(field_names, function(n) {
         paste("input.", n, sep = "")
         }))
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
