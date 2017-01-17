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

#' extract_params.glmnet
#'
#' Extract generalized linear model net parameters from the glmnet library
#' 
#' @param object an object of class "glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
#' @return PFA as a \code{list} of lists that can be inserted into a cell or pool
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet(X, Y, family = 'binomial')
#' my_model_params <- extract_params(model)
#' @export 

extract_params.glmnet <- function(object, lambda=NULL, 
                                  input_name='glm_input', model_name='glm_model') {
  
  if(is.null(lambda)){
    lambda <- object$lambda[round(2/3 * length(object$lambda))]
  }

  coef_obj <- coef(object, s = lambda)
  
  coeff <- as.list(attributes(coef_obj)$x)
  names(coeff) <- attributes(coef_obj)$Dimnames[[1]][(attributes(coef_obj)$i + 1)]
  
  const <- coeff[["(Intercept)"]]
  
  # handle the no intercept model
  if(is.null(const))
    const <- 0
  
  coeff[["(Intercept)"]] <- NULL

  # how to support multiclass problems
  # mcoef <- do.call("cbind", lapply(coef_obj, function(x) as.matrix(x)))
  # mcoef = t(mcoef)
  # mactive <- do.call("c", lapply(coef_obj, function(x) attributes(x)$i))
  # mactive <- sort(unique(mactive)) + 1
  # mcoef <- mcoef[,mactive]
  # coeff <- as.list(unname(data.frame(unname(t(mcoef[,-1])))))
  # const <- as.list(unname(mcoef[,1]))
  # regressors <- dimnames(mcoef)[[2]][-1]
  # responses <- fit$classnames

  regressors <- names(coeff)
  regressors <- lapply(regressors, function(x) gsub("\\.", "_", x))
  
  net_type <- class(object)[class(object)!='glmnet']
  
  list(coeff = coeff,
       const = const,
       regressors = regressors,
       responses = object$classnames,
       net_type = net_type, 
       link = glmnet_link_func_mapper(net_type, input_name, model_name),
       input_name = input_name,
       model_name = model_name)
  
}


#' PFA Formatting of Fitted glmnet objects
#'
#' This function takes a generalized linear model fit using glmnet  
#' and returns a list-of-lists representing a valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R
#' @param object an object of class "glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
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
#' @seealso \code{\link[glmnet]{glmnet}} \code{\link{extract_params.glmnet}}
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet(X, Y, family = 'binomial')
#' model_as_pfa <- pfa(model)
#' 
#' @export
pfa.glmnet <- function(object, lambda = NULL, 
                       name=NULL, version=NULL, doc=NULL, metadata=NULL, 
                       randseed=NULL, options=NULL, ...){
  
  # extract model parameters
  fit <- extract_params.glmnet(object, lambda = lambda)
  
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
                                              paste0(fit$net_type, "Regression")))
  
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


#' extract_params.cv.glmnet
#'
#' Extract generalized linear model net parameters from a cv.glmnet object
#' 
#' @param object an object of class "cv.glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
#' @return PFA as a \code{list} of lists that can be inserted into a cell or pool
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3) > 0)
#' 
#' model <- cv.glmnet(X, Y, family = 'binomial')
#' my_model_params <- extract_params(model)
#' @export 
extract_params.cv.glmnet <- function(object, lambda = object[["lambda.1se"]], 
                                     input_name='glm_input', model_name='glm_model') {
    
  extract_params(object$glmnet.fit, lambda)
  
}


#' PFA Formatting of Fitted glmnet objects
#'
#' This function takes a generalized linear model fit using cv.glmnet  
#' and returns a list-of-lists representing a valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R
#' @param object an object of class "cv.glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
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
#' @seealso \code{\link[glmnet]{glmnet}} \code{\link{extract_params.glmnet}}
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X1 + 3 * X2 + rnorm(100, 0, 3) > 0)
#' 
#' model <- cv.glmnet(X, Y, family = 'binomial')
#' model_as_pfa <- pfa(model)
#' 
#' @export
pfa.cv.glmnet <- function(object, lambda = object[["lambda.1se"]],
                          name=NULL, version=NULL, doc=NULL, metadata=NULL, 
                          randseed=NULL, options=NULL, ...){
  
  pfa(object$glmnet.fit, lambda)
  
}


# not yet supporting
# mrelnet - multiresponse Gaussian
# multnet - multinomial

glmnet_link_func_mapper <- function(net_type, input_name='glm_input', model_name='glm_model') {

  model <- sprintf('model.reg.linear(%s, %s)', input_name, model_name)
  switch(net_type,
         elnet = model,
         fishnet = paste0('m.exp(', model, ')'),
         lognet = paste0('m.link.logit(', model, ')'), 
         coxnet = paste0('m.exp(', model, ')'), 
         stop(sprintf('supplied net type not supported: %s', net_type))) 
}
