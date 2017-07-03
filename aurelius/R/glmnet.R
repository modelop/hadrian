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
#' @importFrom stats coef
#' @importFrom glmnet coef.glmnet
#' @param object an object of class "glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists that can be modified to insert into a cell or pool
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X[,1] + 3 * X[,2] + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet::glmnet(X, Y, family = 'binomial')
#' my_model_params <- extract_params(model)
#' @export
extract_params.glmnet <- function(object, lambda=NULL, ...) {
  
  net_type <- class(object)[class(object)!='glmnet']
  
  if(net_type %in% c('mrelnet')){
    stop(sprintf("Currently not supporting glmnet models of net type: %s", net_type))
  }
  
  if(is.null(lambda)){
    lambda <- object$lambda[round(2/3 * length(object$lambda))]
  }

  coef_obj <- coef(object, s = lambda)

  if(!is.list(coef_obj)){
    coef_obj <- list(coef_obj)  
  }
  # how to support multiclass problems
  mcoef <- do.call("cbind", lapply(coef_obj, function(x) as.matrix(x)))
  mcoef <- t(mcoef)
  mactive <- do.call("c", lapply(coef_obj, function(x) attributes(x)$i))
  mactive <- unique(c(1, sort(unique(mactive)) + 1)) # add the intercept regardless
  mcoef <- mcoef[,mactive, drop=F]
  if(!is.matrix(mcoef)){
    mcoef <- as.matrix(t(mcoef))
  }
  coeff <- as.list(unname(data.frame(unname(t(mcoef[,!(colnames(mcoef) %in% c("(Intercept)"))])))))
  const <- as.list(unname(mcoef[,(colnames(mcoef) == "(Intercept)")]))
  if(length(const) == 0){
    const <- list(0)
  }
  regressors <- dimnames(mcoef)[[2]][!(colnames(mcoef) %in% c("(Intercept)"))]
  regressors <- sapply(regressors, function(x) gsub("\\.", "_", x), USE.NAMES=FALSE)
  responses <- object$classnames
  if(net_type == 'multnet'){
    names(coeff) <- object$classnames
    names(const) <- object$classnames
  } else {
    # flip coeff and const back to vectors because 
    # that is how they are formatted individually for multnet
    coeff <- unlist(coeff)
    const <- unlist(const)
  }
  
  list(coeff = coeff,
       const = const,
       regressors = regressors,
       responses = responses,
       net_type = net_type) 
  
}


#' PFA Formatting of Fitted glmnet objects
#'
#' This function takes a generalized linear model fit using glmnet  
#' and returns a list-of-lists representing a valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
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
#' @seealso \code{\link[glmnet]{glmnet}} \code{\link{extract_params.glmnet}}
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X[,1] + 3 * X[,2] + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet::glmnet(X, Y, family = 'binomial')
#' model_as_pfa <- pfa(model)
#' @export
pfa.glmnet <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                       lambda = NULL,
                       pred_type = c('response', 'prob'),
                       cutoffs = NULL, ...){
  
  # extract model parameters
  fit <- extract_params(object, lambda = lambda)

  # define the input schema
  field_names <- fit$regressors
  field_types <- rep(avro_double, length(field_names))
  names(field_types) <- field_names
  input_type <- avro_record(field_types, "Input")
  
  # create list so that the first action is to construct
  # the inputs into glm_input
  glm_input_list <- list(type = avro_array(avro_double),
                       new = lapply(field_names, function(n) {
                         paste("input.", n, sep = "")
                         }))
  
  which_pred_type <- match.arg(pred_type)
  tm_regression <- avro_typemap(
    Regression = avro_record(list(const = avro_double, 
                                  coeff = avro_array(avro_double)),
                             paste0(fit$net_type, "Regression"))
  )
  
  
  this_cells <- list()
  cast_input_string <- 'glm_input <- glm_input_list'
  cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = fit$responses) 
  if(is.null(fit$responses)){
    output_type <- avro_double
    pred_type_expression <- 'pred'
    this_action <- parse(text=paste(
      cast_input_string, 
      paste0('pred <- ', glmnet_link_func_mapper(fit$net_type, input_name='glm_input', model_name='reg')),
      pred_type_expression,
      sep='\n '))
    this_fcns <- NULL
    this_cells[['reg']] <- pfa_cell(tm_regression("Regression"),
                                    list(const = fit$const,
                                         coeff = as.list(fit$coeff))) 
  } else {
    if(length(fit$responses) == 2){
      # binomial
      if(which_pred_type == 'response'){
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
      this_action <- parse(text=paste(
        cast_input_string,
        paste0('pred <- ', glmnet_link_func_mapper(fit$net_type, input_name='glm_input', model_name='reg')),
        paste0('probs <- new(avro_map(avro_double), `', 
                                       fit$responses[1],
                                       '` = 1 - pred, `', 
                                       fit$responses[2],
                                       '` = pred)'),
        pred_type_expression,
        sep='\n '))
      this_cells[['reg']] <- pfa_cell(tm_regression("Regression"),
                                      list(const = fit$const,
                                           coeff = as.list(fit$coeff)))
    } else {
      # multinomial
      class_regs <- paste0(sapply(seq.int(fit$responses),
                                   FUN=function(x, net_type){
                                      paste0('pred_class', x, ' <- ', 
                                              glmnet_link_func_mapper(net_type, 
                                                                      input_name='glm_input', 
                                                                      model_name=paste0('reg_class', x)))
                                  }, net_type=fit$net_type, USE.NAMES = FALSE),
                              collapse='\n ')
      all_class_preds <- paste0('all_preds <- new(avro_map(avro_double), ',
                                             paste0(sapply(seq.int(fit$responses), FUN=function(x, fit){
                                               sprintf('`%s` = %s', fit$responses[x], paste0('pred_class', x))
                                               }, fit=fit, USE.NAMES = FALSE), 
                                             collapse=', '), ')')    
      if(which_pred_type == 'response'){
        output_type <- avro_string
        pred_type_expression <- 'map.argmax(u.cutoff_ratio_cmp(all_preds, cutoffs))'
        this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
        this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
      } else if(which_pred_type == 'prob'){
        output_type <- avro_map(avro_double)
        pred_type_expression <- 'la.scale(all_preds, 1/a.sum(map.fromset(all_preds)))'
        this_fcns <- NULL
      } else {
        stop('Only "response" and "prob" values are accepted for pred_type')
      }
      this_action <- parse(text=paste(
        cast_input_string, 
        class_regs, 
        all_class_preds, 
        pred_type_expression, 
        sep='\n '))
      for(i in seq.int(fit$responses)){
        this_cells[[paste0('reg_class', i)]] <- pfa_cell(tm_regression("Regression"),
                                                         list(const = fit$const[[i]],
                                                              coeff = as.list(fit$coeff[[i]])))
      }
    }
  }
  
  tm <- avro_typemap(
    Input = input_type,
    Output = output_type,
    Regression = tm_regression("Regression")
  )
  
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


#' extract_params.cv.glmnet
#'
#' Extract generalized linear model net parameters from a cv.glmnet object
#' 
#' @param object an object of class "cv.glmnet"
#' @param lambda a numeric value of the penalty parameter lambda at which 
#' coefficients are required
#' @param ... further arguments passed to or from other methods
#' @return PFA as a \code{list} of lists that can be inserted into a cell or pool
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X[,1] + 3 * X[,2] + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet::cv.glmnet(X, Y, family = 'binomial')
#' my_model_params <- extract_params(model)
#' @export 
extract_params.cv.glmnet <- function(object, lambda = object[["lambda.1se"]], ...) {
    
  extract_params(object = object$glmnet.fit, lambda = lambda)
  
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
#' @seealso \code{\link[glmnet]{glmnet}} \code{\link{extract_params.glmnet}}
#' @examples
#' X <- matrix(c(rnorm(100), runif(100)), nrow=100, ncol=2)
#' Y <- factor(3 - 5 * X[,1] + 3 * X[,2] + rnorm(100, 0, 3) > 0)
#' 
#' model <- glmnet::cv.glmnet(X, Y, family = 'binomial')
#' model_as_pfa <- pfa(model)
#' @export
pfa.cv.glmnet <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                          lambda = object[["lambda.1se"]],
                          pred_type = c('response', 'prob'),
                          cutoffs = NULL, ...){
  
  which_pred_type <- match.arg(pred_type)
  
  pfa(object = object$glmnet.fit, 
      lambda = lambda, 
      pred_type = which_pred_type, 
      cutoffs = cutoffs, 
      name=name, 
      version=version, 
      doc=doc, 
      metadata=metadata, 
      randseed=randseed, 
      options=options, ...)
  
}


# not yet supporting
# mrelnet - multiresponse Gaussian
#' @keywords internal
glmnet_link_func_mapper <- function(net_type, input_name, model_name) {

  model <- sprintf('model.reg.linear(%s, %s)', input_name, model_name)
  
  switch(net_type,
         elnet = model,
         multnet = paste0('m.exp(', model, ')'),
         fishnet = paste0('m.exp(', model, ')'),
         lognet = paste0('m.link.logit(', model, ')'), 
         coxnet = paste0('m.exp(', model, ')'), 
         stop(sprintf('supplied net type not supported: %s', net_type))) 
}