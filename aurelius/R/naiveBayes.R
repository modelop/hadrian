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


#' extract_params.naiveBayes
#'
#' Extracts a parameters from an ensemble made by the naiveBayes function
#' 
#' @param object an object of class "naiveBayes"
#' @param threshold	a value replacing cells with probabilities within eps range.
#' @param eps	a numeric for specifying an epsilon-range to apply laplace 
#' smoothing (to replace zero or close-zero probabilities by theshold.)
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} that is extracted from the naiveBayes object
#' @examples
#' model <- e1071::naiveBayes(Species ~ ., data=iris) 
#' model_params <- extract_params(model)
#' @export
extract_params.naiveBayes <- function(object, threshold = .001, eps = 0, ...) {

  n_classes <- length(object$levels)
  numeric_vars <- vector("list", n_classes)
  names(numeric_vars) <- object$levels
  categorical_vars <- vector("list", n_classes)
  names(categorical_vars) <- object$levels
  
  for(i in 1:length(object$tables)){
    
    this_var_name <- names(object$tables)[i]
    this_var_name <- gsub('\\.', '_', this_var_name)
    
    if(is.table(object$tables[[i]])){
      for(j in 1:nrow(object$tables[[i]])){
        
        this_var_params <- list()
        
        # replace probs if needed
        observed_probs <- object$tables[[i]][j,]
        observed_probs[observed_probs <= eps] <- threshold
        
        this_var_params[[1]] <- as.list(observed_probs)
        names(this_var_params) <- this_var_name
        categorical_vars[[rownames(object$tables[[i]])[j]]] <- c(categorical_vars[[rownames(object$tables[[i]])[j]]], this_var_params)
      }
    } else if(is.matrix(object$tables[[i]])){
      for(j in 1:nrow(object$tables[[i]])){
        
        this_var_params <- list()
        
        # replace variance if needed
        observed_stdev <- unname(object$tables[[i]][rownames(object$tables[[i]])[j],2])
        if(observed_stdev <= eps){
          this_stdev <- threshold
        } else {
          this_stdev <- observed_stdev
        }
        
        this_var_params[[1]] <- list(mean = unname(object$tables[[i]][rownames(object$tables[[i]])[j],1]),
                                     variance = this_stdev^2)
        
        names(this_var_params) <- this_var_name
        numeric_vars[[rownames(object$tables[[i]])[j]]] <- c(numeric_vars[[rownames(object$tables[[i]])[j]]], this_var_params)
      }
    } else {
      stop(sprintf('Could not determine if variable %s is categorical or numeric', 
                   this_var_name))
    }
  }
  
  # pull out classes, the prior distribition for the classes, and then 
  # the distributions for each classes for each model variable
  list(classes = object$levels, 
       apriori_counts = as.list(object$apriori), 
       numeric_cond_probs = numeric_vars, 
       categorical_cond_probs = categorical_vars)
}


#' build_model.naiveBayes
#'
#' Builds an entire PFA list of lists based on a naiveBayes
#' 
#' @param object a object of class naiveBayes
#' @param threshold	a value replacing cells with probabilities within eps range.
#' @param eps	a numeric for specifying an epsilon-range to apply laplace 
#' smoothing (to replace zero or close-zero probabilities by theshold.)
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists representation of the naiveBayes model that can be 
#' inserted into a cell or pool
#' @examples 
#' model <- e1071::naiveBayes(Species ~ ., data=iris) 
#' model_built <- build_model(model)
#' @export
build_model.naiveBayes <- function(object, threshold = .001, eps = 0, ...){
  
  # pull out the parameters from the object
  extracted_model <- extract_params(object = object, threshold = threshold, eps = eps)
  
  numeric_cond_probs <- extracted_model$numeric_cond_probs
  numeric_ind <- all(sapply(numeric_cond_probs, is.null))
  if(numeric_ind){
    for (i in 1:length(numeric_cond_probs)){
      numeric_cond_probs[[i]] <- list(dummy = list(mean = 0, 
                                             variance = 0))
    }
  }
  
  categorical_cond_probs <- extracted_model$categorical_cond_probs
  categorical_ind <- all(sapply(categorical_cond_probs, is.null))
  if(categorical_ind){
    for (i in 1:length(categorical_cond_probs)){
      categorical_cond_probs[[i]] <- list(dummy = list(dummy = 0))
    }
  }
  
  numeric_type <- avro_map(avro_map(avro_record(list(mean = avro_double, variance = avro_double))))
  categorical_type <- avro_map(avro_map(avro_map(avro_double)))
  
  # format the parameters as they would be needed in the PFA cell
  cell_ready <- list(numeric_cond_probs = pfa_cell(type = numeric_type, 
                                                   init = numeric_cond_probs),
                     categorical_cond_probs = pfa_cell(type = categorical_type, 
                                                       init = categorical_cond_probs),
                     apriori_counts = pfa_cell(type = avro_map(avro_double), 
                                               init = extracted_model$apriori_counts))
  return(cell_ready)
}


#' PFA Formatting of Fitted naiveBayess
#'
#' This function takes a Naive Bayes model fit using naiveBayes()
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "naiveBayes"
#' @param threshold	a value replacing cells with probabilities within eps range.
#' @param eps	a numeric for specifying an epsilon-range to apply laplace 
#' smoothing (to replace zero or close-zero probabilities by theshold.)
#' @param pred_type a string with value "response" for returning the predicted class 
#' or the value "prob", which for returns the predicted probability of each class.
#' @param cutoffs A named numeric vector of length equal to 
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
#' @seealso \code{\link[e1071]{naiveBayes}}
#' @examples
#' model <- e1071::naiveBayes(Species ~ ., data=iris) 
#' model_as_pfa <- pfa(model)
#' @export
pfa.naiveBayes <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                           threshold = .001, eps = 0,
                           pred_type = c('response', 'prob'), 
                           cutoffs = NULL, ...){
  
  model_cell <- build_model(object, threshold = threshold, eps = eps)

  # build out 1 action for each class (currently only way given limitations with case-case do statement)
  # TODO: Find a better way so that we only need to evaluate the value type once (string, double, null)
  # rather than repeat for each class
  class_ensembles <- paste0(sapply(seq.int(object$levels), FUN=function(x, object){
                                inv_class_action_string(class_idx = paste0('pred_class', x), 
                                                        class_name = object$levels[x])
                              }, object=object, USE.NAMES = FALSE),
                            collapse='\n ')
  all_class_preds <- paste0('preds <- new(avro_map(avro_double), ',
                             paste0(sapply(seq.int(object$levels), FUN=function(x, object){
                               sprintf('`%s` = a.sum(%s)', object$levels[x], paste0('pred_class', x))
                               }, object=object, USE.NAMES = FALSE), 
                             collapse=', '), ')')  
  
  which_pred_type <- match.arg(pred_type)
  this_cells <- model_cell
  if(which_pred_type == 'response'){
    cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = object$levels)
    output_type <- avro_string
    pred_type_expression <- 'map.argmax(u.cutoff_ratio_cmp(all_preds, cutoffs))'
    this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
    this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
  } else if(which_pred_type == 'prob'){
    output_type <- avro_map(avro_double)
    pred_type_expression <- 'all_preds'
    this_fcns <- NULL
  } else {
    stop('Only "response" and "prob" values are accepted for pred_type')
  }
  this_action <- parse(text=paste('var_names <- map.keys(input)', 
                                  class_ensembles, 
                                  all_class_preds, 
                                  adding_apriori_action_string,
                                  scaling_preds_action_string,
                                  pred_type_expression, 
                                  sep='\n '))
  
  # construct the pfa_document
  doc <- pfa_document(input = avro_map(avro_union(avro_null, avro_string, avro_double)),
                      output = output_type,
                      cells = this_cells,
                      action = this_action,
                      fcns = this_fcns,
                      name = name, 
                      version = version, 
                      doc = doc, 
                      metadata = metadata, 
                      randseed = randseed, 
                      options = options,
                      ...
  )
  
  return(doc)
}


#' @keywords internal
inv_class_action_string <- function(class_idx, class_name){
  return(sprintf(paste0('%s <- a.map(var_names, 
                                     function(var = avro_string -> avro_double)
                                              castblock(cast = input[var],
                                                        cases = list(castcase(as="string",named="y",do=m.ln(categorical_cond_probs["%s"][var][y])), 
                                                                     castcase(as="double",named="y",do=prob.dist.gaussianLL(y,numeric_cond_probs["%s"][var])),
                                                                     castcase(as="null",named="y",do=m.ln(1))),
                                                        partial=FALSE))'), class_idx, class_name, class_name))
}


#' @keywords internal
adding_apriori_action_string <- 'scaled_preds <- map.zipmap(apriori_counts, preds, function(a = avro_double, b = avro_double -> avro_double) m.ln(a) + b)'


#' @keywords internal
scaling_preds_action_string <- 'all_preds <- map.map(scaled_preds, 
                                                     function(class_val = avro_double -> avro_double)
                                                              1/a.sum(a.map(map.values(scaled_preds),
                                                                            function(a = avro_double -> avro_double)
                                                                                     m.exp(a - class_val))))'