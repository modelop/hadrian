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

#' extract_params.lda
#'
#' Extract linear discriminant model parameters from a model created
#' by the lda() function
#'
#' @param object an object of class "lda"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- MASS::lda(Species ~ ., data=iris)
#' model_params <- extract_params(model)
#' @export
extract_params.lda <- function(object, ...) {

  # protect against column names with dots
  # this is not allowed in PFA
  colnames(object$means) <- gsub('\\.', '_', colnames(object$means))
  rownames(object$scaling) <- gsub('\\.', '_', rownames(object$scaling))
  names(object$xlevels) <- gsub('\\.', '_', names(object$xlevels))
  this_input_vars <- attr(object$terms, "term.labels")
  this_input_vars <- gsub('\\.', '_', this_input_vars)
  
  if(!is.null(object$xlevels)){
    
    factor_vars <- names(object$xlevels)
    all_expected_variables <- c()
    
    for(f in factor_vars){
      these_ind_vars <- paste0(f, tail(object$xlevels[[f]],-1))
      all_expected_variables <- c(all_expected_variables, these_ind_vars)
    }
    
    # put all the non_factor variables first 
    # since this is how the PFA input step will approach 
    # to making an array of all non-factor variables first, then 
    # concatentating each of the k-1 indicator variables for each factor variable
    non_factor_vars <- this_input_vars[!(this_input_vars %in% names(object$xlevels))]
    all_expected_variables <- c(non_factor_vars, all_expected_variables)
    
    object$means <- object$means[ ,all_expected_variables, drop=F]
    object$scaling <- object$scaling[all_expected_variables, , drop=F]
  }

  params <- list(inputs = this_input_vars, 
                 factor_levels = object$xlevels,
                 classes = object$lev,
                 prior_probs = object$prior,
                 means = object$means,
                 scaling = object$scaling,
                 svd = object$svd)
  
  return(params)
}


#' build_model.lda
#'
#' Builds an entire PFA list of lists based on a lda() fit
#'
#' @param object a object of class lda
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists representation of the linear discriminant model that can be
#' inserted into a cell or pool
#' @examples
#' model <- MASS::lda(Species ~ ., data=iris)
#' model_built <- build_model(model)
#' @export
build_model.lda <- function(object, ...){

  # pull out the parameters from the object
  extracted_params <- extract_params(object = object)
  
  means_init <- list()
  for(i in 1:nrow(extracted_params$means)){
    means_init[[i]] <- as.list(unname(extracted_params$means[i,]))
  }
  
  scaling_init <- list()
  for(i in 1:nrow(extracted_params$scaling)){
    scaling_init[[i]] <- as.list(unname(extracted_params$scaling[i,]))
  }

  # format the parameters as they would be needed in the PFA cell
  cell_ready <- list(prior = pfa_cell(type = avro_array(avro_array(avro_double)), 
                                      init = list(as.list(unname(extracted_params$prior_probs)))),
                     means = pfa_cell(type = avro_array(avro_array(avro_double)),
                                      init = means_init),
                     scaling = pfa_cell(type = avro_array(avro_array(avro_double)),
                                        init = scaling_init),
                     svd = pfa_cell(type = avro_array(avro_array(avro_double)),
                                    init = list(as.list(object$svd))))
  return(cell_ready)
}


#' PFA Formatting of Fitted Linear Discriminant Models
#'
#' This function takes a linear discriminant model fit using lda
#' and returns a list-of-lists representing in valid PFA document
#' that could be used for scoring
#'
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "lda"
#' @param prior a named vector specifying the prior probabilities of class membership.  
#' If unspecified, the class proportions for the training set are used.
#' @param dimen an integer specifying the dimension of the space to be used. 
#' If this is less than min(p input variables, number of classes - 1) then the 
#' first N number of dimensions will be used in the calculation
#' @param method a character string indicating the prediction method. Currently, 
#' only the plug-in method is supported.
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
#' @seealso \code{\link[MASS]{lda}} \code{\link{extract_params.lda}}
#' @examples
#' model <- MASS::lda(Species ~ ., data=iris)
#' model_as_pfa <- pfa(model)
#' @export
pfa.lda <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL,
                    prior = object$prior, dimen=length(object$svd), method = c('plug-in'),
                    pred_type = c('response', 'prob'),
                    cutoffs = NULL, ...){

  # extract model parameters
  extracted_params <- extract_params(object)
  model_cell <- build_model(object)

  # define the input schema
  # doubles up front
  has_doubles <- FALSE
  has_enums <- FALSE
  model_enum_vars <- names(extracted_params$factor_levels)
  model_double_vars <- extracted_params$input[!(extracted_params$input %in% model_enum_vars)]
  field_types <- list()
  if(length(model_double_vars) > 0){
    field_names <- model_double_vars
    field_types <- c(field_types, as.list(rep(avro_double, length(field_names))))
    names(field_types) <- field_names
    # create list defining the first action of constructing input
    lda_doubles_input_list <- list(type = avro_array(avro_double),
                                   new = lapply(field_names, 
                                                function(n) {
                                                  paste("input.", n, sep = "")
                                                  }
                                                )
                                   )
    has_doubles <- TRUE
  }
  
  enum_action_casts <- c()
  if(length(model_enum_vars) > 0){
    for(i in seq.int(length(model_enum_vars))){
      field_types[[length(field_types) + 1]] <- avro_enum(symbols = as.list(extracted_params$factor_levels[[model_enum_vars[[i]]]]))
      names(field_types)[length(field_types)] <- model_enum_vars[i]
      f <- FALSE
      if(!has_doubles & i == 1){
        f <- TRUE
      }
      enum_action_casts <- c(enum_action_casts,
                             enum_to_model_matrix_string(var_name = model_enum_vars[i],
                                                         target_array = 'lda_input', 
                                                         first = f))
    }
    has_enums <- TRUE
  }
  
  input_type <- avro_record(field_types, "Input")
  
  cast_input_strings <- c()
  if(has_doubles){
    cast_input_strings <- c(cast_input_strings, 'lda_input <- lda_doubles_input_list')
    if(has_enums){
      cast_input_strings <- c(cast_input_strings, enum_action_casts)  
    }
  } else {
    if(has_enums){
      cast_input_strings <- enum_action_casts
    } else {
      stop('Could not find any doubles or enums variables')
    }
  }
  
  initial_cast_action_string <- paste(cast_input_strings, collapse='\n')
  this_classes <- extracted_params$classes
  all_class_preds <- paste0('all_preds <- new(avro_map(avro_double), ',
                            paste0(sapply(seq.int(this_classes), FUN=function(x, classes){
                              sprintf('`%s` = %s', classes[x], paste0('scaled_preds[', x-1, ']'))
                              }, classes=this_classes, USE.NAMES = FALSE), 
                              collapse=', '), ')')    

  # determine the output based on pred_type
  this_cells <- model_cell
  this_cells[['dimen']] <- pfa_cell(type = avro_int, 
                                    init = min(length(extracted_params$svd), dimen))
  this_cells[['svd']] <- NULL # not needed since we are storing the dimen separately
  if(!all(extracted_params$classes %in% names(prior))){
    stop('The supplied prior does not contain for all classes. Please supply a named vector with one element per class and sums to 1.')
  }
  if(!all.equal(sum(prior), 1, tolerance = .01)){
    stop('The supplied prior must sum to 1.')
  }
  this_prior <- prior[extracted_params$classes]
  this_cells[['prior']] <- pfa_cell(type = avro_array(avro_array(avro_double)), 
                                    init = list(as.list(unname(this_prior))))
  this_fcns <- c(ln_la_fcn, exp_la_fcn, normalize_array_fcn)
  which_pred_type <- match.arg(pred_type)
  if(which_pred_type == 'response'){
    cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = extracted_params$classes)
    output_type <- avro_string
    pred_type_expression <- 'map.argmax(u.cutoff_ratio_cmp(all_preds, cutoffs))'
    this_fcns <- c(this_fcns, divide_fcn, cutoff_ratio_cmp_fcn)
    this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
  } else if(which_pred_type == 'prob'){
    output_type <- avro_map(avro_double)
    pred_type_expression <- 'all_preds'
  } else {
    stop('Only "response" and "prob" values are accepted for pred_type')
  }
  
  # create some blank arrays used in the action string
  blank_arr_of_arr <- gen_blank_array_of_arrays(avro_double)
  blank_arr <- gen_blank_array(avro_double)
  
  # piece together the action strings needed to perform prediction calculation
  this_action <- parse(text=paste(initial_cast_action_string,
                                  'lda_input_arr_of_arr <- new(avro_array(avro_array(avro_double)), lda_input)',
                                  lda_action_string,
                                  all_class_preds,
                                  pred_type_expression,
                                  sep='\n'))
  
  # construct the pfa_document
  doc <- pfa_document(input = input_type,
                      output = output_type,
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
enum_to_model_matrix_string <- function(var_name, target_array, first=FALSE) {
  if(!first){
    x <- sprintf('%s <- a.concat(%s, a.remove(cast.fanoutDouble(input["%s"]), 0))',
                 target_array, target_array, var_name)
  } else {
    x <- sprintf('%s <- a.remove(cast.fanoutDouble(input["%s"]), 0)',
                 target_array, var_name)
  }
  return(x)
}


#' @keywords internal
lda_action_string <- 'prior_means <- la.dot(prior,means)
input2 <- la.dot(la.sub(lda_input_arr_of_arr, prior_means), scaling)
centered_means <- blank_arr_of_arr
for(x in means) {
  centered_means <- a.append(centered_means, la.sub(x, prior_means[0]))
}
scaled_means <- la.dot(centered_means, scaling)
trunc_scaled_means <- la.transpose(la.truncate(la.transpose(scaled_means), dimen))
input2 <- la.transpose(la.truncate(la.transpose(input2), dimen))
scaled_means3 <- blank_arr
for(t in trunc_scaled_means) {
  scaled_means3 <- a.append(scaled_means3, a.sum(
                                              a.map(t, 
                                                    function(x = avro_double -> avro_double)
                                                      .5 * x ** 2
                                                    )
                                                )
                            )
}
scaled_means4 <- new(avro_array(avro_array(avro_double)),scaled_means3)
dist <- la.sub(la.sub(scaled_means4,u.la.ln(prior)),la.dot(input2,la.transpose(trunc_scaled_means)))
dist_min <- a.min(dist[0])
dist_min_arr <- blank_arr
for(d in dist[0]){
  dist_min_arr <- a.append(dist_min_arr, dist_min)
}
scaled_preds <- u.a.normalize(u.la.exp(la.scale(la.sub(dist, new(avro_array(avro_array(avro_double)), dist_min_arr)), -1))[0])
'