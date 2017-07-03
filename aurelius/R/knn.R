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


#' extract_params.knn3
#'
#' Extract K-nearest neighbor model parameters from a knn3 object created by 
#' the caret library
#'
#' @source pfa_utils.R
#' @param object an object of class "knn3"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' iris2 <- iris
#' colnames(iris2) <- gsub('\\.', '_', colnames(iris2))
#' model <- caret::knn3(Species ~ ., iris2)
#' extracted_params <- extract_params(model)
#' @export
extract_params.knn3 <- function(object, ...) {
  
  if(class(object)[1] %in% c('knn3', 'ipredknn', 'knnreg')){
    
    this_k <- object$k
    
    this_y <- object$learn$y
    if(is.factor(this_y)){
      this_y <- as.character(this_y)
      this_output_type <- 'classification'
    } else {
      this_output_type <- 'regresssion'
    }
    
    this_x <- object$learn$X
    this_input_vars <- validate_names(colnames(this_x))
    colnames(this_x) <- this_input_vars

    this_codebook <- as_codebook(y = this_y, x = this_x)
    
  } else {
    stop(sprintf("Currently not supporting nearest neighbor models of class %s", class(object)[1]))
  }
  
  list(k = this_k, 
       input_vars = this_input_vars,
       output_type = this_output_type,
       codebook = this_codebook)
}

#' PFA Formatting of Fitted knns
#'
#' This function takes a k-nearest neighbor fit using knn3 
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "knn3"
#' @param pred_type a string with value "response" for returning a prediction on the 
#' same scale as what was provided during modeling, or value "prob", which for classification 
#' problems returns the probability of each class.
#' @param cutoffs (Classification only) A named numeric vector of length equal to 
#' number of classes. The "winning" class for an observation is the one with the 
#' maximum ratio of predicted probability to its cutoff. The default cutoffs assume the 
#' same cutoff for each class that is 1/k where k is the number of classes.
#' @param distance_measure a string representing the type of distance calculation 
#' in order to determine the nearest neighbours.
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
#' @seealso \code{\link[caret]{knn3}} \code{\link{extract_params.knn3}}
#' @examples
#' iris2 <- iris
#' colnames(iris2) <- gsub('\\.', '_', colnames(iris2))
#' model <- caret::knn3(Species ~ ., iris2)
#' model_as_pfa <- pfa(model)
#' @export
pfa.knn3 <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                     pred_type = c('response', 'prob'), 
                     cutoffs = NULL, 
                     distance_measure = c('euclidean', 'manhattan', 'angle', 'jaccard', 'ejaccard'), ...){
  
  extracted_params <- extract_params(object)
  
  which_distance_measure <- match.arg(distance_measure)
  if(which_distance_measure %in% c('euclidean', 'manhattan', 'angle')){
    input_avro_type <- avro_double
  } else {
    input_avro_type <- avro_boolean
  }
  
  # define the input schema
  field_names <- extracted_params$input_vars
  field_types <- rep(input_avro_type, length(field_names))
  names(field_types) <- field_names
  input_type <- avro_record(field_types, "Input")
  
  # create list defining the first action of constructing input
  knn_input_list <- list(type = avro_array(input_avro_type),
                          new = lapply(field_names, function(n) {
                            paste("input.", n, sep = "")
                          }))
  cast_input_string <- 'knn_input <- knn_input_list'
  
  target_type <- if(extracted_params$output_type == 'classification') avro_string else avro_double
  if(which_distance_measure %in% c('euclidean', 'manhattan')){
    input_type <- avro_map(avro_double)
    input_name <- 'knn_input'
    tm <- avro_typemap(Codebook = avro_record(fields = list(target = target_type, center = avro_array(avro_double))))
  } else if(which_distance_measure %in% c('jaccard', 'ejaccard')){
    input_type <- avro_map(avro_boolean)
    input_name <- 'knn_input'
    tm <- avro_typemap(Codebook = avro_record(fields = list(target = target_type, center = avro_array(avro_boolean))))
  } else if(which_distance_measure %in% c('angle')){
    input_type <- avro_map(avro_double)
    input_name <- 'knn_input2'
    special_angle_string <- 'knn_input2 <- new(avro_array(avro_array(avro_double)), knn_input)'
    cast_input_string <- paste(cast_input_string, special_angle_string, sep='\n')
    tm <- avro_typemap(Codebook = avro_record(fields = list(target = target_type, center = avro_array(avro_double))))
  } else {
    stop('codebook cannot be created for knn models with distance measure %s', which_distance_measure)
  }
  
  validate_codebook(extracted_params$codebook)
  # determine the output based on pred_type
  this_cells <- list(k = pfa_cell(type = avro_int, 
                                  init = as.integer(extracted_params$k)),
                     codebook = pfa_cell(type = avro_array(tm("Codebook")),
                                         init = extracted_params$codebook))
  
  which_pred_type <- match.arg(pred_type)
  if(extracted_params$output_type == 'classification'){
    
    all_possible_targets <- unique(sapply(extracted_params$codebook,
                                          FUN = function(x){
                                            x$target
                                          }))
    cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = all_possible_targets)
    this_cells[['all_targets']] <- pfa_cell(type = avro_array(avro_string), 
                                            init = as.list(all_possible_targets))
    if(which_pred_type == 'response'){
      output_type <- avro_string
      aggregator_action <- parse(text=paste('res <- a.concat(all_targets, a.map(closest, function(x = tm("Codebook") -> avro_string) x["target"]))', 
                                            'counts <- a.groupby(res, function(a = avro_string -> avro_string) a)', 
                                            'probs <- map.map(counts, function(c = avro_array(avro_string) -> avro_double) (a.len(c) - 1) / k)', 
                                            'map.argmax(u.cutoff_ratio_cmp(probs, cutoffs))',
                                            sep="\n "))
      this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
      this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
    } else if(which_pred_type == 'prob'){
      output_type <- avro_map(avro_double)
      aggregator_action <- parse(text=paste('res <- a.concat(a.map(closest, function(x = tm("Codebook") -> avro_string) x["target"]), all_targets)', 
                                            'counts <- a.groupby(res, function(a = avro_string -> avro_string) a)', 
                                            'probs <- map.map(counts, function(c = avro_array(avro_string) -> avro_double) (a.len(c) - 1) / k)', 
                                            'probs',
                                            sep="\n "))
      this_fcns <- NULL
    } else {
      stop('Only "response" and "prob" values are accepted for pred_type')
    }
    
  } else {
    output_type <- avro_double
    aggregator_action <- parse(text=paste('res <- a.map(closest, function(x = tm("Codebook") -> avro_double) x["target"])', 
                                          'a.mean(res)',
                                          sep="\n "))
    this_fcns <- NULL
  }
  
  this_action <- parse(text=paste(cast_input_string,
                                  knn_func_mapper(distance_measure = which_distance_measure,
                                                  k = extracted_params$k,
                                                  input_name = input_name, 
                                                  output_name = 'closest',
                                                  codebook_name = 'codebook'),
                                  aggregator_action,
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


#' extract_params.knnreg
#'
#' Extract K-nearest neighbor model parameters from a knnreg object created by 
#' the caret library
#' 
#' @param object an object of class "knnreg"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- caret::knnreg(mpg ~ cyl + hp + am + gear + carb, data = mtcars)
#' extracted_params <- extract_params(model)
#' @export
extract_params.knnreg <- extract_params.knn3


#' PFA Formatting of Fitted knns
#'
#' This function takes a k-nearest neighbor fit using knnreg 
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "knnreg"
#' @param pred_type a string with value "response" for returning a prediction on the 
#' same scale as what was provided during modeling, or value "prob", which for classification 
#' problems returns the probability of each class.
#' @param cutoffs (Classification only) A named numeric vector of length equal to 
#' number of classes. The "winning" class for an observation is the one with the 
#' maximum ratio of predicted probability to its cutoff. The default cutoffs assume the 
#' same cutoff for each class that is 1/k where k is the number of classes.
#' @param distance_measure a string representing the type of distance calculation 
#' in order to determine the nearest neighbours.
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
#' @seealso \code{\link[caret]{knnreg}} \code{\link{extract_params.knn3}}
#' @examples
#' model <- caret::knnreg(mpg ~ cyl + hp + am + gear + carb, data = mtcars)
#' model_as_pfa <- pfa(model)
#' @export
pfa.knnreg <- pfa.knn3


#' extract_params.ipredknn
#'
#' Extract K-nearest neighbor model parameters from a ipredknn object created by 
#' the ipred library
#' 
#' @param object an object of class "ipredknn"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' iris2 <- iris
#' colnames(iris2) <- gsub('\\.', '_', colnames(iris2))
#' model <- ipred::ipredknn(Species ~ ., iris2)
#' params <- extract_params(model)
#' @export
extract_params.ipredknn <- extract_params.knn3


#' PFA Formatting of Fitted knns
#'
#' This function takes a k-nearest neighbor fit using ipredknn 
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "ipredknn"
#' @param pred_type a string with value "response" for returning a prediction on the 
#' same scale as what was provided during modeling, or value "prob", which for classification 
#' problems returns the probability of each class.
#' @param cutoffs (Classification only) A named numeric vector of length equal to 
#' number of classes. The "winning" class for an observation is the one with the 
#' maximum ratio of predicted probability to its cutoff. The default cutoffs assume the 
#' same cutoff for each class that is 1/k where k is the number of classes.
#' @param distance_measure a string representing the type of distance calculation 
#' in order to determine the nearest neighbours.
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
#' @seealso \code{\link[ipred]{ipredknn}} \code{\link{extract_params.knn3}}
#' @examples
#' iris2 <- iris
#' colnames(iris2) <- gsub('\\.', '_', colnames(iris2))
#' model <- ipred::ipredknn(Species ~ ., iris2)
#' model_as_pfa <- pfa(model)
#' @export
pfa.ipredknn <- pfa.knn3


#' @keywords internal
knn_func_mapper <- function(distance_measure, k, input_name, output_name, codebook_name) {
  switch(distance_measure,
         euclidean = sprintf('%s <- model.cluster.closestN(%s,%s,%s)', output_name, k, input_name, codebook_name),
         manhattan = sprintf('%s <- model.cluster.closestN(%s,%s,%s,manhattan_dist_fun)', output_name, k, input_name, codebook_name),
         angle = sprintf('%s <- model.cluster.closestN(%s,%s,%s,angle_dist_fun)', output_name, k, input_name, codebook_name), 
         jaccard = sprintf('%s <- model.cluster.closestN(%s,%s,%s,jaccard_dist_fun)', output_name, k, input_name, codebook_name),
         ejaccard = sprintf('%s <- model.cluster.closestN(%s,%s,%s,jaccard_dist_fun)', output_name, k, input_name, codebook_name),
         stop(sprintf('supplied distance measure not supported: %s', distance_measure))) 
}


#' @keywords internal
as_codebook <- function(y, x){
  lapply(seq.int(nrow(x)), FUN=function(i,y,x){
    list(target = y[i],
         center = as.list(unname(x[i,,drop=F])))
  }, y = y, x = x)
}


#' @keywords internal
validate_codebook <- function(codebook, distance_measure = c('euclidean', 'manhattan', 'angle', 'jaccard', 'ejaccard')){
  
  which_distance_measure <- match.arg(distance_measure)
  if(which_distance_measure %in% c('euclidean', 'manhattan', 'angle')){
    res <- sapply(codebook, 
                  FUN=function(x){
                    all(is.numeric(unlist(x$center)))
                  })
    if(!all(res)){
      stop('Models using distance measure euclidean, manhattan, or angle must have their codebook entries coded as all numeric.')  
    }
  } else if(which_distance_measure %in% c('jaccard', 'ejaccard')){
    res <- sapply(codebook, 
                  FUN=function(x){
                    (all(x %in% c(TRUE, FALSE)) | all(x %in% c(0,1)))
                  })
    if(!all(res)){
      stop('Models using distance measure jaccard or ejaccard must have their codebook entries coded as all binary TRUE/FALSE or 0/1')  
    }
  } else {
    stop('codebook cannot be validated for knn models with distance measure %s', which_distance_measure)
  }
  
  return(invisible(TRUE))
}