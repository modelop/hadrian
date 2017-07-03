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


#' extract_params.kcca
#'
#' Extract K-centroids model parameters from a kcca object created by 
#' the flexclust library
#' 
#' @param object an object of class "kcca"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- flexclust::kcca(iris[,1:4], k = 3, family=flexclust::kccaFamily("kmeans"))
#' extracted_params <- extract_params(model)
#' @export
extract_params.kcca <- function(object, ...) {
  
  if(class(object)[1] %in% c('kcca', 'kccasimple')){
    
    this_centroids <- object@centers
    this_input_vars <- gsub('\\.', '_', colnames(this_centroids))
    colnames(this_centroids) <- this_input_vars
    
    this_family <- object@family@name
    this_k <- object@k
    
  } else if(class(object)[1] == 'kmeans'){
    
    this_centroids <- object$centers
    this_input_vars <- gsub('\\.', '_', colnames(this_centroids))
    colnames(this_centroids) <- this_input_vars
    this_family <- 'kmeans'
    this_k <- as.integer(nrow(object$centers))
    
  } else {
    stop(sprintf("Currently not supporting cluster models with class %s", class(object)[1]))
  }
  
  list(inputs = this_input_vars,
       centroids = this_centroids,
       family = this_family,
       k = this_k)
}

#' PFA Formatting of Fitted K-Centroid Models
#'
#' This function takes a K-centroids model fit using kcca  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "kcca"
#' @param cluster_names a character vector of length k to name the values relating
#' to each cluster instead of just an integer. If not specified, then the predicted 
#' cluster will be the string representation of the cluster index.
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
#' @seealso \code{\link[flexclust]{kcca}} \code{\link{extract_params.kcca}}
#' @examples
#' model <- flexclust::kcca(iris[,1:4], k = 3, family=flexclust::kccaFamily("kmeans"))
#' model_as_pfa <- pfa(model)
#' @export
pfa.kcca <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                     cluster_names=NULL, ...){
  
  # extract model parameters
  extracted_params <- extract_params(object)
  
  if(!(extracted_params$family %in% c('kmeans', 'kmedians', 'angle', 'jaccard'))){
    stop(sprintf("Currently not supporting cluster models with distance metric %s", extracted_params$family))
  } 

  if(extracted_params$family %in% c('kmeans', 'kmedians', 'angle')){
    input_avro_type <- avro_double
  } else {
    input_avro_type <- avro_boolean
  }
  
  # define the input schema
  field_names <- extracted_params$inputs
  field_types <- rep(input_avro_type, length(field_names))
  names(field_types) <- field_names
  input_type <- avro_record(field_types, "Input")
  
  # create list defining the first action of constructing input
  kcca_input_list <- list(type = avro_array(input_avro_type),
                         new = lapply(field_names, function(n) {
                           paste("input.", n, sep = "")
                           }))
  cast_input_string <- 'kcca_input <- kcca_input_list'
  
  if(is.null(cluster_names)){
    cluster_names <- as.character(seq.int(extracted_params$k))
  } else {
    if(length(cluster_names) != extracted_params$k){
      stop(sprintf(paste('Length of provided cluster names (%s) does not match', 
                          'the number of centroids (%s). Please specify a list of names for each centroid.'), 
                   length(cluster_names), extracted_params$k))
    }
  }
  
  centroids_init <- list()
  if(extracted_params$family %in% c('kmeans', 'kmedians')){
    input_type <- avro_map(avro_double)
    input_name <- 'kcca_input'
    centroids_type <- avro_array(avro_record(fields = list(id = avro_string, center = avro_array(avro_double))))
    for(i in 1:nrow(extracted_params$centroids)){
     centroids_init[[length(centroids_init) + 1]] <- list(id = cluster_names[i], 
                                                          center = as.list(unname(extracted_params$centroids[i,,drop=F])))
    }
  } else if(extracted_params$family %in% c('jaccard')){
    input_type <- avro_map(avro_boolean)
    input_name <- 'kcca_input'
    centroids_type <- avro_array(avro_record(fields = list(id = avro_string, center = avro_array(avro_boolean)))) 
    for(i in 1:nrow(extracted_params$centroids)){
     if(!(all(extracted_params$centroids[i,] %in% c(TRUE, FALSE)) | 
          all(extracted_params$centroids[i,] %in% c(0,1)))){
       stop('Models of family jaccard or ejaccard must have their inputs coded as binary TRUE/FALSE or 0/1')
     }
     centroids_init[[length(centroids_init) + 1]] <- list(id = cluster_names[i], 
                                                          center = as.list(as.logical(unname(extracted_params$centroids[i,,drop=F]))))
    }
  } else if(extracted_params$family %in% c('angle')){
    input_type <- avro_map(avro_double)
    input_name <- 'kcca_input2'
    special_angle_string <- 'kcca_input2 <- new(avro_array(avro_array(avro_double)), kcca_input)'
    cast_input_string <- paste(cast_input_string, special_angle_string, sep='\n')
    centroids_type <- avro_array(avro_record(fields = list(id = avro_string, center = avro_array(avro_array(avro_double)))))
    for(i in 1:nrow(extracted_params$centroids)){
     centroids_init[[length(centroids_init) + 1]] <- list(id = cluster_names[i], 
                                                          center = list(as.list(unname(extracted_params$centroids[i,,drop=F]))))
    }
  } else {
    stop('Centroids cannot be created for kcca models of family type %s', extracted_params$family)
  }

  # determine the output based on pred_type
  this_cells <- list(centroids = pfa_cell(type = centroids_type,
                                          init = centroids_init))
  
  this_action <- parse(text=paste(cast_input_string,
                                  kcca_func_mapper(family = extracted_params$family, 
                                                   input_name = input_name, 
                                                   centroids_name = 'centroids'),
                                  sep='\n'))
  
  this_fcns <- NULL
  # construct the pfa_document
  doc <- pfa_document(input = input_type,
                      output = avro_string,
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


#' extract_params.kccasimple
#'
#' Extract K-centroids model parameters from a kccasimple object created by 
#' the flexclust library
#' 
#' @param object an object of class "kccasimple"
#' @param ... further arguments passed to or from other methods
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @examples
#' model <- flexclust::kcca(iris[,1:4], k = 3, 
#'                          family=flexclust::kccaFamily("kmeans"), simple=TRUE)
#' extracted_params <- extract_params(model)
#' @export
extract_params.kccasimple <- extract_params.kcca


#' PFA Formatting of Fitted K-Centroid Models
#'
#' This function takes a K-centroids model fit using kccasimple  
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "kccasimple"
#' @param cluster_names a character vector of length k to name the values relating
#' to each cluster instead of just an integer. If not specified, then the predicted 
#' cluster will be the string representation of the cluster index.
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
#' @seealso \code{\link[flexclust]{kcca}} \code{\link{extract_params.kccasimple}}
#' @examples
#' model <- flexclust::kcca(iris[,1:4], k = 3, 
#'                          family=flexclust::kccaFamily("kmeans"), simple=TRUE)
#' model_as_pfa <- pfa(model)
#' @export
pfa.kccasimple <- pfa.kcca


#' @keywords internal
kcca_func_mapper <- function(family, input_name, centroids_name) {
  switch(family,
         kmeans = sprintf('model.cluster.closest(%s,%s)["id"]', input_name, centroids_name),
         kmedians = sprintf('model.cluster.closest(%s,%s,manhattan_dist_fun)["id"]', input_name, centroids_name),
         angle = sprintf('model.cluster.closest(%s,%s,angle_dist_fun)["id"]', input_name, centroids_name), 
         jaccard = sprintf('model.cluster.closest(%s,%s,jaccard_dist_fun)["id"]', input_name, centroids_name),
         ejaccard = sprintf('model.cluster.closest(%s,%s,jaccard_dist_fun)["id"]', input_name, centroids_name),
         stop(sprintf('supplied link function not supported: %s', family))) 
}