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


#' extract_params.rpart
#'
#' Extracts parameters from a tree made by the rpart() function
#' 
#' @importFrom utils head
#' @param object an object of class "rpart"
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} that is extracted from the rpart object
#' @examples 
#' model <- rpart::rpart(Kyphosis ~ Age + as.factor(Number), data = rpart::kyphosis)
#' my_tree <- extract_params(model)
#' @export
extract_params.rpart <- function(object, ...) {
  
  frame <- object$frame
  categorical_vars <- names(attr(object, "xlevels"))
  ylevels <- attr(object, 'ylevels')
  splits <- data.frame(object$splits, row.names = NULL)
  splits$SplitVar <- row.names(object$splits)
  nc <- frame[, c("ncompete", "nsurrogate")]
  which_primary <- which((frame$var != "<leaf>") + nc[[1L]] + nc[[2L]] > 0)
  primary_compete <- nc[[1L]]
  primary_surrogate <- nc[[2L]]
  splits_fnd <- data.frame(node_idx=character(0), 
                           node_type=character(0),
                           SplitVar=character(0),
                           SplitCategorical=logical(0),
                           SplitPoint=numeric(0),
                           SplitNcat=numeric(0),
                           LeftNode=numeric(0), 
                           RightNode=numeric(0), 
                           MissingNode=numeric(0), 
                           prediction_idx=numeric(0))
  
  id <- as.integer(row.names(frame))  
  ss <- 0
  primary_ss <- 0
  default_node <- 0
  for(i in which_primary){
    
    ss <- ss + 1
    
    cp <- 0.01
    if (frame$complexity[i] > cp) {
      sons <- 2L * id[i] + c(0L, 1L)
      sons.n <- frame$n[match(sons, id)]
      left_node <- sons[1]
      right_node <- sons[2]
      left_node_n <- sons.n[1]
      right_node_n <- sons.n[2]
      default_node <- if(left_node_n >= right_node_n) left_node else right_node
    }
    
    surr <- primary_surrogate[i] # get the surrogates count
    if(surr > 0){
      missing_node <- paste0(i, letters[1])
    } else {
      missing_node <- default_node
    }
    
    d <- data.frame(node_idx=as.character(i), 
                    node_type='primary',
                    SplitVar=splits[ss,'SplitVar'],
                    SplitCategorical=(splits[ss,'SplitVar'] %in% categorical_vars),
                    SplitPoint=splits[ss,'index'],
                    SplitNcat=splits[ss,'ncat'],
                    LeftNode=left_node, 
                    RightNode=right_node, 
                    MissingNode=missing_node, 
                    prediction_idx=NA, 
                    stringsAsFactors = FALSE)
    splits_fnd <- rbind(splits_fnd, d)
    
    # loop over surrogates if they exist
    if(surr > 0){
      for(j in seq.int(surr)){
        
        if(j == surr){
          missing_node <- default_node
        } else {
          missing_node <- paste0(i, letters[j+1])
        }
        
        d <- data.frame(node_idx=paste0(i, letters[j]),
                        node_type='surrogate', 
                        SplitVar=splits[ss + primary_compete[i] + j,'SplitVar'],
                        SplitCategorical=(splits[ss + primary_compete[i] + j,'SplitVar'] %in% categorical_vars),
                        SplitPoint=splits[ss + primary_compete[i] + j,'index'],
                        SplitNcat=splits[ss,'ncat'],
                        LeftNode=left_node, 
                        RightNode=right_node, 
                        MissingNode=missing_node, 
                        prediction_idx=NA, 
                        stringsAsFactors = FALSE)
        splits_fnd <- rbind(splits_fnd, d)
      }
      ss <- ss + primary_compete[i] + surr
    }
  }
  
  # add back the leaves
  leaves <- frame[frame$var == '<leaf>',]
  leaves$node_idx <- row.names(leaves)
  leaves$node_type <- 'leaf'
  leaves$SplitVar <- NA
  leaves$SplitCategorical <- NA
  leaves$SplitPoint <- NA
  leaves$SplitNcat <- NA
  leaves$LeftNode <- 0
  leaves$RightNode <- 0
  leaves$MissingNode <- 0
  leaves$prediction_idx <- row.names(leaves)
  leaves <- leaves[,names(splits_fnd)]
  
  splits_fnd$SplitVar <- validate_names(splits_fnd$SplitVar)
  tree <- rbind(splits_fnd, leaves)
  rownames(tree) <- NULL
  
  this_ylevels <- attr(object, 'ylevels')
  this_xlevels <- attr(object, 'xlevels')
  names(this_xlevels) <- validate_names(names(this_xlevels))
  this_input_vars <- validate_names(attr(object$ordered, 'names'))

  # pull out categorical (factor) variable lookup table, tree table
  list(tree_table = tree, 
       categorical_lookup = object$csplit,
       xlevels = this_xlevels,
       ylevels = this_ylevels, 
       input_vars = this_input_vars)
}


#' build_model.rpart
#'
#' Builds an entire PFA list of lists based on a single tree
#' 
#' @param object a object of class "rpart"
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists representation of the tree that can be 
#' inserted into a cell or pool
#' @examples 
#' model <- rpart::rpart(Kyphosis ~ Age + as.factor(Number), data = rpart::kyphosis)
#' my_tree <- build_model(model)
#' @export
build_model.rpart <- function(object, ...){

  extracted_tree <- extract_params(object)
  tree <- extracted_tree$tree_table
  categorical_lookup <- extracted_tree$categorical_lookup
  valueNeedsTag <- TRUE
  
  # check if we have some categorical variables
  if(any(tree$SplitCategorical[!is.na(tree$SplitCategorical)])) {
    dataLevels <- extracted_tree$xlevels
  } else {
    dataLevels <- NULL
  }

  # infer field types
  fieldTypes <- list()
  input_vars <- extracted_tree$input_vars
  for(i in input_vars){
    fieldTypes[[length(fieldTypes) + 1]] <- if(i %in% names(extracted_tree$xlevels)) avro_string else avro_double
  }
  names(fieldTypes) <- input_vars
  
  build_node_rpart(tree = tree, 
                   categorical_lookup = categorical_lookup,
                   leaf_val_type = avro_string,
                   whichNode = '1',
                   valueNeedsTag = valueNeedsTag, 
                   dataLevels = dataLevels, 
                   fieldTypes = fieldTypes)
}


#' build_node_rpart
#'
#' Builds one node of a rpart model tree
#' 
#' @param tree tree object
#' @param categorical_lookup splits used
#' @param leaf_val_type a character representing an avro type when a value is 
#' returned at a leaf node
#' @param whichNode  left or right node for categorical_lookup
#' @param valueNeedsTag flag for whether node needs label
#' @param dataLevels levels of data
#' @param fieldTypes type of fields
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @keywords internal
build_node_rpart <- function(tree, categorical_lookup, leaf_val_type, whichNode, valueNeedsTag, dataLevels, fieldTypes){
  
  node <- tree[tree$node_idx == whichNode,]

  if (!is.na(node$SplitVar)) {

      field <- node$SplitVar    
      split_val <- node$SplitPoint
      dl <- dataLevels[[field]]        
      
      # "t" is the avro type of the split point
      if (is.null(fieldTypes) | is.null(fieldTypes[[field]])){
        t <- avro_type(split_val)
      } else {
        t <- fieldTypes[[field]]
        if (is.list(t) && "name" %in% names(t)){
            t <- t$name
        }
      }
 
      if (!is.null(dl)) {
        # look up category options in table
        categories <- categorical_lookup[split_val,]
        split_val <- dl[categories == 1] # level names which get sent left
        right <- dl[categories ==  3] # level names which get sent right

        if (length(split_val) == 1) {  # only one factor to check to send right
            op <- "=="
            if (valueNeedsTag) {
                out <- list()
                out[[t]] <- as.list(split_val)[[1]]
                split_val <- out
            }
          } else if (length(split_val) > 1) { # if input is one of several factors send right
            op <- "in" 
            if (valueNeedsTag) {
                out <- list(array = as.list(split_val))
                split_val <- out
            }
          } else {
            stop("can't understand this factor or categorical variable") 
          }
      } else {
        # split_val is numeric
        if (valueNeedsTag) {
            out <- list()
            out[[t]] <- split_val
            split_val <- out
        }
        op <- if(node$SplitNcat == 1) ">=" else "<"
      }  

      list(TreeNode = 
           list(field    = field,
                operator = op,
                value    = split_val,
                pass     = build_node_rpart(tree, categorical_lookup, leaf_val_type, node$LeftNode, valueNeedsTag, dataLevels, fieldTypes),
                fail     = build_node_rpart(tree, categorical_lookup, leaf_val_type, node$RightNode, valueNeedsTag, dataLevels, fieldTypes),
                missing  = build_node_rpart(tree, categorical_lookup, leaf_val_type, node$MissingNode, valueNeedsTag, dataLevels, fieldTypes)))
  } else {
    node <- list(node$prediction_idx) # leaf node
    names(node) <- leaf_val_type
    node
  }
}


#' PFA Formatting of Fitted rpart Tree Models
#'
#' This function takes an tree model fit using the rpart package
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "rpart"
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
#' @seealso \code{\link[rpart]{rpart}}
#' @examples
#' model <- rpart::rpart(Species ~ ., data=iris)
#' model_as_pfa <- pfa(model)
#' @export
pfa.rpart <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                      pred_type = c('response', 'prob'), 
                      cutoffs = NULL, ...){

  extracted_tree <- extract_params(object)
  tree_node <- build_model(object)$TreeNode  
  
  # define the input schema
  fieldNames <- as.list(extracted_tree$input_vars)
  fieldTypes <- list()
  all_field_types <- c()
  for(i in seq_along(extracted_tree$input_vars)){
    this_field_type <- if(extracted_tree$input_vars[i] %in% names(extracted_tree$xlevels)) avro_string else avro_double
    all_field_types <- unique(c(all_field_types, this_field_type))
    fieldTypes[[length(fieldTypes) + 1]] <- avro_union(avro_null, this_field_type)
  }
  names(fieldTypes) <- extracted_tree$input_vars
  input_type <- avro_record(fieldTypes, "Input")
  
  # setup for the possibility of an "in" condition at the split that needs an array
  valueFieldTypes <- list()
  for(a in all_field_types){
    valueFieldTypes[[length(valueFieldTypes) + 1]] <- a
    if(a == avro_string){
      valueFieldTypes[[length(valueFieldTypes) + 1]] <- avro_array(avro_string)
    }
  }
  
  tm_tree_node <- avro_typemap(
    TreeNode = avro_record(
      list(
        field    = avro_enum(fieldNames),
        operator = avro_string,
        value    = valueFieldTypes,
        pass     = avro_union("TreeNode", avro_string),
        fail     = avro_union("TreeNode", avro_string),
        missing  = avro_union("TreeNode", avro_string)), "TreeNode"
      )
  )

  this_cells <- list()
  this_cells[['tree']] <- pfa_cell(tm_tree_node("TreeNode"), tree_node)
  which_pred_type <- match.arg(pred_type)
  
  if(object$method == 'class'){
   if(which_pred_type == 'response'){
      ouput_type <- avro_string
      leaves <- object$frame[object$frame$var == '<leaf>',,drop=FALSE]
      prob_cols <- (1+1*(length(extracted_tree$ylevels)))+seq.int(length(extracted_tree$ylevels))
      pred_vals <- as.data.frame(leaves$yval2[,prob_cols])
      colnames(pred_vals) <- extracted_tree$ylevels
      pred_vals <- apply(pred_vals,1,FUN=function(x){as.list(x)})
      names(pred_vals) <- row.names(leaves)
      this_cells[['prediction_lookup']] <- pfa_cell(type=avro_map(avro_map(avro_double)), 
                                                    init=pred_vals)
      pred_lookup_action <- paste('probs <- prediction_lookup[pred_idx]',
                                  'map.argmax(u.cutoff_ratio_cmp(probs, cutoffs))',
                                  sep="\n ")
      this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
      cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = extracted_tree$ylevels) 
      this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
    } else if(which_pred_type == 'prob'){
      ouput_type <- avro_map(avro_double)
      leaves <- object$frame[object$frame$var == '<leaf>',,drop=FALSE]
      prob_cols <- (1+1*(length(extracted_tree$ylevels)))+seq.int(length(extracted_tree$ylevels))
      pred_vals <- as.data.frame(leaves$yval2[,prob_cols])
      colnames(pred_vals) <- extracted_tree$ylevels
      pred_vals <- apply(pred_vals,1,FUN=function(x){as.list(x)})
      names(pred_vals) <- row.names(leaves)
      this_cells[['prediction_lookup']] <- pfa_cell(type=avro_map(avro_map(avro_double)), 
                                                    init=as.list(pred_vals))
      pred_lookup_action <- 'prediction_lookup[pred_idx]'
      this_fcns <- NULL
    } else {
      stop('Only "response" and "prob" values are accepted for pred_type')
    }
  } else {
    ouput_type <- avro_double
    leaves <- object$frame[object$frame$var == '<leaf>',]
    pred_vals <- leaves$yval
    names(pred_vals) <- row.names(leaves)
    this_cells[['prediction_lookup']] <- pfa_cell(type=avro_map(avro_double), 
                                                  init=as.list(pred_vals))
    pred_lookup_action <- 'prediction_lookup[pred_idx]'
    this_fcns <- NULL
    cutoff_logic <- character(0)
  }
  
  tree_action <- 'pred_idx <- model.tree.missingWalk(input, tree,
                                function(d = tm("Input"), t = tm("TreeNode") -> avro_union(avro_null, avro_boolean))
                                  model.tree.missingTest(d, t)
                              )'
  this_action <- parse(text=paste(tree_action,
                                  pred_lookup_action,
                                  sep='\n '))
  
  tm <- avro_typemap(
    Input = input_type,
    Output = ouput_type,
    TreeNode = tm_tree_node("TreeNode")
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