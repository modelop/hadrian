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

#' extract_params.gbm
#'
#' Extracts a parameters from an ensemble made by the gbm function
#' 
#' @param object an object of class "gbm"
#' @param which_tree the number of the tree to extract
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} that is extracted from the gbm object
#' @importFrom gbm pretty.gbm.tree
#' @examples 
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100)) 
#' dat$Y <- ((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' bernoulli_model <- gbm::gbm(Y ~ X1 + X2, 
#'                             data = dat, 
#'                             distribution = 'bernoulli')
#' my_tree <- extract_params(bernoulli_model, 1)
#' @export
extract_params.gbm <- function(object, which_tree = 1, ...) {

    if (is.null(object$trees))
        stop("No trees in ", deparse(substitute(object)))

    if ((which_tree < 1) || (which_tree > (object$n.trees * object$num.classes)))
        stop("The requested tree, ", which_tree, " is not contained within ", deparse(substitute(object)))

    tree <- pretty.gbm.tree(object, which_tree)

    # node positions were 0-indexed for traversal in C, make them 1-indexed for traversal in R
    tree$LeftNode <- tree$LeftNode + 1
    tree$RightNode <- tree$RightNode + 1
    tree$MissingNode <- tree$MissingNode + 1

    # field positions were 0-indexed for lookup in C, make them 1-indexed for lookup in R
    v <- as.vector(unlist(tree["SplitVar"]))
    v[v == -1] <- NA   # to identify leaves
    v = v + 1
    tree$SplitVar <- object$var.names[v]
    
    # for ordered factor variables they appear like numeric
    # but we still need to let downstream PFA know about these
    # levels so we need to make up a c.splits list
    for(i in 1:length(object$var.names)){
      if(object$var.type[i] == 0 & !all(is.numeric(object$var.levels[[i]]))){
        stop('No support for models with ordered categorical variables')
      }
    }

    # pull out categorical (factor) variable lookup table, tree table
    list(tree_table=tree, categorical_lookup=object$c.splits)
}


#' build_model.gbm
#'
#' Builds an entire PFA list of lists based on a single gbm model tree
#' 
#' @param object a object of class gbm
#' @param which_tree an integer indicating which single tree to build
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists representation of the tree that can be 
#' inserted into a cell or pool
#' @examples
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100))
#' dat$Y <- ((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' bernoulli_model <- gbm::gbm(Y ~ X1 + X2, 
#'                             data = dat, 
#'                             distribution = 'bernoulli')
#' my_tree <- build_model(bernoulli_model, 1)
#' @export
build_model.gbm <- function(object, which_tree = 1, ...){
  
  # pull out the tree from the object
  extracted_tree <- extract_params(object = object,
                                   which_tree = which_tree)
  tree <- extracted_tree$tree_table
  categorical_lookup <- extracted_tree$categorical_lookup

  # determine the levels and field types
  # if the comparison values are a union (mixed numerical and categorical regressors), 
  # then use valueNeedsTag = TRUE, if it is not (all numerical or all categorical), then FALSE
  
  # TODO: determine if this should always be TRUE
  # # assumes that extract_tree will stop any ordered factors since it's supported
  # if(all(object$var.type == 0)){
  #   # all numeric
  #   valueNeedsTag <- FALSE
  # } else if (all(object$var.type > 0)){
  #   # all categorical
  #   valueNeedsTag <- FALSE
  # } else{
  #   valueNeedsTag <- TRUE
  # }
  valueNeedsTag <- TRUE
  
  # check if we have some categorical variables
  if(any(object$var.type > 0)) {
    dataLevels <- list()
    cat_var_idx <- which(object$var.type > 0)
    for(i in cat_var_idx){
      dataLevels[[length(dataLevels) + 1]] <- object$var.levels[[i]]
    }
    names(dataLevels) <- object$var.names[cat_var_idx]
  } else {
    dataLevels <- NULL
  }

  # infer field types
  fieldTypes <- list()
  for(i in 1:length(object$var.names)){
    fieldTypes[[length(fieldTypes) + 1]] <- if(object$var.type[i] == 0) avro_double else avro_string
  }
  names(fieldTypes) <- object$var.names
  
  build_node_gbm(tree = tree, 
                 categorical_lookup = categorical_lookup,
                 leaf_val_type = avro_double,
                 whichNode = 1,
                 valueNeedsTag = valueNeedsTag, 
                 dataLevels = dataLevels, 
                 fieldTypes = fieldTypes)
}

#' build_node_gbm
#'
#' Builds one node of a gbm model tree
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
build_node_gbm <- function(tree, categorical_lookup, leaf_val_type, whichNode, valueNeedsTag, dataLevels, fieldTypes){
  
  node <- tree[whichNode,]

  if (!is.na(node$SplitVar)) {
    
      split.val <- node$SplitCodePred

      # replace "." in a variable name with an underscore "_"
      f <- gsub("\\.", "_", node$SplitVar)
      dl <- dataLevels[[f]]        

      # "t" is the avro type of the split point
      if (is.null(fieldTypes) || is.null(fieldTypes[[f]]))
          t <- avro_type(split.val)
      else {
          t <- fieldTypes[[f]]
          if (is.list(t) && "name" %in% names(t))
              t <- t$name
      }
 
      if (!is.null(dl)) {
          # look up category options in table
          categories <- categorical_lookup[[node$SplitCodePred + 1]]
          split.val  <- dl[categories == -1] # level names which get sent left
          right      <- dl[categories ==  1] # level names which get sent right

          if (length(split.val) == 1) {  # only one factor to check to send right
              op <- "=="
              if (valueNeedsTag) {
                  out <- list()
                  out[[t]] <- as.list(split.val)[[1]]
                  split.val <- out
              }
          }
          else if (length(split.val) > 1) { # if input is one of several factors send right
              op <- "in" 
              if (valueNeedsTag) {
                  out <- list(array = as.list(split.val))
                  split.val <- out
              }
          }
          else 
              stop("can't understand this factor or categorical variable") 
      } 
      else {
      # split.var is numeric
          if (valueNeedsTag) {
              out <- list()
              out[[t]] <- split.val
              split.val <- out
          }
          op <- "<"
      }  

      list(TreeNode = 
           list(field    = f,
                operator = op,
                value    = split.val,
                pass     = build_node_gbm(tree, categorical_lookup, leaf_val_type, node$LeftNode,    valueNeedsTag, dataLevels, fieldTypes),
                fail     = build_node_gbm(tree, categorical_lookup, leaf_val_type, node$RightNode,   valueNeedsTag, dataLevels, fieldTypes),
                missing  = build_node_gbm(tree, categorical_lookup, leaf_val_type, node$MissingNode, valueNeedsTag, dataLevels, fieldTypes)))
  }
  else {
    node <- list(node$Prediction)   # leaf node
    names(node) <- leaf_val_type
    node
  }
}


#' PFA Formatting of Fitted GBMs
#'
#' This function takes a gradient boosted machine (gbm) fit using gbm
#' and returns a list-of-lists representing in valid PFA document 
#' that could be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro.R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "gbm"
#' @param n.trees an integer or vector of integers specifying the number of trees 
#' to use in building the model. If a vector is provided, then only the indices of 
#' thos trees will be used. If a single integer is provided then all trees up until 
#' and including that index will be used.
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
#' @seealso \code{\link[gbm]{gbm}}
#' @examples
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100))
#' dat$Y <- ((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' bernoulli_model <- gbm::gbm(Y ~ X1 + X2, 
#'                             data = dat, 
#'                             distribution = 'bernoulli')
#' model_as_pfa <- pfa(bernoulli_model)
#' @export
pfa.gbm <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                    pred_type = c('response', 'prob'), 
                    cutoffs = NULL,
                    n.trees=NULL, ...){
  
  if(object$distribution$name %in% c('quantile', 'pairwise')){
    stop(sprintf("Currently not supporting gbm models of distribution %s", object$distribution$name))
  }
  
  if(is.null(n.trees)){
    tree_idx <- 1:object$n.trees
  } else if (length(n.trees) == 1) {
    tree_idx <- 1:n.trees
  }
  
  if(any(n.trees > object$n.trees)){
    n.trees <- object$n.trees
    warning(sprintf("Number of trees exceeded number fit so far. Using first %s trees.", n.trees))
  }

  ensemble_of_trees <- list()
  for (i in tree_idx){
    if(object$distribution$name == 'multinomial'){
      num_classes <- object$num.classes
      for(c in 1:num_classes){
        ensemble_of_trees[[paste0('class', c)]][[length(ensemble_of_trees[[paste0('class', c)]]) + 1]] <- build_model(object, which_tree = ((i-1) * num_classes) + c)$TreeNode
      }
    } else {
      ensemble_of_trees[[length(ensemble_of_trees) + 1]] <- build_model(object, which_tree = i)$TreeNode  
    }
  }
  
  # define the input schema
  fieldNames <- as.list(gsub('\\.', '_', object$var.names))
  fieldTypes <- list()
  all_field_types <- c()
  for(i in seq_along(object$var.names)){
    this_field_type <- if(object$var.type[i] == 0) avro_double else avro_string
    all_field_types <- unique(c(all_field_types, this_field_type))
    fieldTypes[[length(fieldTypes) + 1]] <- avro_union(avro_null, this_field_type)
  }
  names(fieldTypes) <- gsub('\\.', '_', object$var.names)
  input_type <- avro_record(fieldTypes, "Input")
  
  valueFieldTypes <- list()
  for(a in all_field_types){
    valueFieldTypes[[length(valueFieldTypes) + 1]] <- a
    if(a == avro_string){
      valueFieldTypes[[length(valueFieldTypes) + 1]] <- avro_array(avro_string)
    }
  }
  
  # currently gbm will only ever emit a type of avro_double
  target_type <- avro_double
  which_pred_type <- match.arg(pred_type)
  tm_tree_node <- avro_typemap(
    TreeNode = avro_record(
      list(
        field    = avro_enum(fieldNames),
        operator = avro_string,
        value    = valueFieldTypes,
        pass     = avro_union("TreeNode", avro_double),
        fail     = avro_union("TreeNode", avro_double),
        missing  = avro_union("TreeNode", avro_double)), "TreeNode"
      )
  )

  this_cells <- list()
  if(object$num.classes == 1) {
    if(object$distribution$name %in% c('bernoulli', 'huberized', 'adaboost')){
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
      this_action <- parse(text=paste(
        tree_score_action_string(output_name='tree_scores', ensemble_name = 'ensemble'),
        pred_link_action_string(object=object, output_name='pred', scores_name='tree_scores'),
        'probs <- new(avro_map(avro_double), `0` = 1 - pred, `1` = pred)',
        pred_type_expression,
        sep='\n '))
      this_cells[['ensemble']] <- pfa_cell(avro_array(tm_tree_node("TreeNode")), ensemble_of_trees)
    } else {
      output_type <- avro_double
      pred_type_expression <- 'pred'
      this_action <- parse(text=paste(
        tree_score_action_string(output_name='tree_scores', ensemble_name = 'ensemble'),
        pred_link_action_string(object=object, output_name='pred', scores_name='tree_scores'),
        pred_type_expression,
        sep='\n '))
      this_fcns <- NULL
      this_cells[['ensemble']] <- pfa_cell(avro_array(tm_tree_node("TreeNode")), ensemble_of_trees)
    }
  } else {
    # this is a multiclass problem
    # build out 1 ensemble for each class
    # calculate tree scores
    class_ensembles <- paste0(sapply(seq.int(object$classes),
                                     FUN=function(x){
                                        tree_score_action_string(output_name=paste0('tree_scores_class', x), 
                                                                 ensemble_name = paste0('ensemble_class', x))
                                    }, USE.NAMES = FALSE),
                                collapse='\n ')
    pred_link_actions <- paste0(sapply(seq.int(object$classes),
                                     FUN=function(x, object){
                                        pred_link_action_string(object=object, 
                                                                output_name=paste0('pred_class', x), 
                                                                scores_name=paste0('tree_scores_class', x))
                                    }, object=object, USE.NAMES = FALSE),
                                collapse='\n ')
    all_class_preds <- paste0('all_preds <- new(avro_map(avro_double), ',
                                           paste0(sapply(seq.int(object$classes), FUN=function(x, object){
                                             sprintf('`%s` = %s', object$classes[x], paste0('pred_class', x))
                                             }, object=object, USE.NAMES = FALSE), 
                                           collapse=', '), ')')    
    if(which_pred_type == 'response'){
      cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = object$classes)
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
    this_action <- parse(text=paste(class_ensembles, 
                                    pred_link_actions, 
                                    all_class_preds, 
                                    'all_preds <- la.scale(all_preds, 1/a.sum(map.fromset(all_preds)))',
                                    pred_type_expression, 
                                    sep='\n '))
    for(i in seq.int(object$classes)){
      this_cells[[paste0('ensemble_class', i)]] <- pfa_cell(avro_array(tm_tree_node("TreeNode")), 
                                                            ensemble_of_trees[[paste0('class', i)]])  
    }
  }  
  
  tm <- avro_typemap(
    Input = input_type,
    Output = output_type,
    TargetType = target_type,
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


#' @keywords internal
pred_link_action_string <- function(object, output_name, scores_name){
 return(paste0(output_name, 
               ' <- ', 
               gbm_link_func_mapper(distribution = object$distribution$name, 
                                    intercept_value = object$initF, 
                                    scores_name = scores_name)))
}


#' @keywords internal
tree_score_action_string <- function(output_name, ensemble_name){
  return(sprintf('%s <- a.map(%s,
                          function(tree = tm("TreeNode") -> tm("TargetType"))
                              model.tree.missingWalk(input, tree,
                                  function(d = tm("Input"), t = tm("TreeNode") -> avro_union(avro_null, avro_boolean))
                                      model.tree.missingTest(d, t)
                                  )
                          )', output_name, ensemble_name))
}


#' @keywords internal
gbm_link_func_mapper <- function(distribution, intercept_value, scores_name='tree_scores') {
  
  lin_pred <- sprintf('%s + a.sum(%s)', intercept_value, scores_name)

  switch(distribution,
         gaussian = lin_pred,
         laplace = lin_pred,
         tdist = lin_pred,
         huberized = lin_pred,
         coxph = lin_pred,
         multinomial = sprintf('m.exp(%s)', lin_pred),
         poisson = sprintf('m.exp(%s)', lin_pred),
         bernoulli = sprintf('m.link.logit(%s)', lin_pred),
         pairwise = sprintf('m.link.logit(%s)', lin_pred),
         adaboost = sprintf('1/(1 + m.exp(-2 * (%s)))', lin_pred),
         stop(sprintf('supplied link function not supported: %s', distribution))) 
}