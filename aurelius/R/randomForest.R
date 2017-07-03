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


#' extract_params.randomForest
#'
#' Extracts parameters from a forest made by the randomForest function
#' 
#' @param object an object of class "randomForest"
#' @param which_tree the number of the tree to extract
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} that is extracted from the randomForest object
#' @examples 
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100))
#' dat$Y <- factor((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' model <- randomForest::randomForest(Y ~ X1 + X2, data=dat, ntree=10)
#' my_tree <- extract_params(model, 1)
#' @export
extract_params.randomForest <- function(object, which_tree = 1, ...) {

  if (is.null(object$forest)) {
      stop("No forest component in ", deparse(substitute(object)))
  }
  
  if (which_tree > object$ntree) {
      stop("There are fewer than ", which_tree, "trees in the forest")
  }
  
  if (object$type == "regression") {
      tree <- cbind(object$forest$leftDaughter[, which_tree],
                    object$forest$rightDaughter[, which_tree],
                    object$forest$bestvar[, which_tree],
                    object$forest$xbestsplit[, which_tree],
                    object$forest$nodestatus[, which_tree],
                    object$forest$nodepred[, which_tree])[1:object$forest$ndbigtree[which_tree], ]
  }
  else {
      tree <- cbind(object$forest$treemap[, , which_tree],
                    object$forest$bestvar[, which_tree],
                    object$forest$xbestsplit[, which_tree],
                    object$forest$nodestatus[, which_tree],
                    object$forest$nodepred[, which_tree])[1:object$forest$ndbigtree[which_tree],]
  }

  dimnames(tree) <- list(1:nrow(tree), c("left daughter",
                                         "right daughter",
                                         "split var",
                                         "split point",
                                         "status",
                                         "prediction"))
  
  tree <- as.data.frame(tree)
  v <- tree[[3]]
  v[v == 0] <- NA
  tree[[3]] <- rownames(object$importance)[v]
  if (object$type == "classification") {
      v <- tree[[6]]
      v[!v %in% 1:nlevels(object$y)] <- NA
      tree[[6]] <- levels(object$y)[v]
  }
  
  return(tree)
}


#' build_model.randomForest
#'
#' Builds an entire PFA list of lists based on a single randomForest model tree
#' 
#' @param object a object of class randomForest
#' @param which_tree an integer indicating which single tree to build
#' @param ... further arguments passed to or from other methods
#' @return a \code{list} of lists representation of the tree that can be 
#' inserted into a cell or pool
#' @examples 
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100))
#' dat$Y <- factor((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' model <- randomForest::randomForest(Y ~ X1 + X2, data=dat, ntree=10)
#' my_tree <- build_model(model, 1)
#' @export
build_model.randomForest <- function(object, which_tree = 1, ...){
  
  # pull out the tree from the object
  tree_table <- extract_params(object = object, which_tree = which_tree)
  
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
  if(any(!sapply(object$forest$xlevels, is.numeric))) {
    dataLevels <- list()
    cat_var_idx <- which(!sapply(object$forest$xlevels, is.numeric))
    for(i in cat_var_idx){
      dataLevels[[length(dataLevels) + 1]] <- object$forest$xlevels[[i]]
    }
    names(dataLevels) <- names(object$forest$xlevels)[cat_var_idx]
  } else {
    dataLevels <- NULL
  }

  # infer field types
  fieldTypes <- list()
  for(i in 1:length(object$forest$xlevels)){
    fieldTypes[[length(fieldTypes) + 1]] <- if(is.numeric(object$forest$xlevels[[i]])) avro_double else avro_string
  }
  names(fieldTypes) <- names(object$forest$xlevels)
  
  build_node_randomForest(tree_table = tree_table, 
                          leaf_val_type = if(object$type == 'classification') avro_string else avro_double,
                          whichNode = 1,
                          valueNeedsTag = valueNeedsTag, 
                          dataLevels = dataLevels, 
                          fieldTypes = fieldTypes)
}


#' build_node_randomForest
#'
#' Builds one node of a randomForest model tree
#' 
#' @param tree_table tree object
#' @param leaf_val_type a character representing an avro type when a value is 
#' returned at a leaf node
#' @param whichNode  pointer to the target node to be built
#' @param valueNeedsTag flag for whether node needs label
#' @param dataLevels levels of data
#' @param fieldTypes type of fields
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @keywords internal
build_node_randomForest <- function(tree_table, leaf_val_type, whichNode, valueNeedsTag, dataLevels, fieldTypes = NULL) {
  
    node <- tree_table[whichNode,]

    if (node[[1]] > 0) {  # branch node
        f <- gsub("\\.", "_", node[[3]])
        dl <- dataLevels[[f]]

        if (is.null(fieldTypes)  ||  is.null(fieldTypes[[f]]))
            t <- avro_type(node[[4]])
        else {
            t <- fieldTypes[[f]]
            if (is.list(t)  &&  "name" %in% names(t))
                t <- t$name
        }

        if (!is.null(dl)  &&  length(dl) == 2  &&  (node[[4]] == 1  ||  node[[4]] == 2  ||  node[[4]] == 0.5)) {
            if (dl[[node[[4]]]] == 0.5)
                val <- FALSE
            else
                val <- dl[[node[[4]]]]
            if (valueNeedsTag) {
                out <- list()
                out[[t]] <- val
                val <- out
            }
            op <- "=="
        }
        else if (!is.null(dl)  &&  length(dl) > 0) {
            l <- length(dl)
            a <- 2^(0:l)
            b <- 2*a
            val <- list()
            for (i in 1:l) {
                if ((node[[4]] %% b[i]) >= a[i])
                    val[length(val) + 1] <- dl[[i]]
            }
            if (valueNeedsTag) {
                out <- list(array = val)
                val <- out
            }
            op <- "in"
        }
        else {
            val <- node[[4]]
            if (valueNeedsTag) {
                out <- list()
                out[[t]] <- val
                val <- out
            }
            op <- "<="
        }

        list(TreeNode =
             list(field = f,
                  operator = op,
                  value = val,
                  pass = build_node_randomForest(tree_table, leaf_val_type, node[[1]], valueNeedsTag, dataLevels, fieldTypes),
                  fail = build_node_randomForest(tree_table, leaf_val_type, node[[2]], valueNeedsTag, dataLevels, fieldTypes)))
    } else {
      node <- list(node[[6]])   # leaf node
      names(node) <- leaf_val_type
      node
    }
}


#' PFA Formatting of Fitted Random Forest Models
#'
#' This function takes a random forest model fit using randomForest() and 
#' returns a list-of-lists representing in valid PFA document that could 
#' be used for scoring
#' 
#' @source pfa_config.R avro_typemap.R avro_R pfa_cellpool.R pfa_expr.R pfa_utils.R
#' @param object an object of class "randomForest"
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
#' @seealso \code{\link[randomForest]{randomForest}}
#' @examples
#' dat <- data.frame(X1 = runif(100), 
#'                   X2 = rnorm(100))
#' dat$Y <- factor((rexp(100,5) + 5 * dat$X1 - 4 * dat$X2) > 0)
#' 
#' model <- randomForest::randomForest(Y ~ X1 + X2, data=dat, ntree=10)
#' model_as_pfa <- pfa(model)
#' @export
pfa.randomForest <- function(object, name=NULL, version=NULL, doc=NULL, metadata=NULL, randseed=NULL, options=NULL, 
                             pred_type=c('response', 'prob'),
                             cutoffs=NULL,
                             n.trees=NULL, ...){

  if(object$type %in% c('unsupervised')){
    stop(sprintf("Currently not supporting randomForest models of type %s", object$type))
  }
  
  if(is.null(n.trees)){
    tree_idx <- 1:object$ntree
  } else if (length(n.trees) == 1) {
    tree_idx <- 1:n.trees
  }
  
  if(any(n.trees > object$ntree)){
    n.trees <- object$ntree
    warning(sprintf("Number of trees exceeded number fit so far. Using first %s trees.", n.trees))
  }

  forest <- list()
  for (i in tree_idx){
    forest[[length(forest) + 1]] <- build_model(object, which_tree = i)$TreeNode
  }
  
  # define the input schema
  fieldNames <- as.list(names(object$forest$xlevels))
  fieldTypes <- list()
  all_field_types <- c()
  for(i in seq_along(names(object$forest$xlevels))){
    this_field_type <- if(is.numeric(object$forest$xlevels[[i]])) avro_double else avro_string
    all_field_types <- unique(c(all_field_types, this_field_type))
    fieldTypes[[length(fieldTypes) + 1]] <- this_field_type
  }
  names(fieldTypes) <- names(object$forest$xlevels)
  input_type <- avro_record(fieldTypes, "Input")
  
  valueFieldTypes <- list()
  for(a in all_field_types){
    valueFieldTypes[[length(valueFieldTypes) + 1]] <- a
    if(a == avro_string){
      valueFieldTypes[[length(valueFieldTypes) + 1]] <- avro_array(avro_string)
    }
  }
  
  tree_scores_action_string <- 'tree_scores <- a.map(forest, function(tree = tm("TreeNode") -> tm("TargetType"))
                                  model.tree.simpleTree(input, tree))'
  
  which_pred_type <- match.arg(pred_type)
  cutoffs <- validate_cutoffs(cutoffs = cutoffs, classes = object$classes) 
  this_cells <- list()
  if(object$type == 'classification'){
    target_type <- avro_string
    random_forest_prob_aggregator <- paste0('probs <- new(avro_map(avro_double), ',
                                           paste0(sapply(object$classes, FUN=function(x){
                                             sprintf('`%s` = a.count(tree_scores, "%s") / a.len(tree_scores)', x, x)
                                             }, USE.NAMES = FALSE), 
                                           collapse=', '), ')')
    if(which_pred_type == 'response'){
      ouput_type <- avro_string
      this_action <- parse(text=paste(tree_scores_action_string, 
                                      random_forest_prob_aggregator,
                                      'map.argmax(u.cutoff_ratio_cmp(probs, cutoffs))',
                                      sep="\n "))
      this_fcns <- c(divide_fcn, cutoff_ratio_cmp_fcn)
      this_cells[['cutoffs']] <- pfa_cell(type = avro_map(avro_double), init = cutoffs)
    } else if(which_pred_type == 'prob'){
      ouput_type <- avro_map(avro_double)
      this_action <- parse(text=paste(tree_scores_action_string, 
                                random_forest_prob_aggregator,
                                'probs',
                                sep="\n "))
      this_fcns <- NULL
    } else {
      stop('Only "response" and "prob" values are accepted for pred_type')
    }
    
  } else {
    target_type <- avro_double
    ouput_type <- avro_double
    this_action <- parse(text=paste(tree_scores_action_string, 
                                    'a.mean(tree_scores)',
                                    sep="\n "))
    this_fcns <- NULL
  }
  
  tm <- avro_typemap(
    Input = input_type,
    Output = ouput_type,
    TargetType = target_type,
    TreeNode = avro_record(
        list(
          field    = avro_enum(fieldNames),
          operator = avro_string,
          value    = valueFieldTypes,
          pass     = avro_union("TreeNode", target_type),
          fail     = avro_union("TreeNode", target_type)
        ), "TreeNode"
      )
    )
  
  this_cells[['forest']] <- pfa_cell(avro_array(tm("TreeNode")), forest)
  
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