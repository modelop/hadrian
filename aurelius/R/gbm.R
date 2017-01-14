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

#' pfa.gbm.extractTree
#'
#' Extracts a tree from a forest made by the gbm library.
#' @param gbm an object of class "gbm"
#' @param whichTree  the number of the tree to extract
#' @return tree that is extracted from gbm object
#' @export pfa.gbm.extractTree
#' @examples
#' X1 <- runif(100)
#' X2 <- rnorm(100)
#' Y <- rexp(100,5) + 5 * X1 - 4 * X2 
#' Y <- Y > 0
#' zz <- gbm(Y ~ X1 + X2)
#' zz$problemType <- "Classification"
#' zz1 <- pfa.gbm.extractTree(zz)

pfa.gbm.extractTree <- function(gbm, whichTree = 1) {
    if (!("gbm" %in% class(gbm)))
        stop("pfa.gbm.extractTree requires an object of class \"gbm\"")
  
    if (is.null(gbm$trees))
        stop("No trees in ", deparse(substitute(gbm)))

    if ((whichTree < 1) || (whichTree > gbm$n.trees))
        stop("The requested tree, ", whichTree, "is not contained within ", deparse(substitute(gbm)))
  
    if (gbm$problemType == "Classification") {
        tree <- data.frame(gbm$trees[[whichTree]])
        names(tree) <- c("SplitVar", "SplitCodePred", "LeftNode", "RightNode",
                         "MissingNode", "ErrorReduction", "Weight", "Prediction")
    }
    else
        stop("gmb problemType other than \"Classification\" not implemented yet")  

    # node positions were 0-indexed for traversal in C, make them 1-indexed for traversal in R
    tree$LeftNode <- tree$LeftNode + 1
    tree$RightNode <- tree$RightNode + 1
    tree$MissingNode <- tree$MissingNode + 1

    # field positions were 0-indexed for lookup in C, make them 1-indexed for lookup in R
    v <- as.vector(unlist(tree["SplitVar"]))
    v[v == -1] <- NA   # to identify leaves
    v = v + 1
    tree$SplitVar <- gbm$var.names[v]

    # pull out categorical (factor) variable lookup table, tree and table
    list(treeTable=tree, categoricalLookup=gbm$c.splits)
}

#' pfa.gbm.buildOneTree
#'
#' Builds one tree extracted by pfa.gbm.extractTree.
#' @param tree tree object
#' @param categoricalLookup splits used
#' @param whichNode  left or right node for categoricalLookup
#' @param valueNeedsTag flag for whether node needs label
#' @param dataLevels levels of data
#' @param fieldTypes type of fields
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @export pfa.gbm.buildOneTree
#' @examples
#' X1 <- runif(100)
#' X2 <- rnorm(100)
#' Y <- rexp(100,5) + 5 * X1 - 4 * X2
#' Y <- Y > 0
#' zz <- gbm(Y ~ X1 + X2)
#' zz$problemType <- "Classification"
#' zz1 <- pfa.gbm.extractTree(zz)
#' zz2 <- pfa.gbm.buildOneTree(zz1$treeTable, list(), 1)




pfa.gbm.buildOneTree <- function(tree, categoricalLookup, whichNode, valueNeedsTag = TRUE, dataLevels = NULL, fieldTypes = NULL) {
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
            categories <- categoricalLookup[[node$SplitCodePred + 1]]
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
                  pass     = pfa.gbm.buildOneTree(tree, categoricalLookup, node$LeftNode,    valueNeedsTag, dataLevels, fieldTypes),
                  fail     = pfa.gbm.buildOneTree(tree, categoricalLookup, node$RightNode,   valueNeedsTag, dataLevels, fieldTypes),
                  missing  = pfa.gbm.buildOneTree(tree, categoricalLookup, node$MissingNode, valueNeedsTag, dataLevels, fieldTypes)))
    }
    else
        list(double = node$Prediction)    
}
