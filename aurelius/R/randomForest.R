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

#' pfa.randomForest.extractTree
#'
#' Extracts a tree from a forest made by the randomForest library.
#' @param forest an object of class "randomForest"
#' @param whichTree which tree to extract
#' @param labelVar flag for whether var needs to be labeled
#' @return single tree extracted from random forest
#' @import randomForest
#' @export pfa.randomForest.extractTree
#' @examples
#' X1 <- runif(100)
#' X2 <- rnorm(100)
#' Y <- rexp(100,5) + 5 * X1 - 4 * X2
#' Y <- Y > 0
#' Y <- factor(Y)
#' zz <- randomForest(Y ~ X1 + X2)
#' zz1 <- pfa.randomForest.extractTree(zz)


pfa.randomForest.extractTree <- function(forest, whichTree = 1, labelVar = FALSE) {
    if (!("randomForest" %in% class(forest)))
        stop("pfa.randomForest.extractTree requires an object of class \"randomForest\"")

    # modification of getTree to remove factors from forest output   
    if (is.null(forest$forest)) {
        stop("No forest component in ", deparse(substitute(forest)))
    }
    if (whichTree > forest$ntree) {
        stop("There are fewer than ", whichTree, "trees in the forest")
    }
    if (forest$type == "regression") {
        tree <- cbind(forest$forest$leftDaughter[, whichTree],
                      forest$forest$rightDaughter[, whichTree],
                      forest$forest$bestvar[, whichTree],
                      forest$forest$xbestsplit[, whichTree],
                      forest$forest$nodestatus[, whichTree],
                      forest$forest$nodepred[, whichTree])[1:forest$forest$ndbigtree[whichTree], ]
    }
    else {
        tree <- cbind(forest$forest$treemap[, , whichTree],
                      forest$forest$bestvar[, whichTree],
                      forest$forest$xbestsplit[, whichTree],
                      forest$forest$nodestatus[, whichTree],
                      forest$forest$nodepred[, whichTree])[1:forest$forest$ndbigtree[whichTree],]
    }
    dimnames(tree) <- list(1:nrow(tree), c("left daughter",
                                           "right daughter",
                                           "split var",
                                           "split point",
                                           "status",
                                           "prediction"))
    if (labelVar) {
        tree <- as.data.frame(tree)
        v <- tree[[3]]
        v[v == 0] <- NA
        tree[[3]] <- rownames(forest$importance)[v]
        if (forest$type == "classification") {
            v <- tree[[6]]
            v[!v %in% 1:nlevels(forest$y)] <- NA
            tree[[6]] <- levels(forest$y)[v]
        }
    }
    tree
}

#' pfa.randomForest.buildOneTree
#'
#' Builds one tree extracted by pfa.randomForest.extractTree.
#' @param tree tree to build
#' @param whichNode the node to extract
#' @param valueNeedsTag flag for whether the node needs a label
#' @param dataLevels levels of data
#' @param fieldTypes type of fields
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @import randomForest
#' @export pfa.randomForest.buildOneTree
#' @examples
#' X1 <- runif(100)
#' X2 <- rnorm(100)
#' Y <- rexp(100,5) + 5 * X1 - 4 * X2
#' Y <- Y > 0
#' Y <- factor(Y)
#' zz <- randomForest(Y ~ X1 + X2)
#' zz1 <- pfa.randomForest.extractTree(zz)
#' zz2 <- pfa.randomForest.buildOneTree(zz1, 1, TRUE, dataLevels = list())


pfa.randomForest.buildOneTree <- function(tree, whichNode, valueNeedsTag, dataLevels, fieldTypes = NULL) {
    node <- tree[whichNode,]

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
                  pass = pfa.randomForest.buildOneTree(tree, node[[1]], valueNeedsTag, dataLevels, fieldTypes),
                  fail = pfa.randomForest.buildOneTree(tree, node[[2]], valueNeedsTag, dataLevels, fieldTypes)))
    }
    else
        list(string = node[[6]])   # leaf node
}

pfa.randomForest.fromFrame <- function(dataFrame, exclude = list()) {
    out <- list()
    for (x in names(dataFrame))
        if (!(x %in% exclude))
            out[[gsub("\\.", "_", x)]] <- avro_type(dataFrame[[x]])
    out
}

pfa.randomForest.inputType <- function(regressors, name = NULL, namespace = NULL) {
    avro_record(regressors, name, namespace)
}

pfa.randomForest.treeType <- function(regressors, response) {
    fieldNames <- names(regressors)

    dataTypes <- unique(unlist(regressors, use.names = FALSE))
    if (length(dataTypes) == 1)
        value <- dataTypes[[1]]
    else
        value <- do.call(avro_union, lapply(dataTypes, 
                                            function(t) { 
                                              if (t == "string") {
                                                avro_array(avro_string) 
                                              } else {
                                                t
                                              }
                                              }))

    avro_record(list(field = avro_enum(fieldNames, "FieldNames"),
                     operator = avro_string,
                     value = value,
                     pass = avro_union(response, "TreeNode"),
                     fail = avro_union(response, "TreeNode")),
                "TreeNode")
}

pfa.randomForest.walkTree <- function(input, inputType, tree) {
    env = new.env()
    env[["input"]] = input
    env[["inputType"]] = inputType
    env[["tree"]] = tree
    pfa_expr(quote(model.tree.simpleWalk(input,
                                         tree,
                                         function(d = inputType, t = "TreeNode" -> avro_boolean) {
                                             model.tree.simpleTest(d, t)
                                         })), env = env)
}

pfa.randomForest.walkForest <- function(input, inputType, forest) {
    env = new.env()
    env[["walkTree"]] <- pfa.randomForest.walkTree(input, inputType, "tree")
    pfa_expr(quote(a.map(forest, function(tree = "TreeNode" -> avro_string) walkTree)), env = env)
}

pfa.randomForest.modelParams <- function(forest, regressors, dataFrame) {
    if (!("randomForest" %in% class(forest)))
        stop("pfa.randomForest.modelParams requires an object of class \"randomForest\"")

    ntree <- forest$ntree
    out <- list()

    dataTypes <- unique(unlist(regressors, use.names = FALSE))
    valueNeedsTag <- (length(dataTypes) > 1)

    dataLevels <- list()
    for (name in names(regressors))
        if (is.factor(dataFrame[[name]]))
            dataLevels[[name]] <- levels(dataFrame[[name]])

    for (i in 1:ntree) {
        tree <- pfa.randomForest.extractTree(forest, i, labelVar = TRUE)
        out[[i]] <- pfa.randomForest.buildOneTree(tree, 1, valueNeedsTag, dataLevels)$TreeNode
    }
    out
}
