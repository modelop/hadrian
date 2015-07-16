# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    tree
}

pfa.gbm.buildOneTree <- function(tree, whichNode, valueNeedsTag = TRUE, dataLevels = NULL, fieldTypes = NULL) {
    node <- tree[whichNode,]
  
    if (!is.na(node$SplitVar)) {
        split.val <- node$SplitCodePred

        # replace "." in a variable name with an underscore "_"
        f <- gsub("\\.", "_", node$SplitVar)
    
        # "t" is the avro type of the split point
        if (is.null(fieldTypes) || is.null(fieldTypes[[f]]))
            t <- avro.type(split.val)
        else {
            t <- fieldTypes[[f]]
            if (is.list(t) && "name" %in% names(t))
                t <- t$name
        }
    
        if (valueNeedsTag) {
            out <- list()
            out[[t]] <- split.val
            split.val <- out
        }
        op <- "<"
  
        list(TreeNode = 
             list(field    = f,
                  operator = op,
                  value    = split.val,
                  pass     = pfa.gbm.buildOneTree(tree, node$LeftNode,    valueNeedsTag, dataLevels, fieldTypes),
                  fail     = pfa.gbm.buildOneTree(tree, node$RightNode,   valueNeedsTag, dataLevels, fieldTypes),
                  missing  = pfa.gbm.buildOneTree(tree, node$MissingNode, valueNeedsTag, dataLevels, fieldTypes)))
    }
    else
        list(double = node$Prediction)    
}
