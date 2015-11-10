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

#' pfa.glmnet.extractParams
#'
#' Extract generalized linear model net parameters from the glm library
#' @param cvfit an object of class "cv.glmnet"
#' @param lambdaval FIXME
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @export pfa.glmnet.extractParams
#' @examples
#' FIXME

pfa.glmnet.extractParams <- function(cvfit, lambdaval = "lambda.1se") {
    if (!("cv.glmnet" %in% class(cvfit)))
        stop("pfa.glmnet.extractParams requires an object of class \"cv.glmnet\"")

    fitobj <- cvfit$glmnet.fit
    predobj <- predict(cvfit, type="coef", s = cvfit[[lambdaval]])
    callobj <- fitobj$call

    if (is.null(callobj$alpha))
        callobj$alpha <- 1
    if (is.null(callobj$family))
        callobj$family <- "binomial"
    
    if (callobj$family == "binomial") {
        binary <- TRUE
        coeff <- as.list(attributes(predobj)$x[-1])
        const <- attributes(predobj)$x[1]
        regressors <- cvfit$predictors[attributes(predobj)$i]
        responses <- fitobj$classnames
        linkFcn <- "model.reg.norm.logit"
        producerParams <- list(alpha = fitobj$call$alpha,
                               lambda = cvfit[[lambdaval]],
                               whichLambda = lambdaval,                             
                               minCVError = cvfit$cvm[cvfit$lambda == cvfit[[lambdaval]]],
                               nObs = fitobj$nobs)
    }
    else {
        binary <- FALSE
        mcoef <- do.call("cbind", lapply(predobj, function(x) as.matrix(x)))
        mcoef = t(mcoef)
        mactive <- do.call("c", lapply(predobj, function(x) attributes(x)$i))
        mactive <- sort(unique(mactive)) + 1
        mcoef <- mcoef[,mactive]
        coeff <- as.list(unname(data.frame(unname(t(mcoef[,-1])))))
        const <- as.list(unname(mcoef[,1]))
        regressors <- dimnames(mcoef)[[2]][-1]
        responses <- fitobj$classnames
        linkFcn <- "model.reg.norm.softmax"
        producerParams <- list(alpha = callobj$alpha,
                               lambda = cvfit[[lambdaval]],
                               whichLambda = lambdaval,
                               minCVError = cvfit$cvm[cvfit$lambda == cvfit[[lambdaval]]],
                               nObs = fitobj$nobs)
    }   

    list(binary = binary,
         coeff = coeff,
         const = const,
         regressors = lapply(regressors, function(x) gsub("\\.", "_", x)),
         responses = responses,
         linkFcn = linkFcn,
         producerParams = producerParams)
}

#' pfa.glmnet.inputType
#'
#' describeme
#' @param x describeme
#' @return describeme
#' @export pfa.glmnet.inputType
#' @examples
#' someExamples

pfa.glmnet.inputType <- function(params, name = NULL, namespace = NULL) {
    fields = list()
    for (x in params$regressors)
        fields[[x]] <- avro.double
    avro.record(fields, name, namespace)
}

#' pfa.glmnet.regressionType
#'
#' describeme
#' @param x describeme
#' @return describeme
#' @export pfa.glmnet.regressionType
#' @examples
#' someExamples

pfa.glmnet.regressionType <- function(params) {
    if (params$binary)
        avro.record(list(coeff = avro.array(avro.double),
                         const = avro.double),
                    "LogisticRegression")
    else
        avro.record(list(coeff = avro.array(avro.array(avro.double)),
                         const = avro.array(avro.double)),
                    "LogisticRegression")
}

#' pfa.glmnet.predictProb
#'
#' describeme
#' @param x describeme
#' @return describeme
#' @export pfa.glmnet.predictProb
#' @examples
#' someExamples

pfa.glmnet.predictProb <- function(params, input, model) {
    newarray <- list(type = avro.array(avro.double),
                     new = lapply(params$regressors, function (x)
                         list(attr = input, path = list(list(string = x)))))
    rawresult <- list(model.reg.linear = list(newarray, model))
    out <- list()
    out[[params$linkFcn]] <- rawresult
    out
}

#' pfa.glmnet.modelParams
#'
#' describeme
#' @param x describeme
#' @return describeme
#' @export pfa.glmnet.modelParams
#' @examples
#' someExamples

pfa.glmnet.modelParams <- function(params) {
    list(coeff = params$coeff, const = params$const)
}
