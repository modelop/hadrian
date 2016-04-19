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

#' pfa.glmnet.extractParams
#'
#' Extract generalized linear model net parameters from the glm library
#' @param cvfit an object of class "cv.glmnet"
#' @param lambdaval  value of tuning parameter
#' @return PFA as a list-of-lists that can be inserted into a cell or pool
#' @export pfa.glmnet.extractParams
#' @examples
#' x1 <- rnorm(100)
#' x2 <- runif(100)
#' X <- matrix(c(x1, x2), nc = 2)
#' Y <- 3 * x1 * x2
#' hist(Y)
#' Y <- Y > 0
#' uu <- glmnet(X, Y)

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
#' Extracts a set of regression variable names from a glmnet object
#' @param params list of regressor names
#' @param name required name
#' @param namespace optional namespace
#' @return List of regression parameter names
#' @export pfa.glmnet.inputType
#' @examples
#' xr <- list(regressors = c("x1", "x2"))
#' xrType <- pfa.glmnet.inputType(xr)


pfa.glmnet.inputType <- function(params, name = NULL, namespace = NULL) {
    fields = list()
    for (x in params$regressors)
        fields[[x]] <- avro.double
    avro.record(fields, name, namespace)
}

#' pfa.glmnet.regressionType
#'
#' Determines regression variable type
#' @param params type of output variable
#' @return type of output variable specified by params
#' @export pfa.glmnet.regressionType
#' @examples
#' pfa.glmnet.regressionType(list(binary = TRUE))

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
#' Function to extract prediction probabilities from glmnet object
#' @param params parameters
#' @param input variables
#' @param model input model
#' @return prediction probabilities
#' @export pfa.glmnet.predictProb
#' @examples
#' x1 <- rnorm(100)
#' x2 <- runif(100)
#' X <- matrix(c(x1, x2), nc = 2)
#' Y <- 3 * x1 * x2
#' Y <- Y > 0
#' uu <- glmnet(X, Y)
#' u1 <- list(regressors = uu$beta, linkFcn = "logit")
#' u2 <- pfa.glmnet.predictProb(u1, "V1", uu)


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
#' Extracts model coefficients
#' @param params function to extract glmnet model parameters
#' @return glmnet intercept and coefficients
#' @export pfa.glmnet.modelParams
#' @examples
#' x1 <- rnorm(100)
#' x2 <- runif(100)
#' X <- matrix(c(x1, x2), nc = 2)
#' Y <- 3 * x1 * x2
#' Y <- Y > 0
#' uu <- glmnet(X, Y)
#' vv <- pfa.glmnet.modelParams(list(const = uu$a0[1], coeff = uu$beta[1, ]))



pfa.glmnet.modelParams <- function(params) {
    list(coeff = params$coeff, const = params$const)
}
