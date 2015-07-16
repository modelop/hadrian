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

pfa.glm.extractParams <- function(fit) {
    if (!("glm" %in% class(fit)))
        stop("pfa.glm.extractParams requires an object of class \"glm\"")

    if (fit$family$family == "binomial") {
        binary <- TRUE
        coeff <- as.list(fit$coefficients)
        const <- coeff[["(Intercept)"]]
        coeff[["(Intercept)"]] <- NULL
        covmatrix <- vcov(fit)
        covar <- vector("list", nrow(covmatrix))
        for (i in 1:nrow(covmatrix)) {
            row <- vector("list", ncol(covmatrix))
            for (j in 1:ncol(covmatrix)) {
                if (j == 1)
                    jj = ncol(covmatrix)  # intercept goes last, not first
                else
                    jj = j - 1
                row[[jj]] = covmatrix[i, j]
            }
            if (i == 1)
                ii = nrow(covmatrix)  # intercept goes last, not first
            else
                ii = i - 1
            covar[[ii]] = row
        }
        regressors <- names(coeff)
        if (fit$family$link == "logit")
            linkFcn <- "model.reg.norm.logit"
        else
            stop("unrecognized link function")
    }
    else {
        stop("not implemented yet")
    }

    list(binary = binary,
         coeff = coeff,
         const = const,
         covar = covar,
         regressors = lapply(regressors, function(x) gsub("\\.", "_", x)),
         linkFcn = linkFcn)
}

pfa.glm.inputType <- function(params, name = NULL, namespace = NULL) {
    fields = list()
    for (x in params$regressors)
        fields[[x]] <- avro.double
    avro.record(fields, name, namespace)
}

pfa.glm.regressionType <- function(params) {
    if (params$binary)
        avro.record(list(coeff = avro.array(avro.double),
                         const = avro.double),
                    "LogisticRegression")
    else
        avro.record(list(coeff = avro.array(avro.array(avro.double)),
                         const = avro.array(avro.double)),
                    "LogisticRegression")
}

pfa.glm.predictProb <- function(params, input, model) {
    newarray <- list(type = avro.array(avro.double),
                     new = lapply(params$regressors, function (x)
                         list(attr = input, path = list(list(string = x)))))
    rawresult <- list(model.reg.linear = list(newarray, model))
    out <- list()
    out[[params$linkFcn]] <- rawresult
    out
}

pfa.glm.modelParams <- function(params) {
    list(coeff = params$coeff, const = params$const)
}
