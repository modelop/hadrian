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

source("pfa-generator.R")

baselineMu <- 2.0
baselineSigma <- 5.0
alternateMu <- 10.0
alternateSigma <- 3.0

initialValue <- 0.0
resetValue <- 0.0
threshold <- 5.0

pfaDocument <- pfa.config(
    input = avro.double,
    output = avro.boolean,
    cells = list(last = pfa.cell(avro.double, initialValue)),
    method = "emit",
    action = expression(
        last <<- function(oldValue = avro.double -> avro.double) {
            newValue <- stat.change.updateCUSUM(
                # alternate minus baseline
                prob.dist.gaussianLL(input, alternateMu, alternateSigma) - prob.dist.gaussianLL(input, baselineMu, baselineSigma),
                oldValue,
                resetValue)
            emit(newValue > threshold)
            newValue
        }
        ))

engine <- pfa.engine(pfaDocument)

## > data <- c(3.35, -1.37, -3.92, 6.74, 12.06, 3.81, 3.35, -1.18, -1.39, 5.55, 5.3, 12.8, 10.36, 12.05, 3.8, 12.81, 11.1, 8.37, 7.32, 15.22)
## > lapply(data, engine$action)
## [[1]]
## [1] FALSE

## [[2]]
## [1] FALSE

## [[3]]
## [1] FALSE

## [[4]]
## [1] FALSE

## [[5]]
## [1] FALSE

## [[6]]
## [1] FALSE

## [[7]]
## [1] FALSE

## [[8]]
## [1] FALSE

## [[9]]
## [1] FALSE

## [[10]]
## [1] FALSE

## [[11]]
## [1] FALSE

## [[12]]
## [1] FALSE

## [[13]]
## [1] FALSE

## [[14]]
## [1] TRUE

## [[15]]
## [1] TRUE

## [[16]]
## [1] TRUE

## [[17]]
## [1] TRUE

## [[18]]
## [1] TRUE

## [[19]]
## [1] TRUE

## [[20]]
## [1] TRUE

