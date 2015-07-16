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

pfaDocument <- pfa.config(
    input = avro.string,
    output = avro.int,
    pools = list(wordCount = pfa.pool(avro.int, json.map())),
    action = expression(
        wordCount[[input]] <<- function(old = avro.int -> avro.int) { old + 1 } <- 0,
        wordCount[[input]]
        ))

engine <- pfa.engine(pfaDocument)
## > engine$action("hello")
## [1] 1
## > engine$action("hello")
## [1] 2
## > engine$action("there")
## [1] 1
## > engine$action("there")
## [1] 2
## > engine$action("one")
## [1] 1
## > engine$action("hello")
## [1] 3
## > engine$action("hello")
## [1] 4
## > engine$action("hello")
## [1] 5
