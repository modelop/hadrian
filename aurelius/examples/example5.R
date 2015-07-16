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
    input = avro.int,
    output = avro.array(avro.double),
    action = expression(u.bernoulli(input)),
    fcns = list(
        bernoulli = expression(function(N = avro.int -> avro.array(avro.double)) {
            BN <- new(avro.array(avro.double), 1, -0.5)
            for (M in 1:(N - 1)) {
                S <- -(1/(M + 1) - 0.5)
                for (K in 1:(M - 1)) {
                    R <- new(avro.double, 1.0)
                    for (J in 1:(K - 1))
                        R <- R*(J + M - K)/J
                    S <- S - R*BN[K - 1]
                }
                BN <- a.append(BN, S)
            }
            for (M in 2:N)
                if (M %% 2 == 0)
                    BN <- a.replace(BN, M, 0)
            BN
        })))

engine <- pfa.engine(pfaDocument)

engine$action(10)
