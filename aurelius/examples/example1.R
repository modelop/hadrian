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

pfaDocument <- pfa.config(input = avro.double,
                          output = avro.double,
                          action = expression(input + 100))

engine <- pfa.engine(pfaDocument)
## > engine$action(1)
## [1] 101
## > engine$action(2)
## [1] 102
## > engine$action(3)
## [1] 103

outFile <- file("/tmp/test.pfa")
write(json(pfaDocument), outFile)
close(outFile)
## {"input": "double","output": "double","action": [{"+": ["input",100]}]}
