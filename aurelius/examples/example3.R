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
    input = avro.record(list(name = avro.string,
                             x = avro.double,
                             y = avro.double,
                             z = avro.double,
                             spec = avro.string,
                             planets = avro.boolean,
                             mag = avro.union(avro.double, avro.null))),
    output = avro.double,
    action = expression(m.sqrt(a.sum(new(avro.array(avro.double), input$x**2, input$y**2, input$z**2)))))

engine <- pfa.engine(pfaDocument)
## > engine$action(json.map(name = "Sun", x = 0.0, y = 0.0, z = 0.0, spec = "G2 V", planets = TRUE, mag = list(double = -26.72)))
## [1] 0
## > engine$action(json.map(name = "Proxima Centauri", x = 2.94, y = -3.05, z = -0.14, spec = "M5 Ve", planets = FALSE, mag = list(double = 11.05)))
## [1] 4.238596
## > engine$action(json.map(name = "Alpha Centauri A", x = 3.13, y = -3.05, z = -0.05, spec = "G2 V", planets = FALSE, mag = list(double = 0.01)))
## [1] 4.370572
## > engine$action(json.map(name = "Alpha Centauri B", x = 3.13, y = -3.05, z = -0.05, spec = "K0 V", planets = FALSE, mag = list(double = 1.34)))
## [1] 4.370572
## > engine$action(json.map(name = "Alpha Centauri Bb", x = 3.13, y = -3.05, z = -0.05, spec = "", planets = FALSE, mag = NULL))
## [1] 4.370572
## > engine$action(json.map(name = "Barnard's Star", x = 4.97, y = 2.99, z = 1.45, spec = "M3.5 V", planets = FALSE, mag = list(double = 9.57)))
## [1] 5.978587
## > engine$action(json.map(name = "Luhman 16 A", x = 1.72, y = -6.32, z = 0.61, spec = "L7.5", planets = FALSE, mag = NULL))
## [1] 6.578214
## > engine$action(json.map(name = "Luhman 16 B", x = 1.72, y = -6.32, z = 0.61, spec = "T0.5", planets = FALSE, mag = NULL))
## [1] 6.578214
