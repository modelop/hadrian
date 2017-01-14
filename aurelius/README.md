<!-- README.md is generated from README.Rmd. Please edit that file -->
aurelius
========

[![Build Status](https://travis-ci.org/opendatagroup/hadrian/hadrian.png?branch=master)](https://travis-ci.org/opendatagroup/hadrian) [![Coverage Status](https://img.shields.io/codecov/c/github/opendatagroup/hadrian/master.svg)](https://codecov.io/github/opendatagroup/hadrian?branch=master)

**aurelius** is a toolkit for generating PFA in the R programming language. It focuses on porting models to PFA from their R equivalents.

Supported Models:

-   glm
-   glmnet
-   randomForest
-   gbm

### Install and Load aurelius Library

``` r

devtools::install_github('opendatagroup/hadrian', subdir='aurelius')
library("aurelius")
```

### Build a Model and Save as PFA

The main purpose of the package is to create PFA documents based on logic created in R. This example shows how to build a simple linear regression model and save as PFA. PFA is a plain-text JSON format.

``` r

  # build a model
  lm_model <- lm(mpg ~ hp, data = mtcars)
  
  # convert the lm object to a list of lists PFA representation
  lm_model_as_pfa <- pfa(lm_model)
  
  # save as plain-text JSON
  json(lm_model_as_pfa, fileName = "myModel.pfa")
```

### License

The **aurelius** package is licensed under the Apache License 2.0 (<http://choosealicense.com/licenses/apache-2.0/>).
