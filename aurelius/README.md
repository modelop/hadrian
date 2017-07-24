<!-- README.md is generated from README.Rmd. Please edit that file -->
aurelius
========

[![Build Status](https://travis-ci.org/opendatagroup/hadrian.svg?branch=master)](https://travis-ci.org/opendatagroup/hadrian) [![Coverage Status](https://img.shields.io/codecov/c/github/opendatagroup/hadrian/master.svg)](https://codecov.io/github/opendatagroup/hadrian?branch=master)

**aurelius** is a toolkit for translating models and analytics from the R programming language into the Portal Format for Analytics (PFA). There are functions for importing, exporting and converting common R classes of models into PFA. There are also functions for converting variable assignment, control structures, and other elements of the R syntax into PFA.

Getting Started
---------------

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
```

The model can be saved as PFA JSON and used in other systems.

``` r
# save as plain-text JSON
write_pfa(lm_model_as_pfa, file = "my-model.pfa")
```

Just as models can be written as a PFA file, they can be read.

``` r
my_model <- read_pfa("my-model.pfa")
```

Supported Models
----------------

The `pfa()` function in this package supports direct conversion to PFA for objects created by the following functions:

| Model                                            | Function                             | Prediction                           | Libraries           |
|:-------------------------------------------------|:-------------------------------------|:-------------------------------------|:--------------------|
| Autoregressive Integrated Moving Average (ARIMA) | `arima()`, `Arima()`, `auto.arima()` | Time Series                          | `stats`, `forecast` |
| Classification and Regression Trees (CART)       | `rpart()`                            | Classification, Regression, Survival | `rpart`             |
| Exponential Smoothing State Space                | `ets()`, `ses()`, `hw()`, `holt()`   | Time Series                          | `forecast`          |
| Generalized Boosted Regression Models            | `gbm()`                              | Classification, Regression, Survival | `gbm`               |
| Generalized Linear Model                         | `glm()`                              | Classification, Regression           | `stats`             |
| Holt-Winters Filtering                           | `HoltWinters()`                      | Time Series                          | `stats`, `forecast` |
| K-Centroids Clustering                           | `kcca()`                             | Clustering                           | `flexclust`         |
| K-Means Clustering                               | `kmeans()`                           | Clustering                           | `stats`             |
| k-Nearest Neighbour                              | `knn3()`, `knnreg()`, `ipredknn()`   | Classification, Regression           | `caret`, `ipred`    |
| Linear Discriminant Analysis                     | `lda()`                              | Classification                       | `MASS`              |
| Linear Model                                     | `lm()`                               | Regression                           | `stats`             |
| Naive Bayes Classifier                           | `naiveBayes()`                       | Classification                       | `e1071`             |
| Random Forest                                    | `randomForest()`                     | Classification, Regression           | `randomForest`      |
| Regularized Generalized Linear Models            | `glmnet()`, `cv.glmnet()`            | Classification, Regression, Survival | `glmnet`            |

License
-------

The **aurelius** package is licensed under the [Apache License 2.0](http://choosealicense.com/licenses/apache-2.0/).
