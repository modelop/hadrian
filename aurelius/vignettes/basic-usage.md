# aurelius Basic Usage
Steven Mortimer  
`r Sys.Date()`  


# Motivation

The purpose of this document is to demonstrate some of the functionalty provided by 
the aurelius package.




# A Simple Example

First, we load the `aurelius`


```r
library(aurelius)
```

Second, let's build a simple linear regression model and convert to PFA.


```r

# build a model
lm_model <- lm(mpg ~ hp, data = mtcars)

# convert the lm object to a list-of-lists PFA representation
lm_model_as_pfa <- pfa(lm_model)
```

The model can be saved as PFA JSON and used in other systems.


```r

# save as plain-text JSON
write_pfa(lm_model_as_pfa, file = "my-model.pfa")

```

Just as models can be written as a PFA file, they can be read.


```r

my_model <- read_pfa("my-model.pfa")

```
