suppressMessages(suppressWarnings(library(testthat)))
suppressMessages(suppressWarnings(library(aurelius)))

if (identical(tolower(Sys.getenv("NOT_CRAN")), "true") & 
    identical(tolower(Sys.getenv("TRAVIS_PULL_REQUEST")), "false")) {
  
  test_check('aurelius')
  
}
