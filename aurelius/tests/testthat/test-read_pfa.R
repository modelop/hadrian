context("read_pfa")

test_that("read_pfa works with all input object types", {

  model_as_list <- list(input='double', output='double', action=list(list(`+`=list('input', 10))))
  
  # literal JSON string  (useful for small examples)
  toy_model <- read_pfa('{"input": "double", "output": "double", "action": [{"+": ["input", 10]}]}')
  expect_identical(toy_model, model_as_list)
  
  # from a local path, must be wrapped in "file" command to create a connection
  file_conn <- file(system.file("extdata", "my-model.pfa", package = "aurelius"))
  local_model <- read_pfa(file_conn)
  expect_identical(local_model, model_as_list)
  
  # from a url
  url_conn <- url(paste0("https://raw.githubusercontent.com/ReportMort/hadrian",
                         "/feature/add-r-package-structure/aurelius/inst/extdata/my-model.pfa"))
  url_model <- read_pfa(url_conn)
  expect_identical(url_model, model_as_list)
  
})
