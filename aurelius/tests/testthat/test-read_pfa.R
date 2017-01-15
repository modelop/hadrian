context("read_pfa")

test_that("read_xml works with all input object types", {

  model_as_list <- list(input='double', output='double', action=list(list(`+`=list('input', 10))))
  
  # literal JSON string  (useful for small examples)
  toy_model <- read_pfa('{"input": "double", "output": "double", "action": [{"+": ["input", 10]}]}')
  expect_identical(toy_model, model_as_list)
  
  # from a local path
  local_model <- read_pfa(system.file("extdata", "my-model.pfa", package = "aurelius"))
  expect_identical(local_model, model_as_list)
  
  # from a url
  url_model <- read_pfa("https://raw.githubusercontent.com/ReportMort/hadrian/feature/add-r-package-structure/aurelius/inst/extdata/my-model.pfa")
  expect_identical(url_model, model_as_list)
  
})
