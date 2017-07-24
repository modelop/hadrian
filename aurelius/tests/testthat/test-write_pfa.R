context("write_pfa")

model_as_string <- '{"input":"double","output":"double","action":[{"+":["input",10]}]}'
model <- read_pfa(model_as_string)

test_that("write_pfa errors for incorrect directory and with invalid inputs", {
  
  filename <- ".../test.pfa"
  expect_error(suppressWarnings(write_pfa(model, file=filename)), "cannot open the connection")
  expect_error(suppressWarnings(write_pfa(model, file=c("test.pfa", "foo"))), "invalid 'description' argument")
  
})

test_that("write_pfa works with relative file paths", {
  
  filename <- "../test.pfa"
  on.exit(unlink(filename))
  write_pfa(model, filename)
  expect_identical(readChar(filename, 1000L), paste0(model_as_string, '\n'))
  
})

test_that("write_pfa works with an explicit connections", {

  filename <- "../test.pfa"
  file <- file(filename, "wb")
  on.exit(unlink(filename))
  write_pfa(model, file)
  close(file)
  expect_identical(readChar(filename, 1000L), paste0(model_as_string, '\n'))
  
})

test_that("write_pfa works with an implicit connections", {

  filename <- "../test.pfa.gz"
  write_pfa(model, filename)
  file <- gzfile(filename, "rb")
  on.exit({unlink(filename); close(file)})
  expect_identical(readChar(file, 1000L), paste0(model_as_string, '\n'))
  
})
