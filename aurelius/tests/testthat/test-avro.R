context("avro")

test_that("test various avro types", {
  
  fixed <- avro_fixed(6, "MACAddress")
  expect_is(fixed, "list")
  expect_equal(names(fixed), c('type', 'size', 'name'))
  
  enum <- avro_enum(list("one", "two", "three"))
  expect_is(enum, "list")
  expect_equal(names(enum), c('type', 'symbols', 'name'))
   
  array1 <- avro_array(avro_int)
  expect_is(array1, "list")
  expect_equal(names(array1), c('type', 'items'))
    
  array2 <- avro_array(avro_string)
  expect_is(array2, "list")
  expect_equal(names(array2), c('type', 'items'))  
  
  map1 <- avro_map(avro_int)
  expect_is(map1, "list")
  expect_equal(names(map1), c('type', 'values'))
  
  map2 <- avro_map(avro_string)
  expect_is(map2, "list")
  expect_equal(names(map2), c('type', 'values'))
  
  record <- avro_record(list(one = avro_int, two = avro_double, three = avro_string))
  expect_is(record, "list")
  expect_equal(names(record), c('type', 'fields', 'name'))
  
  union1 <- avro_union(avro_null, avro_int)    
  expect_is(union1, "list")
  expect_equal(unlist(union1), c('null', 'int'))
  
  union2 <- avro_union(avro_double, avro_string)
  expect_is(union2, "list")
  expect_equal(unlist(union2), c('double', 'string'))
  
})


test_that("test avro_from_df", {

  avro_df <- avro_from_df(data.frame(x = c(1, 3, 5)))
  expect_is(avro_df, "list")
  expect_equal(names(avro_df), c('type', 'fields', 'name'))

})


test_that("test avro_fullname", {

  expect_equal('MyRecord', avro_fullname(avro_record(list(), "MyRecord")))
  expect_equal('com.wowzers.MyRecord', avro_fullname(avro_record(list(), "MyRecord", "com.wowzers")))

})


test_that("test avro_type", {
  
  expect_equal('string', avro_type("hello"))           
  expect_equal('string', avro_type(factor("hello")))
  expect_equal('double', avro_type(3.14))  
  expect_equal('int', avro_type(as.integer(3)))
  expect_equal('boolean', avro_type(TRUE))   
  expect_equal('null', avro_type(NULL))
  
  expect_error(avro_type(data.frame(x = c(1, 3, 5))))

})


test_that("test avro_typemap", {
  
  tm <- avro_typemap(
      MyType1 = avro_record(list(one = avro_int, two = avro_double, three = avro_string)),
      MyType2 = avro_array(avro_double)
  )
  
  whole_decl <- tm("MyType1")
  expect_is(whole_decl, "list")
  expect_equal(names(whole_decl), c('type', 'fields', 'name'))
  
  just_schema_name <- tm("MyType1")
  expect_is(just_schema_name, "character")
  
  whole_decl1 <- tm("MyType2")
  expect_is(whole_decl1, "list")
  expect_equal(names(whole_decl1), c('type', 'items'))
  
  whole_decl2 <- tm("MyType2")
  expect_is(whole_decl2, "list")
  expect_equal(names(whole_decl2), c('type', 'items'))

})
