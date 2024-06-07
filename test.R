
main <- function(script_dir, configuration_path, connection_string, new_files) {
   

  data <- data.frame(configuration_path = configuration_path, file_paths = new_files, stringsAsFactors = FALSE)
  writeLines(c(script_dir, configuration_path, connection_string, new_files), "Test2.txt")
  
  # Write the dataframe to a CSV file
  write.csv(data, file = "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\file_paths.csv", row.names = FALSE)
  write.csv(data, file = "file_paths.csv", row.names = FALSE)
  
}
  
  


args <- commandArgs(trailingOnly = TRUE)


new_files <- args[-c(1:3)]
script_dir <- args[1]
configuration_path <- args[2]
connection_string <- args[3]

writeLines(c(script_dir, configuration_path, connection_string, new_files), "Test.txt")

main(script_dir, configuration_path, connection_string, new_files)
