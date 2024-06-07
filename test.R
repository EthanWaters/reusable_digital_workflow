
main <- function(script_dir, configuration_path, connection_string, new_files) {
   

  data <- data.frame(configuration_path = configuration_path, file_paths = new_files, stringsAsFactors = FALSE)
  
  # Write the dataframe to a CSV file
  write.csv(data, file = "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\file_paths.csv", row.names = FALSE)
  
}
  
  
  

args <- commandArgs(trailingOnly = TRUE)
new_path_list <- args[2]
configuration_path <- args[1]

main(configuration_path, new_path_list)
