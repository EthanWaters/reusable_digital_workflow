# Format the new control data into the stardard legacy format 


import_data <- function(data, configuration){
    out <- tryCatch(
      {
        file_name <- tools::file_path_sans_ext(basename(data))
       
        # Opens file and stores information into dataframe. Different file types
        # require different functions to read data
        file_extension <- file_ext(data)
        if (file_extension == 'xlsx'){
          data_tibble <- read_xlsx(data, sheet = configuration$metadata$legacy_sheet_index)
          data_tibble_colnames <- colnames(data_tibble)
          data_df <- data.frame(data_tibble)
          colnames(data_df) <- data_tibble_colnames
        } else if (file_extension == 'csv'){
          data_df <- read.csv(data, header = TRUE,  encoding="UTF-8", check.names=FALSE)
        } else {
          data_df <- read.table(file=data, header=TRUE)
        }
        
        column_names <- colnames(data_df)
        column_names <- gsub("\\.", " ", column_names)
        column_names <- gsub("\\s+", " ", column_names)
        colnames(data_df) <- column_names
        
        return(data_df)
      },
      
      error=function(cond) {
        message(paste("Cannot read from:", data))
        message("Original error message:")
        message(cond)
      }
    ) 
  return(out)
}

create_metadata_report <- function(control_data_type){
    #Generate the XML template for the control data process report.
    reports_location <<- "reports\\"
    if (!dir.exists(reports_location)) {
      dir.create(reports_location)
    }
    
    #create file name systematically
    date <- as.character(Sys.Date())
    timestamp <- as.character(Sys.time())
    filename <- paste0(reports_location, "Processed Control Data Report ", date, control_data_type, ".xml", sep="")
    
    #generate template
    template <- xml_new_root("session") 
    control_data <- xml_find_all(template, "//session")
    xml_add_child(control_data, "timestamp", timestamp) 
    xml_add_child(control_data, "Notifications")
    write_xml(template, file = filename, options =c("format", "no_declaration"))
}


contribute_to_metadata_report <- function(control_data_type, data, key = "Warning"){
  # Finds desired control data node and adds a section from the information 
  # obtained in the previously executed function. 

  # finds files with current date in file name and attempts to open xml file
  file_count <- 1
  reports_location <- "reports\\"
  trywait <- 0
  xml_files <- list.files(path= reports_location, pattern = as.character(Sys.Date()))
  xml_filename <- xml_files[file_count]
  xml_file <- paste0(reports_location, xml_filename, sep="")
  xml_file_data <- try(read_xml(xml_file))
  while ((class(xml_file_data)[[1]]=='try-error')&(trywait<=(5))){
    file_count <- file_count + 1
    print(paste('retrying in ', trywait, 'second(s)')) 
    Sys.sleep(trywait) 
    trywait <- trywait + 1 
    xml_file_data <- try(read_xml(paste0(reports_location, xml_filename, sep="")))
  }
  node <- xml_find_all(xml_file_data, "//Notifications")
  
  # Only add to node if it exists. Node Should always exist. 
  # A dataframe will add a key value pair for each column. A single value will  
  # directly assigned as a value. Columns with Multiple entries will form a list
  # before being assigned as a value. 
  # A matrix will pair each entry with the specified key. 
  if(length(node) > 0){
    if(is.data.frame(data)){
      xml_add_child(node, key ,data)
    } else {
      desired_nodes <- xml_find_all(xml_file_data, "//Notifications")
      newest_desired_node <- desired_nodes[[length(desired_nodes)]]
      sapply(1:length(data), function(i) {
        xml_add_child(newest_desired_node, key, data[i])
       }
      )
    }
    
  }
 
  
  write_xml(xml_file_data, file = xml_file, options =c("format", "no_declaration"))
}

outersect <- function(x, y) {
  sort(c(x[!x%in%y],
         y[!y%in%x]))
}

separate_control_dataframe <- function(new_data_df, legacy_data_df, control_data_type){
  # 
  # new_data_df <- verified_data_df
  # legacy_data_df <- legacy_df
  
  ID_col <- colnames(new_data_df)[1]
  
  column_names <- colnames(legacy_data_df)
  verified_data_df <- data.frame(matrix(ncol = length(column_names), nrow = 0))
  colnames(verified_data_df) <- column_names 
  
  # If there is a unique ID then perfect duplicates can easily be removed.
  if(has_authorative_ID){
    
    # Find close matches of rows left without an ID and no perfect duplicates. 
    # Rows with a single close match will be considered a discrepancy and then
    # the ID checked against the all the IDs in legacy_data_df to ensure it does 
    # not already exist.
    
    # Determine additional columns required by the new data format and remove 
    # from comparison
    
    required_columns <- intersect(configuration$mappings$new_fields$field, names(new_data_df))
    # save original dataframes for legacy and new data so it can be manipulated
    # without loosing data. Columns will be removed that are not going to be  
    # compared for similarity. A unique identifier for each row will be created 
    # based on data in the row and compared. 
    temp_legacy_df <- legacy_data_df[,-which(colnames(legacy_data_df) %in% required_columns)]
    temp_new_df <- new_data_df[,-which(colnames(new_data_df) %in% required_columns)]
   
    # Create a unique identifier for each row in legacy_data_df and new_data_df
    temp_legacy_df$Identifier <- apply(temp_legacy_df, 1, function(row) paste(row, collapse = "_"))
    temp_new_df$Identifier <- apply(temp_new_df, 1, function(row) paste(row, collapse = "_"))
    
    #find perfect duplicates and add to verified data df
    perfect_duplicates <- legacy_data_df[temp_legacy_df$Identifier %in% temp_new_df$Identifier, ]
    verified_data_df <- rbind(verified_data_df, perfect_duplicates)
    
    # remove identifier columns
    temp_legacy_df$Identifier <- NULL
    temp_new_df$Identifier <- NULL
    
    # find new entries based on whether the ID is present in both dataframes 
    new_entries <- anti_join(new_data_df, legacy_data_df, by=ID_col)
    
    # Determine discrepancies and store both versions of the entries
    non_discrepancy_ids <- c(perfect_duplicates[[ID_col]], new_entries[[ID_col]])
    discrepancies_new <- new_data_df[!(new_data_df[[ID_col]] %in% non_discrepancy_ids),]
    discrepancies_legacy <- legacy_data_df[!(legacy_data_df[[ID_col]] %in% non_discrepancy_ids),]
      
  } else {
  
    # Determine additional columns required by the new data format and remove 
    # from comparison
    required_columns <- intersect(c(configuration$mappings$new_fields$field, ID_col), names(new_data_df))
    
    # find close matching rows (distance of two) based on all columns except ID. ID is not 
    # because it will always be null if the data is exported from powerBI. 
    distance <- 2
    
    temp_new_df <- new_data_df[ , -which(names(new_data_df) %in% required_columns)]
    temp_legacy_df <- legacy_data_df[ , -which(names(legacy_data_df) %in% required_columns)]
    close_match_rows <- matrix_close_matches_vectorised(temp_legacy_df, temp_new_df, distance)
    
    # There can be many to many perfect matches. This means that there will be 
    # multiple indices referring to the same row for perfect duplicates. 
    # unique() should be utilised when subsetting the input dataframes.
    seperated_close_matches <- vectorised_seperate_close_matches(close_match_rows)
    perfect_duplicates <- new_data_df[unique(seperated_close_matches$perfect[,2]),]
    new_entries_i <- unique(c(seperated_close_matches$discrepancies[,2],seperated_close_matches$perfect[,2]))
    
    # This will contain any new entries and any rows that could not be separated
    new_entries <- new_data_df[-new_entries_i,]
    verified_data_df <- rbind(verified_data_df, perfect_duplicates)
  }
  
  # Given that it is not possible to definitively know if a change / discrepancy 
  # was intentional or not both new and change entries will pass through the 
  # same validation checks and if passed will be accepted as usable and assumed to be . If failed, 
  # assumed to be a QA change. If failed,  the data will be flagged. Failed 
  # discrepancies will check the original legacy entry, which if failed will 
  # be left as is. 
 
  verified_discrepancies <- compare_discrepancies(new_data_df, legacy_data_df, seperated_close_matches$discrepancies, ID_col)
  verified_data_df <- rbind(verified_data_df, verified_discrepancies)
  verified_data_df <- rbind(verified_data_df, new_entries)
  
  # merge the verified dataset
  return(verified_data_df)
  
}


flag_duplicates <- function(new_data_df){
  # New entries need to be checked for duplicates. If there is more than one
  # duplicate it can be assumed to be an error and the error flag set. This 
  # will use a similar identifier new_entries df. This will only flag the 
  # duplicate versions of the row as an error as it still contains new 
  # information there has just been multiple instances of data entry. 
  # Additionally no new entry should be a duplicate of any "perfect duplicate" 
  # as legitimate duplicates would come from the same source and uploaded at 
  # the same time.
  
  new_data_df$Identifier <- apply(new_data_df[,2:ncol(new_data_df)], 1, function(row) paste(row, collapse = "_"))
  duplicates <- duplicated(new_data_df$Identifier)|duplicated(new_data_df$Identifier, fromLast=TRUE)
  counts <- ave(duplicates, new_data_df$Identifier, FUN = sum)
  new_data_df$error_flag <- ifelse(counts >= 3 & duplicates, 1, new_data_df$error_flag)
  new_data_df$Identifier <- NULL
  return(new_data_df)
}


compare_discrepancies <- function(new_data_df, legacy_data_df, discrepancies , ID_col){
  # compare the rows identified as discrepancies from the new and legacy 
  # dataframes. Most changes should be QA and either still meet the requirements 
  # or now meet the requirements and hence will not be flagged as an error. 
  # However a minor number of cases a row is mistakingly changed to no longer be
  # useable, when this occurs the original row will be used implace of the new 
  # one. 
  
  legacy_error_flag <- legacy_data_df[discrepancies[,1], "error_flag"]
  new_error_flag <- new_data_df[discrepancies[,2], "error_flag"]
  
  is_legacy_rows <- ifelse((new_error_flag == 1) & (legacy_error_flag == 0), 1, 0)
  is_legacy_rows <- as.logical(is_legacy_rows)
  output <- rbind(new_data_df[discrepancies[!is_legacy_rows,2],], legacy_data_df[discrepancies[is_legacy_rows,1],])
  return(output)
  
}


vectorised_seperate_close_matches <- function(close_match_rows){
  # Separate the close matching rows with a vectorized process. Duplicates in 
  # columns of `close_match_rows` indicates that there are multiple close 
  # matches between a row(s) in dataframe and and row(s) in y. This process 
  # utilised logical checks on vectors or matrices so Boolean operations can be 
  # utilsied to separate the rows. Although this does reduce readability, the 
  # reduced computational time of two or three orders of magnitude was deemed a 
  # worthy trade off. 
  
  # Order of matches handled
  # 1) Handle rows with one-one close matches 
  # 2) Handle rows with many-many perfect matches 
  # 3) Handle rows with one-many perfect matches
  # 4) Handle rows with one-many nonperfect matches
  # 5) Handle rows with many-many nonperfect matches
  # 6) Handle rows with one-one and one-many nonperfect matches
  
  # This order MATTERS. This is NOT a definitive process and therefore the order
  # needs to maximize the probability that a row from the new data set is 
  # matched with one from the previous data set. One to one matches and
  # many-many perfect matches are the most likely to be correct and therefore 
  # are removed first. It is important that the next matches handled are
  # one-many. This is to ensure a match is found for the "one", as its most 
  # likely match is one of the many with the smallest distance. Any rows with 
  # those indices can then be removed to prevent double handling. Once many-many 
  # rows have been handled, one-one or one-many relationships may have been 
  # formed and therefore can be handled repetitively until all matches have been 
  # found or no more can be found
  
  
  ### ---------- 
  #Initialise variables 
  
  x_df_col <- 1
  y_df_col <- 2
  distance <- max(close_match_rows[,3])
  
  # initialise variables to store indices of rows after seperation
  discrepancies_indices <<- matrix(,ncol=3, nrow =0)
  perfect_duplicate_indices <<- matrix(,ncol=3, nrow =0)
  error_indices <<- matrix(,ncol=3, nrow =0)
  

  
  ### ---------- 
  #one-one close matches
  
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows)
  
  ### ---------- 
  #one-many perfect matches
  
  x_updated_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  
  # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
  perfect_one_to_many <- !(y_updated_dup_indices & x_updated_dup_indices) & (y_updated_dup_indices | x_updated_dup_indices) & (close_match_rows_updated[,3] == 0)
  
  # Only select one match for a row
  perfect_one_to_many_matches <- close_match_rows_updated[perfect_one_to_many,, drop = FALSE]
  x_indices <- perfect_one_to_many_matches[,x_df_col]
  y_indices <- perfect_one_to_many_matches[,y_df_col]
  x_dup_indices <- (duplicated(x_indices)|duplicated(x_indices, fromLast=TRUE))
  y_dup_indices <- (duplicated(y_indices)|duplicated(y_indices, fromLast=TRUE))
  dup_indices <- (y_dup_indices | x_dup_indices)
  non_dup_indices <- !dup_indices
  
  perfect_duplicate_indices <- rbind(perfect_duplicate_indices, perfect_one_to_many_matches[non_dup_indices,])
  error_indices <- rbind(error_indices, perfect_one_to_many_matches[dup_indices,])
  
  # remove checked rows
  close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% x_indices), which(close_match_rows_updated[,y_df_col] %fin% y_indices))),]

  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #many-many perfect matches
  
  # initialize variables for many to many separation
  one_to_one_indices <- c()
  one_to_many_indices <- c()
  many_to_many_indices<- c()
  mistake_duplicates_indices <- c()
  
  # Boolean logic to subset many-many perfect matches from the `close_match_rows`
  # matrix
  is_perfect_match <- close_match_rows_updated[,3] == 0
  x_updated_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  many_to_many <- (y_updated_dup_indices & x_updated_dup_indices) 
  many_to_many_with_perfect_match <- many_to_many & is_perfect_match
  
  # many-many with perfect matches have three possible scenarios. 
  # x_df_col) A single perfect matching row with multiple close matches
  # 2) Two perfect matching rows (This is considered coincidental and both are
  #     treated as unique)
  # 3) more than two perfect matching rows (This is considered a mistake and 
  #     flagged)
  # All scenarios need to be checked against.
  
  # update the relevant matrices if matches are found. The indices with 
  # multiple perfect matches may also have other matches that are nonperfect 
  # and therefore need to be filtered again to ensure only perfect is 
  # considered
  
  if(any(many_to_many_with_perfect_match)){
    
    many_to_many_with_perfect_match_entries <- close_match_rows_updated[many_to_many_with_perfect_match,, drop = FALSE]
    
    x_dup_indices <- (duplicated(many_to_many_with_perfect_match_entries[,x_df_col])|duplicated(many_to_many_with_perfect_match_entries[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(many_to_many_with_perfect_match_entries[,y_df_col])|duplicated(many_to_many_with_perfect_match_entries[,y_df_col], fromLast=TRUE))
    
    
    many_to_many_i <- x_dup_indices & y_dup_indices
    one_to_many_i <- !(x_dup_indices & y_dup_indices) & (y_dup_indices | x_dup_indices)
    one_to_one_i <- !(x_dup_indices | y_dup_indices)
    
    # argument , drop = FALSE keeps as a matrix even if it is only a single row. 
    many_to_many_e <- many_to_many_with_perfect_match_entries[many_to_many_i,, drop = FALSE]
    one_to_many_e <- many_to_many_with_perfect_match_entries[one_to_many_i,, drop = FALSE]
    one_to_one_e <- many_to_many_with_perfect_match_entries[one_to_one_i,, drop = FALSE]
    
    # separate groups of many to many matches. There should not be one to many 
    # relationship of perfect matches, if there is there are a combination of 
    # discrepancies which will not be able to be definitively determined and 
    # therefore will be set as new entries. 
    
    if(nrow(many_to_many_e) > 0){
      
      my_df_colm_split <- lapply(split(many_to_many_e[,x_df_col:y_df_col], many_to_many_e[,y_df_col]), matrix, ncol=y_df_col)
      
      groups <- replicate(length(my_df_colm_split), vector(), simplify = FALSE)
      group <- 0
      output_rec_group <- rec_group(my_df_colm_split, groups, group)
      
      # Now that the many to many perfect matches are grouped, if they are
      # genuine perfect matches with no mistakes there will be as many duplicate
      # vectors as their are matching rows. Any with more than 2 matching rows 
      # as previously discussed will be removed and considered mistake 
      # duplicates. A pair of perfect duplicates will be considered a 
      # coincidence and kept 
      
      perfect_matching_many_to_many_rows <- (duplicated(output_rec_group)|duplicated(output_rec_group, fromLast=TRUE))
      too_many_matches <- unlist(lapply(output_rec_group, function(x) length(x) > 2))
      too_many_matches_indices <- unique(unlist(output_rec_group[too_many_matches]))
      
      too_few_matches <- unlist(lapply(output_rec_group, function(x) length(x) < 2))
      too_few_matches_indices <- unique(unlist(output_rec_group[too_few_matches]))
      
      new_many_to_many <- unique(many_to_many_e[,y_df_col])
      incorrect_many_to_many <- new_many_to_many[new_many_to_many %fin% too_few_matches_indices | new_many_to_many %fin% too_many_matches_indices]
      correct_many_to_many <- new_many_to_many[!(new_many_to_many %fin% too_few_matches_indices | new_many_to_many %fin% too_many_matches_indices)]
      

      mistake_duplicate_manye_indices <- which(many_to_many_e[,y_df_col] %fin% incorrect_many_to_many)
      mistake_duplicates <- many_to_many_e[mistake_duplicate_manye_indices,]
      mistake_duplicates_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% mistake_duplicates[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% mistake_duplicates[,x_df_col])))
      many_to_many_e <- many_to_many_e[-mistake_duplicate_manye_indices,]
      many_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% many_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% many_to_many_e[,x_df_col])))
      
    }
  
    if(nrow(one_to_many_e) > 0){ 
      one_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_many_e[,x_df_col])))
    }
      
    one_to_one_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_one_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_one_e[,x_df_col])))
    
    perfect_duplicate_indices <- rbind(perfect_duplicate_indices, one_to_one_e, many_to_many_e)
    error_indices <- rbind(error_indices, mistake_duplicates, one_to_many_e)
    
    # remove rows that have already been handled to prevent double handling.
    close_match_rows_updated <- close_match_rows_updated[-unique(c(one_to_one_indices, one_to_many_indices, many_to_many_indices, mistake_duplicates_indices)),]

  }
  
  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #one-many nonperfect matches
  
  # Iterates starting with matches of the closest distance to ensure they are 
  # given priority. 
  for(i in 1:distance){
    x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
    
    # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
    one_to_many <- !(y_dup_indices & x_dup_indices) & (y_dup_indices | x_dup_indices) & close_match_rows_updated[,3] == i
    
    # Check that the same row is not being matched to multiple. If they are 
    # them and they will be handled at the end by being treated as new rows
    matches <- close_match_rows_updated[one_to_many,]
    if(!is.null(nrow(matches))){
      match_x_dup_indices <- (duplicated(matches[,x_df_col])|duplicated(matches[,x_df_col], fromLast=TRUE))
      match_y_dup_indices <- (duplicated(matches[,y_df_col])|duplicated(matches[,y_df_col], fromLast=TRUE))
      discrepancies_indices <- rbind(discrepancies_indices, matches[!(match_y_dup_indices | match_x_dup_indices),])
      error_indices <- rbind(error_indices, matches[(match_y_dup_indices | match_x_dup_indices),])
      
      # remove checked rows and any that have already been matched. 
      close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% matches[,x_df_col]), which(close_match_rows_updated[,y_df_col] %fin% matches[,y_df_col]))),]
    }
  }
  
  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #many-many nonperfect matches

  x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  
  
    # Only many-to-many non perfect matches are left and need to be handled. 
  for(i in 1:distance){
    if(nrow(close_match_rows_updated) > 0){
      many_to_man_with_nonperfect_match <- y_dup_indices & x_dup_indices & (close_match_rows_updated[,3] == i)
      many_to_many_with_nonperfect_match_entries <- close_match_rows_updated[many_to_man_with_nonperfect_match,]
      
      x_dup_indices <- (duplicated(many_to_many_with_nonperfect_match_entries[,x_df_col])|duplicated(many_to_many_with_nonperfect_match_entries[,x_df_col], fromLast=TRUE))
      y_dup_indices <- (duplicated(many_to_many_with_nonperfect_match_entries[,y_df_col])|duplicated(many_to_many_with_nonperfect_match_entries[,y_df_col], fromLast=TRUE))
      
      
      many_to_many_i <- x_dup_indices & y_dup_indices
      one_to_many_i <- !(y_updated_dup_indices & x_updated_dup_indices) & (y_updated_dup_indices | x_updated_dup_indices)
      one_to_one_i <- !(y_updated_dup_indices | x_updated_dup_indices)
      
      
      many_to_many_e <- many_to_many_with_nonperfect_match_entries[many_to_many_i,,drop = FALSE]
      one_to_many_e <- many_to_many_with_nonperfect_match_entries[one_to_many_i,,drop = FALSE]
      one_to_one_e <- many_to_many_with_nonperfect_match_entries[one_to_one_i,,drop = FALSE]
      
      if(sum(nrow(many_to_many_e), nrow(one_to_many_e), nrow(one_to_one_e)) > 0){
        
        
        many_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% many_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% many_to_many_e[,x_df_col])))
        one_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_many_e[,x_df_col])))
        one_to_one_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_one_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_one_e[,x_df_col])))
        
        perfect_duplicate_indices <- rbind(perfect_duplicate_indices, one_to_one_e)
        error_indices <- rbind(error_indices, one_to_many_e, many_to_many_e)
        
        # remove rows that have already been handled to prevent double handling.
        close_match_rows_updated <- close_match_rows_updated[-unique(c(one_to_one_indices, one_to_many_indices, many_to_many_indices)),]
      }
    } else{
      next
    }
  }
  ### ---------- 
  #one-one and one-many nonperfect matches 
  
  # Handling one to many relationship or many to many with discrepancies may  
  # have left a number of one to one or one to many rows. This will be iterative 
  # until none remain or no further matches can be found. Anything left will be assigned to  
  # new entry to ensure that it is processed correctly.
  is_unmatched <- nrow(close_match_rows_updated) > 0 
  while(is_unmatched){
    count <- nrow(close_match_rows_updated)
    
    close_match_rows_updated <- find_one_to_one_matches(close_match_rows)
  
    # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
    for(i in 1:distance){
      if(nrow(close_match_rows_updated) == 0){
        break
      }
      x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
      y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
      one_to_many <- !(y_dup_indices & x_dup_indices) & (y_dup_indices | x_dup_indices) & close_match_rows_updated[,3] == i
      
      # Check that the same row is not being matched to multiple. If they are 
      # them and they will be handled at the end by being treated as new rows
      matches <- close_match_rows_updated[one_to_many,]
      if(nrow(matches) > 0){
        match_x_dup_indices <- (duplicated(matches[,x_df_col])|duplicated(matches[,x_df_col], fromLast=TRUE))
        match_y_dup_indices <- (duplicated(matches[,y_df_col])|duplicated(matches[,y_df_col], fromLast=TRUE))
        discrepancies_indices <- rbind(discrepancies_indices, matches[!(match_y_dup_indices | match_x_dup_indices),])
        error_indices <- rbind(error_indices, matches[(match_y_dup_indices | match_x_dup_indices),])
        
        # remove checked rows and any that have already been matched. 
        close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% matches[,x_df_col]), which(close_match_rows_updated[,y_df_col] %fin% matches[,y_df_col]))),]
        
      }
      is_unmatched <- !((count - nrow(close_match_rows_updated) == 0) | nrow(close_match_rows_updated) == 0)
      
    }
  }
  # check for mistakes 
  is_mistake_present <- check_for_mistake(control_data_type)

  output <- list(discrepancies_indices, perfect_duplicate_indices, error_indices)
  names(output) <- c("discrepancies", "perfect", "error") 
  return(output)
}

check_for_mistake <- function(control_data_type){
  x_df_col <- 1
  y_df_col <- 2
  
  # Check that there are no duplicates between data frames
  elements_count_x <- table(c(unique(discrepancies_indices[,x_df_col]),unique(perfect_duplicate_indices[,x_df_col]),unique(error_indices[,x_df_col])))
  common_x_values <- names(elements_count_x[elements_count_x >= 2])
  
  elements_count_y <- table(c(unique(discrepancies_indices[,y_df_col]),unique(perfect_duplicate_indices[,y_df_col]),unique(error_indices[,y_df_col])))
  common_y_values <- names(elements_count_y[elements_count_y >= 2])
  
  if (any(common_y_values == 0)| any(common_x_values == 0)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows were not correctly separated and the following row indexes were labeled as a `Perfect Duplicate`, `Discrepancy` and/or an `Error`:", 
                     "New Data Indices (Y):", toString(common_y_values), "Legacy Data Indices (X):", toString(common_x_values))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
}

verify_entries <- function(data_df, configuration){
  ID_col <- configuration$metadata$ID_col
  control_data_type <- configuration$metadata$control_data_type
    
  data_df <- verify_integers_positive(data_df)
  data_df <- verify_reef(data_df)
  data_df <- verify_percentages(data_df)
  
  #verify long and lat separately
  data_df <- verify_lat_lng(data_df, max_val=160, min_val=138, columns=c("Longitude", "Start Lng", "End Lng"), ID_col)
  data_df <- verify_lat_lng(data_df, max_val=-5, min_val=-32, columns=c("Latitude", "Start Lat", "End Lat"), ID_col)
  
  if (control_data_type == "manta_tow") {
    
    data_df <- verify_tow_date(data_df)
    data_df <- verify_coral_cover(data_df)
    data_df <- verify_scar(data_df)
    
  } else if (control_data_type == "cull") {
    
    data_df <- verify_voyage_dates(data_df)
    
  } else if (control_data_type == "RHISS") {
    
    data_df <- verify_RHISS(data_df)
    
  } 
  data_df <- verify_na_null(data_df, configuration)
  data_df$error_flag <- as.integer(data_df$error_flag)
  return(data_df)
}

verify_lat_lng <- function(data_df, max_val, min_val, columns, ID_col){
  for (col in columns) {
    if (col %in% colnames(data_df)) {
      values <- data_df[[col]]
      out_of_range <- values < min_val | values > max_val
      
      # Set any NA values to TRUE as the check was unable to be completed 
      # correctly
      out_of_range <- ifelse(is.na(out_of_range),TRUE,out_of_range)
      data_df[["error_flag"]] <- data_df[["error_flag"]] | out_of_range
      
      if (any(out_of_range)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have inappropriate LatLong coordinates:", 
                         toString(data_df[out_of_range, 1]), "and the following indexes", toString((1:nrow(data_df))[out_of_range]))
        message(warning)
        
        # Append the warning to an existing matrix 
        warning_matrix <- matrix(warning)
        contribute_to_metadata_report(control_data_type, warning_matrix)
        
      }
      
    }
  }
  return(data_df)
}


verify_scar <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_scar <- c("a", "p", "c")
  
  tryCatch({
    check_valid_scar <- data_df$`Feeding Scars` %in% valid_scar
    data_df[["error_flag"]] <- data_df[["error_flag"]] | !check_valid_scar 
    
    
    if (any(!check_valid_scar )) {
      grandparent <- as.character(sys.call(sys.parent()))[1]
      parent <- as.character(match.call())[1]
      warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid COTS scar:",
                       toString(data_df[!check_valid_scar , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_valid_scar ]))
      message(warning)
      
      # Append the warning to an existing matrix 
      warning_matrix <- matrix(warning)
      contribute_to_metadata_report(control_data_type, warning_matrix)
      
    }
    
  }, error = function(e) {
    print(paste("Error validating scars:", conditionMessage(e)))
    data_df[["error_flag"]] <- 1 
  })
  
  
  return(data_df)
}

verify_tow_date <- function(data_df){
  # Approximate a tow date based on vessel and voyage if it does not exist. 
  
  tow_date <- data_df[["Tow date"]]
  incomplete_dates <- unique(which(is.na(tow_date)))
  if (length(incomplete_dates) > 0){
    incomplete_date_rows <- data_df[incomplete_dates,]
    vessel_voyage <- unique(incomplete_date_rows[,which(names(incomplete_date_rows) %in% c("Vessel", "Voyage"))])
    for(i in 1:nrow(vessel_voyage)){
      data_df_filtered <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
      is_any_voyage_date_correct <- any(!is.na(data_df_filtered$`Tow date`))
      if(is_any_voyage_date_correct){
        correct_date <- data_df_filtered[!(is.na(data_df_filtered$`Tow date`) | is.null(data_df_filtered$`Tow date`)),][1,"Tow date"]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Tow date`)), "Tow date"] <- correct_date
      } else if(!is_any_voyage_date_correct & !is_any_survey_date_correct) {
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Tow date`)), c("error_flag")] <- 1
        
      }
    }
    
  }

  
  
  post_estimation_tow_dates <- data_df[["Tow date"]]
  na_present <- (is.na(post_estimation_tow_dates) | is.null(post_estimation_tow_dates)) & (is.na(tow_date) | is.null(tow_date))
  dated_estimated <- !(is.na(post_estimation_tow_dates) | is.null(post_estimation_tow_dates)) & (is.na(tow_date) | is.null(tow_date))
  
  if (any(dated_estimated | na_present)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- c()
    if (any(dated_estimated)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have their tow date estimated from their vessel",
                       toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning1)
      warning <- c(warning, warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have no tow date",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning2)
      warning <- c(warning, warning2)
    }  
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(c(warning)))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  return(data_df)
}


verify_RHISS <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_tide <- c("L", "M", "H")
  check_tide <- data_df$`Tide` %in% valid_tide
  
  cols <- c("Slime Height (cm)", "Entangled/Mat-Like Height (cm)", "Filamentous Height (cm)", "Leafy/Fleshy Height (cm)", "Tree/Bush-Like Height (cm)")
  valid_macroalgae <- c("A", "B", "C", "0", "1", "2", "3")
  is_valid_macroalgae <- apply(data_df[, cols], 2, function(x) !(x %in% valid_macroalgae))
  check_macroalgae <- rowSums(is_valid_macroalgae) > 0
  
  valid_descriptive_bleach_severity <- c("Totally bleached white", "pale/fluoro (very light or yellowish)", "None", "Bleached only on upper surface", "Pale (very light)/Focal bleaching", "Totally bleached white/fluoro", "Recently dead coral lightly covered in algae")
  bcols <- c("Mushroom Bleach Severity", "Massive Bleach Severity", "Encrusting Bleach Severity", "Vase/Foliose Bleach Severity", "Plate/Table Bleach Severity", "Bushy Bleach Severity", "Branching Bleach Severity")
  is_valid_descriptive_bleach_severity <- apply(data_df[, bcols], 2, function(x) !(x %in% valid_descriptive_bleach_severity))
  check_descriptive_bleach_severity <- rowSums(is_valid_descriptive_bleach_severity) > 0
  
  bleached_severity <- data_df$`Bleached Average Severity Index (calculated via matrix)`
  bleached_severity <- ifelse(is.na(bleached_severity),TRUE,bleached_severity)
  check_bleach_severity <- bleached_severity >= 1 & bleached_severity <= 8
  
  check <- !check_tide | check_macroalgae | !check_bleach_severity | check_descriptive_bleach_severity
  if (any(check )) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    if (any(!check_tide)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid tide values:",
                       toString(data_df[!check_tide , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_tide]))
      message(warning1)
    }
    if (any(check_macroalgae)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid macroalgae values:",
                       toString(data_df[check_macroalgae, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_macroalgae]))
      message(warning2)
    }
    if (any(!check_bleach_severity)) {
      warning3 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid bleach severity values:",
                       toString(data_df[!check_bleach_severity, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_bleach_severity]))
      message(warning3)
    }
    if (any(check_descriptive_bleach_severity)) {
      warning4 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid descriptive beach severity:",
                       toString(data_df[check_descriptive_bleach_severity , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_descriptive_bleach_severity ]))
      message(warning4)
    }
    
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(c(warning1,warning2,warning3,warning4)))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  data_df[["error_flag"]] <- data_df[["error_flag"]] | check 
  
  
  return(data_df)
}


verify_percentages <- function(data_df) {
  # Check that all percentages in a row are between 0 and 100
  perc_cols <- grep("%", colnames(data_df))
  if(length(perc_cols) > 0){
    perc_cols_vals <- data_df[, perc_cols]
    col_check <- apply(perc_cols_vals, 2, function(x) x < 0 | x > 100)
    check <- rowSums(col_check) > 0
    check <- ifelse(is.na(check),TRUE,check)
    data_df[["error_flag"]] <- data_df[["error_flag"]] | check
    if (any(check)) {
      grandparent <- as.character(sys.call(sys.parent()))[1]
      parent <- as.character(match.call())[1]
      warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have percentages in an invalid format:",
                       toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
      message(warning)
      
      # Append the warning to an existing matrix 
      warning_matrix <- matrix(warning)
      contribute_to_metadata_report(control_data_type, warning_matrix)
      
    }
  }
  return(data_df)
}


verify_na_null <- function(data_df, configuration) {
  # check if there are any values that are NA or NULL and flag those rows as an 
  # error. This does not include new additional columns as they are assigned a
  # default value at the end of the verification process or ID column.
  ID_col <- configuration$metadata$ID_col
  control_data_type <- configuration$metadata$control_data_type
  
  exempt_cols <- intersect(c(configuration$mappings$new_fields$field, ID_col), names(data_df))
  na_present <- apply(data_df[, -exempt_cols], 2, function(x) is.na(x) | is.null(x) | x == "")
  check <- rowSums(na_present) > 0
  check <- ifelse(is.na(check),TRUE,check)
  data_df[["error_flag"]] <- data_df[["error_flag"]] | check
  if (any(check)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have missing data:",
                     toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  return(data_df)
}

verify_integers_positive <- function(data_df) {
  # R function that verifys all integers are positive values as they represent 
  # real quantities. Note: Whole numbers are not integers, they must be declared
  # as such. All relevant columns were set as integers in the set_data_type 
  # function
  
    is_integer <- is.integer(data_df[1,])
    if(any(is_integer)){
      col_check <- apply(data_df[,is_integer], 2, function(x) x < 0)
      check <- rowSums(col_check) > 0
      check <- ifelse(is.na(check),TRUE,check)
      data_df[, "error_flag"] <- data_df[, "error_flag"] | check
     
      if (any(check)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have non-positive integer values:",
                         toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
        message(warning)
        
        # Append the warning to an existing matrix 
        warning_matrix <- matrix(warning)
        contribute_to_metadata_report(control_data_type, warning_matrix)
      }
    }
  return(data_df)
}

remove_leading_spaces <- function(data_df) {
  # R function that removes leading and trailing spaces from all entries in a 
  # data frame column
  cols <- colnames(data_df)
  for (col_name in cols) {
    is_character <- is.character(data_df[[col_name]])
    if(any(is_character)){
      data_df[is_character, col_name] <- gsub("^\\s+|\\s+$", "", data_df[is_character, col_name])
    } 
  }
  return(data_df)
}


verify_coral_cover <- function(data_df) {
  accepted_values <- c("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", "5+", "0")
  
  # Identify possible corrections based on common mistakes
  possible_corrections <- data.frame(
    Old_Value = c("-1", "-2", "-3", "-4", "-5","+1", "+2", "+3", "+4", "+5"),
    New_Value = c("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", "5+")
  )
  
  # Substitute similar characters for correct ones
  data_df[["Hard Coral"]] <- gsub("—|–|−", "-", data_df[["Hard Coral"]])
  data_df[["Soft Coral"]] <- gsub("—|–|−", "-", data_df[["Soft Coral"]])
  data_df[["Recently Dead Coral"]] <- gsub("—|–|−", "-", data_df[["Recently Dead Coral"]])
  
  # Loop through possible corrections and replace values
  for (i in 1:nrow(possible_corrections)) {
    old_value <- possible_corrections$Old_Value[i]
    new_value <- possible_corrections$New_Value[i]
    data_df$`Hard Coral`[data_df$`Hard Coral` == old_value] <- new_value
    data_df$`Soft Coral`[data_df$`Soft Coral` == old_value] <- new_value
    data_df$`Recently Dead Coral`[data_df$`Recently Dead Coral` == old_value] <- new_value
  }
  
  hc_check <- data_df$`Hard Coral` %in% accepted_values
  sc_check <- data_df$`Soft Coral` %in% accepted_values
  rdc_check <- data_df$`Recently Dead Coral` %in% accepted_values
  
  error <- !hc_check | !sc_check | !rdc_check
  
  data_df[["error_flag"]] <- data_df[["error_flag"]] | error
  if (any(error)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid coral cover values:",
                     toString(data_df[error , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[error]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  return(data_df)
}


verify_cots_scars <- function(data_df) {
  # check that the cots scars are an expected metric
  
  valid_values <- c("a", "p", "c")
  check <- data_df$`COTS Scars` %in% valid_values
  data_df[["error_flag"]] <- data_df[["error_flag"]] | !check
  return(data_df)
}

verify_reef <- function(data_df){
  # Check that the reef ID is in one of the correct standard formats with regex.
  # Look for most similar reef ID if it is not. Am not checking for a match 
  # because that would mean no new reefs would be accepted and I believe that 
  # the reef input is restricted to existing reefs so it is unlikley to be a typo
  
  reef_id <- data_df[["Reef ID"]]
  correct_reef_id_format <- grepl("^(1[1-9]|2[0-9])-\\d{3}[a-z]?$", reef_id)
  data_df[,"error_flag"] <- data_df[,"error_flag"] | !correct_reef_id_format
  if (any(!correct_reef_id_format)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid Reef IDs:",
                     toString(data_df[!correct_reef_id_format , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!correct_reef_id_format]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  return(data_df)
}

verify_voyage_dates <- function(data_df){
  # Check that voyage dates of observation are within in voyage dates and that 
  # none of the dates are NA. If Voyage dates are NA set start and end to min 
  # and max observation date. Check that voyage dates associated with a vessels 
  # voyage are unique (There should only be on departure and return date)
  voyage_start <- data_df[["Voyage Start"]]
  voyage_end <- data_df[["Voyage End"]]
  
  incomplete_dates <- unique(c(which(is.na(voyage_end)), which(is.na(voyage_start))))
  if (length(incomplete_dates) > 0){
    incomplete_date_rows <- data_df[incomplete_dates,]
    vessel_voyage <- unique(incomplete_date_rows[,which(names(incomplete_date_rows) %in% c("Vessel", "Voyage"))])
    for(i in 1:nrow(vessel_voyage)){
      data_df_filtered <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
      is_any_voyage_date_correct <- any(!is.na(data_df_filtered$`Voyage Start`) & !is.na(data_df_filtered$`Voyage End`))
      is_any_survey_date_correct <- any(!is.na(data_df_filtered$`Survey Date`))
      if(is_any_voyage_date_correct){
        correct_dates <- data_df_filtered[!is.na(data_df_filtered),][1,c("Voyage Start", "Voyage End")]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`)), "Voyage Start"] <- correct_dates[1]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage End`)), "Voyage End"] <- correct_dates[2]
      } else if(!is_any_voyage_date_correct & is_any_survey_date_correct){
        incomplete_date_rows_filtered <- incomplete_date_rows[(incomplete_date_rows$Vessel == vessel_voyage[i,1]) & (incomplete_date_rows$Voyage == vessel_voyage[i,2]),]
        estimate_start <- min(incomplete_date_rows_filtered$`Survey Date`)
        estimate_end <- min(incomplete_date_rows_filtered$`Survey Date`)
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`)), c("Voyage Start")] <-  estimate_start
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage End`)), c("Voyage End")] <-  estimate_end
      } else if(!is_any_voyage_date_correct & !is_any_survey_date_correct) {
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`) | is.na(data_df$`Voyage End`)), "error_flag"] <- 1
      }
    }
   
  }
  
  
  post_voyage_start <- data_df[["Voyage Start"]]
  post_voyage_end <- data_df[["Voyage End"]]
  na_present <- ((is.na(post_voyage_start) | is.null(post_voyage_start)) | (is.na(post_voyage_end) | is.null(post_voyage_end))) & ((is.na(voyage_start) | is.null(voyage_start)) | (is.na(voyage_end) | is.null(voyage_end)))
  dated_estimated <- !((is.na(post_voyage_start) | is.null(post_voyage_start)) | (is.na(post_voyage_end) | is.null(post_voyage_end))) & ((is.na(voyage_start) | is.null(voyage_start)) | (is.na(voyage_end) | is.null(voyage_end)))
  
  if (any(dated_estimated | na_present)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- c()
    if (any(dated_estimated) & !any(na_present)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have their voyage dates estimated from their vessel",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning1)
      warning <- c(warning, warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have voyage dates",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning2)
      warning <- c(warning, warning2)
    }  
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(warning))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  
  data_df$error_flag <- data_df$error_flag | (data_df$`Survey Date` < data_df$`Voyage Start` | data_df$`Survey Date` > data_df$`Voyage End`)
  
  vessel_voyage <- unique(data_df[,which(names(data_df) %in% c("Vessel", "Voyage"))])
  for (i in 1:nrow(vessel_voyage)){
    filtered_data_df <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
    start_dates <- unique(filtered_data_df$`Voyage Start`) 
    end_dates <- unique(filtered_data_df$`Voyage End`)
    if(length(start_dates) > 1){
      mf_start <- names(sort(table(start_dates), decreasing = TRUE)[1])
      data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]), "Voyage Start"] <- mf_start
    }
    if(length(end_dates) > 1){
      mf_end <- names(sort(table(end_dates), decreasing = TRUE)[1])
      data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]), "Voyage End"] <- mf_end
    }
  }
  
  return(data_df)
}

find_one_to_one_matches <- function(close_match_rows){
  
  x_df_col <- 1
  y_df_col <- 2
  
  # Determine duplicates of the row indices.
  x_dup_indices <- (duplicated(close_match_rows[,x_df_col])|duplicated(close_match_rows[,x_df_col], fromLast=TRUE))
  y_dup_indices <- (duplicated(close_match_rows[,y_df_col])|duplicated(close_match_rows[,y_df_col], fromLast=TRUE))
  dup_indices <- y_dup_indices|x_dup_indices
  non_dup_indices <- !dup_indices
  
  # Add rows matches with only a single to relevant matrix.
  perfect_duplicate_indices <<- rbind(perfect_duplicate_indices, close_match_rows[non_dup_indices & close_match_rows[,3] == 0,])
  discrepancies_indices <<- rbind(discrepancies_indices, close_match_rows[non_dup_indices & close_match_rows[,3] > 0,])
  
  # remove rows that have already been handled to prevent double handling.
  close_match_rows_updated <- close_match_rows[dup_indices,]
  return(close_match_rows_updated)
}


rec_group <- function(m2m_split, groups, group){
  # That is recursive with the purpose of grouping sets of matching rows so that 
  # it can be determined whether or not they are mistakes or coincidental 
  # perfect matches.  
  group <- group + 1
  stack <- m2m_split[[group]][,1]
  names <- names(m2m_split)
  for(i in 1:length(names)){
    if(identical(stack,m2m_split[[names[i]]][,1])){
      groups[[group]] <- c(groups[[group]], m2m_split[[names[i]]][1,2])
    }
    
  }
  if(length(m2m_split) != group){
    return(rec_group(m2m_split, groups, group))
  } else {
    return(groups)
  }
}

# Re-write base operator %in% faster
`%fin%` <- function(x, table) {
  stopifnot(require(fastmatch))
  fmatch(x, table, nomatch = 0L) > 0L
}

matrix_close_matches_vectorised <- function(x, y, distance){
  # Find list of all close matches between rows in x and y within a specified 
  # distance. This distance is the number of non perfect column matches within
  # a row. returns a the indices of the rows matched and the distance from
  # perfect. (X_index, Y_index, Distance). Pre-allocates memory for the matrix
  # assuming worst case scenario or maximum allocation possible. The operations 
  # would not be possible if this fails and is still faster than 
  # dynamically updating an object. 
  
  #Pre-allocate variables and memory
  x_rows <- nrow(x)
  x_cols <- ncol(x)
  y_rows <- nrow(y)
  
  num_rows <- y_rows*x_rows
  if(num_rows > 1000000000){
    num_rows <- 1000000000 
  }
  match_indices <- try(matrix(data=NA, nrow=num_rows, ncol=3))
  if(class(num_rows)[[1]]=='try-error'){
    num_rows <- 10000000
    match_indices <- matrix(data=NA, nrow=num_rows, ncol=3)
  }
  index <- 1
  
  # Iterate through each row in matrix/dataframe x and evaluate for each value 
  # in the row whether it matches the column of y. This vector of logical values
  # are then appeneded to the `matches` matrix. After Iterating over every 
  # column there will be a matrix of size (y_rows, x_cols). A perfect matching 
  # row in y will have a corresponding row in `matches` exclusively containing 
  # TRUE. Given TRUE = 1, a row sum can be utilised to determine the distance 
  # from a perfect match. Can then use a customised vectorised function to 
  # append matches to match_indices. 
  matches <- matrix(data=NA, nrow=y_rows, ncol=x_cols)
  for(z in 1:x_rows){ 
    for(i in 1:x_cols){
      matches[,i] <- x[z,i] == y[,i]
    }
    matches <- !(matches)
    num_matches <- rowSums(matches, dims = 1)
    num_matches <- ifelse(num_matches <= distance, num_matches, NA)
    nonNA <- which(!is.na(num_matches))
    nonNAvalues <- num_matches[nonNA]
    if(length(nonNAvalues) >= 1){
      match <- store_index_vec(nonNAvalues, nonNA, z)
      match <- t(match)
      match_indices[index:(index+nrow(match)-1),] <- match
      index <- index + nrow(match)
    }
  }
  match_indices <- na.omit(match_indices)
  return(match_indices)
}


store_index <- function(nonNAvalues, nonNA, z){
  match_indices <- c(z, nonNA, nonNAvalues)
  return(match_indices)
}
store_index_vec <- base::Vectorize(store_index)

find_previous_process_date <- function(){
  # finds the dates stored in the metadata report file names with REGEX. 
  # reports_location is a global variable defined when creating the meta data 
  # report. 
  xml_files <- list.files(path= reports_location, pattern = as.character("Processed Control Data Report "))
  dates <- regmatches(xml_files, regexpr("[0-9-]{10}", xml_files))
  if(length > 0) {
    return(max(as.Date(dates)))
  } else {
    return(NA)
  }
 
}

get_file_and_line <- function() {
  # can get the information of where warnings were generated approximately
  
  frame <- sys.frame(n = 1)
  info <- rlang::trace_back(frame, x = FALSE)
  file <- attr(info, "file")
  line <- attr(info, "line")
  return(list(file = file, line = line))
}

set_data_type <- function(data_df, mapping){
  # sets the data_type of each column of any data frame input based on
  # configuration file
  
  output_df <- data_df
  for (i in seq_len(nrow(mapping))) {
    column_name <- mapping$field[i]
    data_type <- mapping$data_type[i]
    
    # Convert the column to the specified data type
    if(tolower(data_type) == "date"){
      output_df[[column_name]] <- parse_date_time(data_df[[column_name]], orders = c('dmy', 'ymd', '%d/%b/%Y %I:%M:%S %p', '%Y/%b/%d %I:%M:%S %p', '%I:%M:%S'))
    } else {
      output_df[[column_name]] <- as(data_df[[column_name]], tolower(data_type))
    }
    
  }
  
  # Identify which rows have been coerced to NA and listed in metadata report. 
  original_has_na <- apply(data_df, 2, function(x) is.na(x))
  conversion_has_na <- apply(output_df, 2, function(x) is.na(x))
  coerced_na <- rowSums(original_has_na) < rowSums(conversion_has_na)
  
  if(any(coerced_na)){
    
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    indexes <- (1:length(coerced_na))
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have had value(s) coerced to NA:",
                     paste(data_df[coerced_na, 1], collapse = ", "), "and the following indexes", paste(indexes[coerced_na], collapse = ", "))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  
  # remove trailing and leading spaces from strings for comparison. 
  output_df <- remove_leading_spaces(output_df)
  return(output_df)
}
  

transform_data_structure <- function(data_df, mappings, new_fields){
  
  transformed_df <- data.frame(matrix(ncol = nrow(mappings) + nrow(new_fields), nrow = nrow(data_df)))
  colnames(transformed_df) <- c(mappings$target_field, new_fields$field)
  transformed_df <- transformed_df[, c(mappings$position, new_fields$position)]
  
  data_colnames <- colnames(data_df)
  is_already_mapped <- all(!is.na(match(mappings$target_field, data_colnames)))
  if(is_already_mapped){
    col_indices <- match(data_colnames, mappings$target_fields)
    positions <- mappings$position[col_indices]
    transformed_df <- data_df[,positions]
    return(transformed_df)
  }
  
  for (i in seq_len(nrow(new_fields))) {
    new_field <- new_fields$field[i]
    default_value <- new_fields$default[i]
    position <- new_fields$position[i]
    transformed_df[, position] <- default_value
    colnames(transformed_df)[position] <- new_field
    
  }
  closest_matches <- get_closest_matches(colnames(data_df), mappings$source_field)
  for (i in seq_len(ncol(closest_matches))) {
    index <- match(closest_matches[2,i], mappings$source_field)
    position <- mappings$position[index]
    mapped_name <- mappings$target_field[index]
    colnames(transformed_df)[position] <- mapped_name 
    transformed_df[, position] <- data_df[[closest_matches[1,i]]]
  }
  
 
  return(transformed_df)
}


get_closest_matches <- function(sources, targets){
    
  # perform swap to ensure that sources is longer than targets
  is_targets_longer <- length(targets) > length(sources)
  if(is_targets_longer){
    temp <- targets
    targets <- sources
    sources <- temp
  }  

  transformed_sources <- matrix(0, nrow = 2, ncol = min(length(sources), length(targets)))
  levenshtein_distances <- adist(sources , targets)
  smallest_source_distances <- apply(levenshtein_distances, 1, min)
  smallest_source_indices <- which(rank(smallest_source_distances, ties.method = "min") <= length(targets))
  smallest_target_indices <- apply(levenshtein_distances, 1, which.min)
  transformed_sources[1,] <- sources[smallest_source_indices]
  transformed_sources[2,] <- targets[smallest_target_indices[smallest_source_indices]]
  
  if(is_targets_longer){
    transformed_sources[c(1, 2), ] <- transformed_sources[c(2, 1), ]
  }  
  return(transformed_sources)
}

assign_nearest_method_c <- function(kml_data, data_df, layer_names_vec, crs, raster_size=0.0005, x_closest=1, is_standardised=1, save_rasters=1){
  # Assign nearest sites to manta tows with method developed by Cameron Fletcher
  
  sf_use_s2(FALSE)
  pts <- get_centroids(data_df, crs)
  kml_data_simplified <- simplify_reef_polyogns_rdp(kml_data)
  site_regions <- assign_raster_pixel_to_sites(kml_data_simplified, layer_names_vec, crs, raster_size, x_closest, is_standardised)
  
  tryCatch({
    if(save_rasters){
      for(i in 1:length(site_regions)){
        file_name <- names(site_regions[i])
        modified_file_name <- gsub("/", "_", file_name)
        writeRaster(site_regions[[i]], filename = paste("Cameron Method\\",raster_size,"\\",modified_file_name,".tif", sep=""), format = "GTiff", overwrite = TRUE)
      }
    }
  }, error = function(e) {
    cat("An error occurred while saving", modified_file_name, "\n")
  })
  
  updated_pts <- pts
  for(i in 1:length(site_regions)){
    is_contained <- sapply(pts$`Reef Number`, function(str) grepl(str, names(site_regions[i])))
    if(any(is_contained) == FALSE){
      next
    }
    reef_pts <- pts[is_contained,]
    nearest_site_manta_data <- raster::extract(site_regions[[i]], reef_pts)
    updated_pts[is_contained, "Nearest Site"] <- nearest_site_manta_data
  }
  return(updated_pts)
}

get_centroids <- function(data_df, crs, precision=0){
  # Determine the centroid of the manta tow and create geospatial points
  
  data_df <- data_df %>%
    mutate(
      mean_lat = (`Start Lat` + `End Lat`) / 2,
      mean_long = (`Start Lng` + `End Lng`) / 2
    )
  
  if(precision != 0){
    data_df <- data_df %>%
      mutate_at(vars(`Start lat`, `Start long`, `End lat`, `End long`, `mean_lat`, `mean_long`), ~ round(., precision))
  }
  
  #create manta_tow points
  is_coord_na <- !is.na(data_df$mean_lat) & !is.na(data_df$mean_long)
  data_filtered <- data_df[is_coord_na, ]
  pts <- st_as_sf(data_filtered, coords=c("mean_long", "mean_lat"), crs=crs)
  return(pts)
}

assign_raster_pixel_to_sites <- function(kml_data, layer_names_vec, crs, raster_size, x_closest=1, is_standardised=0){
  # This is a method of assigning sites to manta tows that was initially 
  # implemented in Mathmatica by Dr Cameron Fletcher. A set of rasters are 
  # created slightly larger than the bounding box of each layer in the KML file.
  # The rasters are convereted into a matrix of geospatial "POINTS" to compare 
  # the distance between each point and the sites on the corresponding layer. 
  # Each raster pixel will then be assigned a value corresponding with the 
  # nearest site (geodesic distance) 
  
  if(is_standardised){
    expanded_extent <- standardise_extents(kml_data)
  } else {
    expanded_extent <- list()
    for(i in 1:length(kml_data)) {
      expanded_extent[[i]] <- extent(kml_data[[i]])
    }
  }
  
  # Define the increase amount in both x and y directions. 
  increase_amount <- 0.003
  expanded_bboxs <- setNames(lapply(expanded_extent, function(i) {
    
    original_extent <- i
    # Increase the extent by the specified amount
    expanded_bboxs <- extent(original_extent[1] - increase_amount,
                             original_extent[2] + increase_amount,
                             original_extent[3] - increase_amount,
                             original_extent[4] + increase_amount)
    
  }), layer_names_vec)
  
  # Create an empty raster based on the bounding box, cell size, and projection
  rasters <- create_raster_templates(expanded_bboxs, layer_names_vec, crs, raster_size)
  
  # The original script in mathmatica utilised a function "RegionDistance". This 
  # finds the Euclidean distance between the polygon and a point and does not
  # consider the curviture of the earth. 
  site_regions <- setNames(vector("list", length=length(rasters)),layer_names_vec)
  
  for (i in seq_along(rasters)) {
    raster <- rasters[[i]]
    site_poly <- kml_data[[i]]
    
    # set all values of the raster layer so every pixel can be exported to a 
    # dataframe and then converted to points 
    values(raster) <- 1
    pixel_coords <- rasterToPoints(raster)
    pixel_coords <- pixel_coords[,1:2]
    raster_points <- pixel_coords |> as.matrix() |> st_multipoint() |> st_sfc() |> st_cast('POINT')
    st_crs(raster_points) <- crs
    
    # Get site numbers
    site_names <- site_poly$Name
    site_numbers <- site_names_to_numbers(site_names)
    
    # Can determine the Euclidean and geodesic distance (st_distance)
    distances <- st_distance(site_poly, raster_points)
    if (dim(distances)[1] != 1){
      
      distances <- apply(distances, 2, as.numeric)
      min_distances_list <- apply(distances, 2, xth_smallest, x_values=x_closest)
      min_distances_df <- do.call(rbind, min_distances_list)
      min_distance_site_numbers <- site_numbers[as.vector(min_distances_df$`Nearest Site`)]
      min_distances <- min_distances_df$`distance`
      
    } else {
      min_distances <- drop_units(distances)
      min_distance_site_numbers <- rep(site_numbers, length(min_distances))
    }
    
    # Set site numbers to NA if they are more than 300m away. 
    is_within_300 <- min_distances > 300
    min_distance_site_numbers[is_within_300] <- 0
    min_distances[is_within_300] <- 0
    values(raster) <- min_distance_site_numbers
    names(raster) <- c("Nearest Site")
    
    site_regions[[i]] <- raster
  }
  
  return(site_regions)
}

site_names_to_numbers <- function(site_names){
  return(as.numeric(sub(".+_(\\d+)$", "\\1", site_names)))
}

simplify_reef_polyogns_rdp <- function(kml_data){
  # simplify all reef polygons stored in a list that was retrieved from the kml
  # file with the Ramer-Douglas-Peucker algorithm
  
  simplified_kml_data <- kml_data
  for (j in 1:length(kml_data)){
    reef_geometries <- kml_data[[j]][[3]]
    reef_geometries_updated <- reef_geometries
    for(i in 1:length(reef_geometries)){
      # A vast majority of reef_geometries at this level are polygons but 
      # occasionally they are geometrycollections and require iteration. 
      
      site_polygon <- reef_geometries[[i]]
      if (class(site_polygon[[2]])[2] == "GEOMETRYCOLLECTION"){
        for(k in 1:length(site_polygon[[2]])){
          polygon_points <- site_polygon[[2]][[k]][[1]]
          approx_polygon_points <- polygon_rdp(polygon_points)
          site_polygon[[2]][[k]][[1]] <- approx_polygon_points
        }
      } else {
        polygon_points <- site_polygon[[2]][[1]]
        approx_polygon_points <- polygon_rdp(polygon_points)
        site_polygon[[2]][[1]] <- approx_polygon_points
      }
      reef_geometries_updated[[i]] <- site_polygon
    }
    simplified_kml_data[[j]][[3]] <- reef_geometries_updated
    
  }
  return(simplified_kml_data)
}

polygon_rdp <- function(polygon_points, epsilon=0.00001) {
  # adaptation of the Ramer-Douglas-Peucker algorithm. The original algorithm 
  # was developed for a use with a line not a ploygon. Remove the last point 
  # temporarily, perform the algorithm and then return the value.
  
  line_points <- polygon_points[1:nrow(polygon_points)-1,]
  approx_line_points <- rdp(line_points, epsilon)
  polygon <- rbind(approx_line_points, approx_line_points[1,])
  return(polygon)
}

rdp <- function(points, epsilon=0.00001) {
  # Ramer-Douglas-Peucker algorithm s
  if (nrow(points) <= 2) {
    return(points)
  }
  
  dmax <- 0
  index <- 0
  end <- nrow(points)
  
  # Find the point with the maximum distance
  for (i in 2:(end - 1)) {
    d <- perpendicularDistance(points[i,], points[1,], points[end,])
    if (d > dmax) {
      index <- i
      dmax <- d
    }
  }
  
  
  result <- matrix(nrow = 0, ncol = ncol(points))
  # If max distance is greater than epsilon, recursively simplify
  if (dmax > epsilon) {
    recursive1 <- rdp(points[1:index,], epsilon)
    recursive2 <- rdp(points[(index):end,], epsilon)
    result <- rbind(result, rbind(recursive1[1:nrow(recursive1) - 1,], recursive2))
  } else {
    result <- rbind(points[1,], points[end,])
  }
  
  return(result)
}

# Calculate perpendicular distance of a point p from a line segment AB
perpendicularDistance <- function(p, A, B) {
  numerator <- abs((B[2] - A[2]) * p[1] - (B[1] - A[1]) * p[2] + B[1] * A[2] - B[2] * A[1])
  denominator <- sqrt((B[2] - A[2])^2 + (B[1] - A[1])^2)
  result <- (numerator / denominator)
  return(result)
}












