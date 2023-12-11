# Format the new control data into the stardard legacy format. ONLY REQUIRED IF 
# rio::import() depreciates. 
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
        base::message(paste("Cannot read from:", data))
        base::message("Original error message:")
        base::message(cond)
      }
    ) 
  return(out)
}

contribute_to_metadata_report <- function(key, data, parent_key=NULL, report_path=NULL){
  if(is.null(report_path)){
    report_path <- find_recent_file("Output\\reports", "Report", "json")
  }
  if (file.exists(report_path)) {
    report <- fromJSON(report_path)
  } else {
    print("report does not exist")
    return(NULL)
  }
  if(!is.null(parent_key)){
    report <- report[[parent_key]]
  }
  report[[key]] <- data
  toJSON(report, pretty = TRUE, auto_unbox = TRUE) %>% writeLines(report_path)
}

separate_control_dataframe <- function(new_data_df, legacy_data_df, control_data_type){

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
    # from comparison. Even when "Nearest Site" exists in the legacy version we 
    # do not want to compare it to see if it has changed. It is recalculated 
    # every single workflow run through based on the available KML file.
    required_columns <- intersect(c(configuration$mappings$new_fields$field, ID_col), names(new_data_df))
    
    # find close matching rows (distance of two) based on all columns except ID. ID is not 
    # because it will always be null if the data is exported from powerBI. 
    distance <- 3
    
    temp_new_df <- new_data_df[ , -which(names(new_data_df) %in% required_columns)]
    temp_legacy_df <- legacy_data_df[ , -which(names(legacy_data_df) %in% required_columns)]
    close_match_rows <- matrix_close_matches_vectorised(temp_legacy_df, temp_new_df, distance)
    
    # There can be many to many perfect matches. This means that there will be 
    # multiple indices referring to the same row for perfect duplicates. 
    # unique() should be utilised when subsetting the input dataframes.
    separated_close_matches <- vectorised_separate_close_matches(close_match_rows)
    perfect_duplicates <- new_data_df[unique(separated_close_matches$perfect[,2]),]
    new_entries_i <- unique(c(separated_close_matches$discrepancies[,2],separated_close_matches$perfect[,2]))
    
    # This will contain any new entries and any rows that could not be separated
    new_entries <- new_data_df[-new_entries_i,]
    
  }
  
  # Given that it is not possible to definitively know if a change / discrepancy 
  # was intentional or not both new and change entries will pass through the 
  # same validation checks and if passed will be accepted as usable and assumed to be . If failed, 
  # assumed to be a QA change. If failed,  the data will be flagged. Failed 
  # discrepancies will check the original legacy entry, which if failed will 
  # be left as is. 
 
  verified_data_df <- rbind(verified_data_df, perfect_duplicates)
  verified_discrepancies <- compare_discrepancies(new_data_df, legacy_data_df, separated_close_matches$discrepancies)
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
  duplicates <- duplicated(new_data_df$Identifier)
  counts <- ave(duplicates, new_data_df$Identifier, FUN = sum)
  is_duplicate <- (counts >= 2 & duplicates)
  new_data_df$error_flag <- ifelse(is_duplicate, 1, new_data_df$error_flag)
  new_data_df$Identifier <- NULL
  
  if (any(is_duplicate)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have been flagged as duplicates", 
                     toString(data_df[is_duplicate, 1]), "and the following indexes", toString((1:nrow(data_df))[is_duplicate]))
    base::message(warning)
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[is_duplicate, 1],
        index = (1:nrow(data_df))[is_duplicate],
        message = "flagged as duplicates"
      )
      contribute_to_metadata_report("Duplicates", warnings, parent_key = "Warning")
    }
  }
  return(new_data_df)
}


compare_discrepancies <- function(new_data_df, legacy_data_df, discrepancies){
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


vectorised_separate_close_matches <- function(close_match_rows){
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
    base::message(warning)
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        index_new = common_y_values,
        index_legacy = common_x_values,
        message = "Row with listed indices were incorrectly separated"
      )
      contribute_to_metadata_report("Separation", warnings, parent_key = "Warning")
    }
    
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
      out_of_range <- ifelse(is.na(out_of_range), FALSE, out_of_range)
      data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | out_of_range)
      
      if (any(out_of_range)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have inappropriate LatLong coordinates:", 
                         toString(data_df[out_of_range, 1]), "and the following indexes", toString((1:nrow(data_df))[out_of_range]))
        base::message(warning)
        
        if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
          # Append the warning to an existing matrix 
          warnings <- data.frame(
            ID = data_df[out_of_range, 1],
            index = (1:nrow(data_df))[out_of_range],
            message = "Inappropriate Lat Long values"
          )
          contribute_to_metadata_report("Coordinates", warnings, parent_key = "Warning")
        }
        
      }
      
    }
  }
  return(data_df)
}


verify_scar <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_scar <- c("a", "p", "c")
  

    check_valid_scar <- data_df$`Feeding Scars` %in% valid_scar
    out_of_range <- ifelse(is.na(check_valid_scar), TRUE, check_valid_scar)
    data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | !check_valid_scar)
    
    
    if (any(!check_valid_scar )) {
      grandparent <- as.character(sys.call(sys.parent()))[1]
      parent <- as.character(match.call())[1]
      warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid COTS scar:",
                       toString(data_df[!check_valid_scar , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_valid_scar ]))
      base::message(warning)
      
      
      if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
        # Append the warning to an existing matrix 
        warnings <- data.frame(
          ID = data_df[!check_valid_scar, 1],
          index = (1:nrow(data_df))[!check_valid_scar],
          message = "Invalid COT Scar"
        )
        contribute_to_metadata_report("Scar", warnings, parent_key = "Warning")
      }
      
    }

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
      base::message(warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have no tow date",
                        toString(data_df[na_present , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[na_present]))
      base::message(warning2)
    }  
    
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[dated_estimated, 1],
        index = (1:nrow(data_df))[dated_estimated],
        message = "Invalid Tow Date. Date was successfully estimated."
      )
      contribute_to_metadata_report("Estimated Tow Date", warnings, parent_key = "Warning")
      warnings <- data.frame(
        ID = data_df[na_present, 1],
        index = (1:nrow(data_df))[na_present],
        message = "Invalid Tow Date. "
      )
      contribute_to_metadata_report("Invalid Tow Date", warnings, parent_key = "Warning")
    }
    
  }
  return(data_df)
}


verify_RHISS <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_tide <- c("L", "M", "H")
  check_tide <- data_df$`Tide` %in% valid_tide
  check_tide <- ifelse(is.na(check_tide), TRUE, check_tide)
  
  cols <- c("Slime Height (cm)", "Entangled/Mat-Like Height (cm)", "Filamentous Height (cm)", "Leafy/Fleshy Height (cm)", "Tree/Bush-Like Height (cm)")
  valid_macroalgae <- c("A", "B", "C", "0", "1", "2", "3")
  is_valid_macroalgae <- apply(data_df[, cols], 2, function(x) !(x %in% valid_macroalgae))
  is_valid_macroalgae <- ifelse(is.na(is_valid_macroalgae), FALSE, is_valid_macroalgae)
  check_macroalgae <- rowSums(is_valid_macroalgae) > 0
  
  valid_descriptive_bleach_severity <- c("Totally bleached white", "pale/fluoro (very light or yellowish)", "None", "Bleached only on upper surface", "Pale (very light)/Focal bleaching", "Totally bleached white/fluoro", "Recently dead coral lightly covered in algae")
  bcols <- c("Mushroom Bleach Severity", "Massive Bleach Severity", "Encrusting Bleach Severity", "Vase/Foliose Bleach Severity", "Plate/Table Bleach Severity", "Bushy Bleach Severity", "Branching Bleach Severity")
  is_valid_descriptive_bleach_severity <- apply(data_df[, bcols], 2, function(x) !(x %in% valid_descriptive_bleach_severity))
  is_valid_descriptive_bleach_severity <- ifelse(is.na(is_valid_descriptive_bleach_severity), FALSE, is_valid_descriptive_bleach_severity)
  check_descriptive_bleach_severity <- rowSums(is_valid_descriptive_bleach_severity) > 0
  
  bleached_severity <- data_df$`Bleached Average Severity Index (calculated via matrix)`
  check_bleach_severity <- bleached_severity >= 1 & bleached_severity <= 8
  bleached_severity <- ifelse(is.na(bleached_severity), TRUE, bleached_severity)

  check <- !check_tide | check_macroalgae | !check_bleach_severity | check_descriptive_bleach_severity
  if (any(check)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    if (any(!check_tide)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid tide values:",
                       toString(data_df[!check_tide , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_tide]))
      
    }
    if (any(check_macroalgae)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid macroalgae values:",
                       toString(data_df[check_macroalgae, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_macroalgae]))
    }
    if (any(!check_bleach_severity)) {
      warning3 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid bleach severity values:",
                       toString(data_df[!check_bleach_severity, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_bleach_severity]))
    }
    if (any(check_descriptive_bleach_severity)) {
      warning4 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid descriptive beach severity:",
                       toString(data_df[check_descriptive_bleach_severity , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_descriptive_bleach_severity ]))
    }
  
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[!check_tide, 1],
        index = (1:nrow(data_df))[!check_tide],
        message = "Invalid tide value"
      )
      contribute_to_metadata_report("Tide", warnings, parent_key = "Warning")
      
      warnings <- data.frame(
        ID = data_df[check_macroalgae, 1],
        index = (1:nrow(data_df))[check_macroalgae],
        message = "Invalid macroalgae"
      )
      contribute_to_metadata_report("Macroalgae", warnings, parent_key = "Warning")
      
      warnings <- data.frame(
        ID = data_df[!check_bleach_severity, 1],
        index = (1:nrow(data_df))[!check_bleach_severity],
        message = "Invalid Bleach Severity"
      )
      contribute_to_metadata_report("Bleach Severity", warnings, parent_key = "Warning")
      
      warnings <- data.frame(
        ID = data_df[check_descriptive_bleach_severity, 1],
        index = (1:nrow(data_df))[check_descriptive_bleach_severity],
        message = "Invalid Bleach Severity Description"
      )
      contribute_to_metadata_report("Bleach Severity Description", warnings, parent_key = "Warning")
    }
    
  }
  data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | check)
  
  
  return(data_df)
}


verify_percentages <- function(data_df) {
  # Check that all percentages in a row are between 0 and 100
  perc_cols <- grep("%", colnames(data_df))
  if(length(perc_cols) > 0){
    perc_cols_vals <- data_df[, perc_cols]
    col_check <- apply(perc_cols_vals, 2, function(x) x < 0 | x > 100)
    col_check <- ifelse(is.na(col_check), FALSE, col_check)
    check <- rowSums(col_check) > 0
    data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | check)
    if (any(check)) {
      grandparent <- as.character(sys.call(sys.parent()))[1]
      parent <- as.character(match.call())[1]
      warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have percentages in an invalid format:",
                       toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
      base::message(warning)
      
      if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
        # Append the warning to an existing matrix 
        warnings <- data.frame(
          ID = data_df[check, 1],
          index = (1:nrow(data_df))[check],
          message = "Invalid Percentages"
        )
        contribute_to_metadata_report("Percentages", warnings, parent_key = "Warning")
        
      }
      
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
  nonexempt_df <- data_df[, -which(names(data_df) %in% exempt_cols)]
  na_present <- apply(nonexempt_df, 2, function(x) is.na(x) | is.null(x) | x == "")
  na_present <- ifelse(is.na(na_present), FALSE, na_present)
  check <- rowSums(na_present) > 0
  data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | check)
  if (any(check)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have missing data:",
                     toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
    base::message(warning)
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[check, 1],
        index = (1:nrow(data_df))[check],
        message = "NA or Null values present"
      )
      contribute_to_metadata_report("NA|NULL", warnings, parent_key = "Warning")
      
    }
    
  }
  return(data_df)
}

verify_integers_positive <- function(data_df) {
  # R function that verifys all integers are positive values as they represent 
  # real quantities. Note: Whole numbers are not integers, they must be declared
  # as such. All relevant columns were set as integers in the set_data_type 
  # function
  
    is_integer <- sapply(output_df[1,],is.integer)
    if(any(is_integer)){
      col_check <- apply(data_df[,is_integer], 2, function(x) x < 0)
      col_check <- ifelse(is.na(col_check), FALSE, col_check)
      check <- rowSums(col_check) > 0
      data_df[, "error_flag"] <- as.integer(data_df[, "error_flag"] | check)
     
      if (any(check)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have non-positive integer values:",
                         toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
        base::message(warning)
        
        
        if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
          # Append the warning to an existing matrix 
          warnings <- data.frame(
            ID = data_df[check, 1],
            index = (1:nrow(data_df))[check],
            message = "Non-positive integers present"
          )
          contribute_to_metadata_report("Integers", warnings, parent_key = "Warning")
          
        }
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
  
  hc_check <- ifelse(is.na(hc_check), TRUE, hc_check)
  sc_check <- ifelse(is.na(sc_check), TRUE, sc_check)
  rdc_check <- ifelse(is.na(rdc_check), TRUE, rdc_check)
  
  error <- !hc_check | !sc_check | !rdc_check
  
  data_df[["error_flag"]] <- as.integer(data_df[["error_flag"]] | error)
  if (any(error)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid coral cover values:",
                     toString(data_df[error , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[error]))
    base::message(warning)
    
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[error, 1],
        index = (1:nrow(data_df))[error],
        message = "Invalid coral cover values"
      )
      contribute_to_metadata_report("Coral Cover", warnings, parent_key = "Warning")
    }
  }
  return(data_df)
}

verify_reef <- function(data_df){
  # Check that the reef ID is in one of the correct standard formats with regex.
  # Look for most similar reef ID if it is not. Am not checking for a match 
  # because that would mean no new reefs would be accepted and I believe that 
  # the reef input is restricted to existing reefs so it is unlikley to be a typo
  
  reef_id <- data_df[["Reef ID"]]
  correct_reef_id_format <- grepl("^(1[0-9]|2[0-9]|10)-\\d{3}[a-z]?$", reef_id)
  data_df[,"error_flag"] <- as.integer(data_df[,"error_flag"] | !correct_reef_id_format)
  if (any(!correct_reef_id_format)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid Reef IDs:",
                     toString(data_df[!correct_reef_id_format , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!correct_reef_id_format]))
    base::message(warning)
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[!correct_reef_id_format, 1],
        index = (1:nrow(data_df))[!correct_reef_id_format],
        message = "Invalid reef ID"
      )
      contribute_to_metadata_report("Reef ID", warnings, parent_key = "Warning")
    }
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
    if (any(dated_estimated) & !any(na_present)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have their voyage dates estimated from their vessel",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      base::message(warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have voyage dates",
                        toString(data_df[na_present , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[na_present]))
      base::message(warning2)
    }  
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[dated_estimated, 1],
        index = (1:nrow(data_df))[dated_estimated],
        message = "Invalid Voyage Date. Date was successfully estimated."
      )
      contribute_to_metadata_report("Estimated Voyage Date", warnings, parent_key = "Warning")
      warnings <- data.frame(
        ID = data_df[na_present, 1],
        index = (1:nrow(data_df))[na_present],
        message = "Invalid Voyage Date. "
      )
      contribute_to_metadata_report("Invalid Voyage Date", warnings, parent_key = "Warning")
    }
    
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
  # perfect. (X_index, Y_index, Distance). Pre-allocating memory for the matrix
  # is not definitive and needs to assume worst case scenario or maximum 
  # allocation possible. However, this requires to much memory, and therefore 
  # will dynamically allocate memory as needed even though this is slower.
  
  #Pre-allocate variables and memory 
  x_rows <- nrow(x)
  x_cols <- ncol(x)
  y_rows <- nrow(y)
  
  match_indices <- matrix(nrow = 0, ncol = 3)
  
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
      match_indices <- rbind(match_indices, match)
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
    } else if (tolower(data_type) == "time") {
      time <- as.POSIXct(data_df[[column_name]], format = "%H:%M:%S")
      output_df[[column_name]] <- format(time, '%H:%M:%S')
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
    warning <- paste("Warning in", parent , "within", grandparent, "- Incorrect data type present. The rows with the following IDs have had value(s) coerced to NA:",
                     paste(data_df[coerced_na, 1], collapse = ", "), "and the following indexes", paste(indexes[coerced_na], collapse = ", "))
    base::message(warning)
    
    if (exists("contribute_to_metadata_report") && is.function(contribute_to_metadata_report)) {
      # Append the warning to an existing matrix 
      warnings <- data.frame(
        ID = data_df[coerced_na, 1],
        index = (1:nrow(data_df))[coerced_na],
        message = "Incorrect data type present. Values coerced to NA"
      )
      contribute_to_metadata_report("Data Type", warnings, parent_key = "Warning")
    }
  }
  
  # remove trailing and leading spaces from strings for comparison. 
  output_df <- remove_leading_spaces(output_df)
  return(output_df)
}
  
update_config_file <- function(data_df, config_path) {
  
  config <- fromJSON(config_path)
  data_colnames <- colnames(data_df)
  expected_source_names <- config$mappings$transformations$source_field
  
  if (!all(data_colnames %in% expected_source_names)) {
    warning <- "Column names in 'data_df' do not match the expected source names. New json config file will be created with most appropriate mapping. Please check after process is complete."
    warning(warning)
    send_error_email("Auth\\", "ethankwaters@gmail", warning, "CCIP Reusable Workflow - Config File out-of-date")
    closest_matches <- get_closest_matches(data_colnames, expected_source_names)
    new_json_data <- config
    
    # Replace the original source values in new_json_data with closest matches
    for (i in seq_len(ncol(closest_matches))) {
      new_input <- closest_matches[1, i]
      closest_match <- closest_matches[2, i]
      index <- which(new_json_data$mappings$transformations$source_field %in% closest_match)
      if (!is.na(index) ) {
        new_json_data$mappings$transformations$source_field[index] <- new_input
        contribute_to_metadata_report("Config Mapping Update", paste(closest_match, "updated to", new_input), parent_key = "Warning")
      }
    }
    
    json_data <- toJSON(new_json_data, pretty = TRUE)
    directory <- dirname(config_path)
    writeLines(json_data, file.path(directory, paste(config$metadata$control_data_type, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json", sep = "")))
    
  }
}

map_new_fields <- function(data_df, new_fields){
  for (i in seq_len(nrow(new_fields))) {
    new_field <- new_fields$field[i]
    default_value <- new_fields$default[i]
    position <- new_fields$position[i]
    data_df[, position] <- default_value
    colnames(data_df)[position] <- new_field
  }
}  
  
map_all_fields <- function(data_df, transformed_df, mappings){
  closest_matches <- get_closest_matches(colnames(data_df), mappings$source_field)
  for (i in seq_len(ncol(closest_matches))) {
    index <- match(closest_matches[2,i], mappings$source_field)
    position <- mappings$position[index]
    mapped_name <- mappings$target_field[index]
    colnames(transformed_df)[position] <- mapped_name 
    transformed_df[, position] <- data_df[[closest_matches[1,i]]]
  }
  
}

map_data_structure <- function(data_df, mappings, new_fields){
  
  transformed_df <- data.frame(matrix(ncol = nrow(mappings) + nrow(new_fields), nrow = nrow(data_df)))
  data_colnames <- colnames(data_df)
  is_already_mapped <- all(!is.na(match(mappings$target_field, data_colnames)))
  if(is_already_mapped){
    col_indices <- match(data_colnames, mappings$target_fields)
    positions <- mappings$position[col_indices]
    transformed_df <- data_df[,positions]
    return(transformed_df)
  }

  transformed_df <- map_new_fields(transformed_df, new_fields)
  transformed_df <- map_all_fields(data_df, transformed_df, mappings)
  
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

extract_dates <- function(input){
  input <- sapply(input, basename)
  dates <- str_extract(input, "\\d{8}_\\d{6}")
  dates <- ifelse(is.na(dates), str_extract(input, "\\d{4}_\\d{1,2}_\\d{1,2}_\\d{1,2}_\\d{1,2}(?:_\\d{1,2}(?:_\\d{1,2})?)?_[APMapm]{2}"), dates)
  dates <- ifelse(is.na(dates), str_extract(input, "\\d{8}"), dates)
  dates <- ifelse(is.na(dates), str_extract(input, "\\d{6}"), dates)
  date_objects <- parse_date_time(dates, orders = c("Ymd_HMS", "Y_m_d_H_M_%p", "Y_m_d_H_M", "Y_m_d_H_M_S_%p", "Ymd", "ymd"))
  return(date_objects)
}

find_recent_file <- function(directory_path, keyword, file_extension) {
  # Get a list of files in the directory
  files <- list.files(file.path(getwd(),directory_path), pattern = paste0(keyword, ".*\\.", file_extension), full.names = TRUE)
  if (length(files) == 0) {
    cat("No matching files found.\n")
    return(NULL)
  }
  date_objects <- extract_dates(files)
  most_recent_index <- which.max(date_objects)
  return(files[most_recent_index])
}

save_spatial_as_raster <- function(output_path, serialized_spatial_path){
  tryCatch({
    site_regions <- readRDS(serialized_spatial_path)
    for(i in 1:length(site_regions)){
      file_name <- names(site_regions[i])
      modified_file_name <- gsub("/", "_", file_name)
      if (file.info(output_path)$isdir) {
        file.remove(output_path)
        output_path <- dirname(output_path)
      }
      writeRaster(site_regions[[i]], filename = file.path(output_path, paste(modified_file_name,".tif", sep=""), format = "GTiff", overwrite = TRUE))
    }
  }, error = function(e) {
    cat("An error occurred while saving", modified_file_name, "\n")
  })
}

get_spatial_differences <- function(kml_data, previous_kml_data){
  spatial_differences <- list()
  reef_id_pattern <- "\\b(1[0-9]|2[0-9]|10)-\\d{3}[a-z]?\\b"

  # Extract Reef IDs from each list
  reef_ids <- sapply(str_extract(names(kml_data), reef_id_pattern), toString)
  reef_ids_previous <- sapply(str_extract(names(previous_kml_data), reef_id_pattern), toString)
  
  # Iterate through all IDS that exist in the new KML file. Any that have 
  # changed or do not exist in the previous data set will be added to the new 
  # one. It is intentional that cull sites that have been removed are not 
  # included, as a result of the general philosophy outlined in the documentation
  for (reef_id in reef_ids) {
    index_reefs <- which(reef_ids == reef_id)
    index_reefs_previous <- which(reef_ids_previous == reef_id)
    
    if (length(index_reefs) > 0 && length(index_reefs_previous) > 0) {
      reef <- kml_data[[index_reefs]]
      reef_name <- names(kml_data)[[index_reefs]]
      reef_previous <- previous_kml_data[[index_reefs_previous]]
      reef_name_previous <- names(previous_kml_data)[[index_reefs_previous]]
      
      # If comparison fails or does not indicate that reefs are identical then 
      # add reef to list of changed reefs.
      is_unchanged <- FALSE
      tryCatch({
        if (all(dim(reef) == dim(reef_previous))) {
          is_unchanged <- (all(reef_previous == reef)) && (reef_name_previous == reef_name)
        }
        if(!is_unchanged){
          name <- names(kml_data)[[index_reefs]]
          spatial_differences[[name]] <- kml_data[[index_reefs]]
        }
      }, error = function(e) {
        name <- names(kml_data)[[index_reefs]]
        spatial_differences[[name]] <- kml_data[[index_reefs]]
      })
    }
  }
  return(spatial_differences)
}

compute_checksum <- function(data) {
  digest(data, algo = "md5", serialize = TRUE)
}


assign_nearest_site_method_c <- function(data_df, kml_path, keyword, calculate_site_rasters=1, kml_path_previous=NULL, spatial_path=NULL, raster_size=0.0005, x_closest=1, is_standardised=0, save_spatial_as_raster=0){
  # Assign nearest sites to manta tows with method developed by Cameron Fletcher
  
  kml_layers <- st_layers(kml_path)
  layer_names_vec <- unlist(kml_layers["name"])
  kml_data <- setNames(lapply(layer_names_vec, function(i)  st_read(kml_path, layer = i)), layer_names_vec)
  crs <- projection(kml_data[[1]])
  sf_use_s2(FALSE)
  checksum <- compute_checksum(kml_data)
  
  # Acquire the directory to store raster outputs and the most recent spatial 
  # file that was saved as an R binary
  if(!is.null(spatial_path)){
    if(file.info(spatial_path)$isdir){
      print("Invalid path to serialized spatial data. Must be a file not a directory")
      spatial_file <- NULL
      spatial_directory <- spatial_path
    } else {
      spatial_file <- spatial_path
      spatial_directory <- dirname(spatial_path)
    }
  }
  
  # compare two kml files and return the geometry collections that have been 
  # updated 
  update_kml <- FALSE
  if(!is.null(kml_path_previous) && !is.null(spatial_file) && calculate_site_rasters){
    previous_kml_layers <- st_layers(kml_path_previous)
    previous_layer_names_vec <- unlist(previous_kml_layers["name"])
    previous_kml_data <- setNames(lapply(previous_layer_names_vec, function(i)  st_read(kml_path_previous, layer = i)), previous_layer_names_vec)
    previous_crs <- projection(previous_kml_data[[1]])
    previous_checksum <- compute_checksum(previous_kml_data)
    if(checksum != previous_checksum){
      if(previous_crs == crs){
        kml_data_to_update <- get_spatial_differences(kml_data, previous_kml_data)
        if(!is.na(kml_data_to_update) | !is.null(kml_data_to_update)){
          update_kml <- TRUE
        }
      } 
    } else {
      base::message("Checksum determined current and previous KML data are identical")
      calculate_site_rasters <- 0
    }
  }

  # if loading data fails calculate site regions
  load_site_rasters_failed <- TRUE
  if(!calculate_site_rasters && !is.null(spatial_file)){
    tryCatch({
      base::message("Loading previously saved raster data ...")
      site_regions <- readRDS(spatial_file)
      load_site_rasters_failed <- FALSE
      base::message("Loaded data successfully")
    }, error = function(e) {
      print(paste("Error site regions could not be loaded. Site regions will be calculated instead.", conditionMessage(e)))
    })
  }

  if(calculate_site_rasters){
    if(!update_kml | load_site_rasters_failed){
      base::message("Assigning sites to raster pixels for all reefs...")
      kml_data_simplified <- simplify_reef_polyogns_rdp(kml_data)
      site_regions <- assign_raster_pixel_to_sites(kml_data_simplified, layer_names_vec, crs, raster_size, x_closest, is_standardised)
    } else {
      base::message("Updating raster pixels for reefs that have changed since last process date...")
      kml_data_simplified <- simplify_reef_polyogns_rdp(kml_data_to_update)
      updated_layer_names_vec <- names(kml_data_simplified)
      updated_site_regions <- assign_raster_pixel_to_sites(kml_data_simplified, updated_layer_names_vec, crs, raster_size, x_closest, is_standardised)
      
      reef_id_pattern <- "\\b(1[0-9]|2[0-9]|10)-\\d{3}[a-z]?\\b"
      site_regions_reef_ids <- sapply(str_extract(names(kml_data), reef_id_pattern), toString)
      updated_site_regions_reef_ids <- sapply(str_extract(names(kml_data_to_update), reef_id_pattern), toString)
      
      index_reefs <- match(updated_site_regions_reef_ids, site_regions_reef_ids)
      site_regions <- site_regions[-index_reefs]
      site_regions <- c(site_regions, updated_site_regions)
      site_regions <- site_regions[order(names(site_regions))]
    }
    # The is for testing purposes 
    assign("site_regions", site_regions, envir = .GlobalEnv)
    
    
    base::message("Saving raster data as serialised binary file...")
    tryCatch({
      if (!dir.exists(spatial_directory)) {
        dir.create(spatial_directory, recursive = TRUE)
      }
      saveRDS(site_regions, file.path(spatial_directory, paste(keyword, "_site_regions_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".rds", sep = "")), row.names = FALSE)
    }, error = function(e) {
      print(paste("Error site regions raster data - Data saved in source directory", conditionMessage(e)))
      saveRDS(site_regions, paste(keyword,"_site_regions_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".rds", sep = ""))
      contribute_to_metadata_report("output", file.path(getwd(), paste(keyword,"_site_regions_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".rds", sep = "")))
    })
    
  } 
  
  if(save_spatial_as_raster == 1 && !is.null(spatial_path)){
    base::message("Saving raster data as gtiff files...")
    raster_output <- file.path(spatial_directory, "rasters")
    if (!dir.exists(raster_output)) {
      dir.create(raster_output, recursive = TRUE)
    }
    save_spatial_as_raster(raster_output, spatial_file)
  }
 
  base::message("Assigning sites to data...")
  data_df <- get_centroids(data_df, crs)
  updated_pts <- data_df
  for(i in 1:length(site_regions)){
    is_contained <- sapply(data_df$`Reef ID`, function(str) grepl(str, names(site_regions[i])))
    if(any(is_contained) == FALSE){
      next
    }
    reef_pts <- data_df[is_contained,]
    nearest_site_manta_data <- raster::extract(site_regions[[i]], reef_pts)
    updated_pts[is_contained, c("Nearest Site")] <- nearest_site_manta_data
  }
  updated_pts <- st_drop_geometry(updated_pts)
  is_nearest_site_has_error <- is.na(updated_pts$`Nearest Site`) | ifelse(is.na(updated_pts$`Nearest Site` == -1),FALSE,updated_pts$`Nearest Site` == -1)
  updated_pts[,"error_flag"] <- as.integer(updated_pts[,"error_flag"] | is_nearest_site_has_error)
  
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
      mutate_at(vars(`Start Lat`, `Start Lng`, `End Lat`, `End Lng`, `mean_lat`, `mean_long`), ~ round(., precision))
  }
  
  #create manta_tow points. Can't create pts that are NA, use 0 as place holder.
  # the st_as_sf function by default removes these columns which is desirable as
  # they are not in the legacy format. This means that after geometry is dropped 
  # the desired remaining columns will all be correct and Nearest Site will be 
  # NA as expected. 
  data_df$mean_lat[is.na(data_df$mean_lat)] <- 0
  data_df$mean_long[is.na(data_df$mean_long)] <- 0
  pts <- st_as_sf(data_df, coords=c("mean_long", "mean_lat"), crs=crs)
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
      min_distances <- min_distances_df$`Distance to Site`
      
    } else {
      min_distances <- drop_units(distances)
      min_distance_site_numbers <- rep(site_numbers, length(min_distances))
    }
    
    # Set site numbers to NA if they are more than 300m away.
    is_within_required_distance <- min_distances > 300
    min_distance_site_numbers[is_within_required_distance] <- -1
    values(raster) <- min_distance_site_numbers
    names(raster) <- c("Nearest Site")
    
    # distance_raster <- raster
    # values(distance_raster) <- min_distances
    # names(distance_raster) <- c("Distance to Site")
    # 
    # site_region <- brick(raster,distance_raster)
    site_regions[[i]] <- raster
  }
  
  return(site_regions)
}

site_names_to_numbers <- function(site_names){
  return(as.numeric(sub(".+_(\\d+).*", "\\1", site_names)))
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


find_largest_extent <- function(kml_data){
  num_geometries <- length(kml_data)
  
  # Preallocate the data frame
  result <- data.frame(
    x_difference = numeric(num_geometries),
    y_difference = numeric(num_geometries)
  )
  
  for (i in 1:num_geometries) {
    bbox <- extent(kml_data[[i]])
    diff_x <- bbox@xmax - bbox@xmin
    diff_y <- bbox@ymax - bbox@ymin
    result[i, "x_difference"] <- diff_x
    result[i, "y_difference"] <- diff_y
  }
  
  colnames(result) <- c("x_difference", "y_difference")
  largest_extent <- c(max(result$x_difference),max(result$y_difference))
  names(largest_extent) <- c("x_difference", "y_difference")
  
  return(largest_extent)
}

standardise_extents <- function(kml_data){
  largest_extent <- find_largest_extent(kml_data)
  adjusted_extents <- list()
  for(i in 1:length(kml_data)) {
    extent <- extent(kml_data[[i]])
    diff_x <- extent@xmax - extent@xmin
    diff_y <- extent@ymax - extent@ymin
    largest_extent_centered <- extent(extent@xmin + diff_x/2 - largest_extent[1]/2,
                                      extent@xmax - diff_x/2 + largest_extent[1]/2,
                                      extent@ymin + diff_y/2 - largest_extent[2]/2,
                                      extent@ymax - diff_y/2 + largest_extent[2]/2
    )
    adjusted_extents[[i]] <- largest_extent_centered
  }
  return(adjusted_extents)
}


create_raster_templates <- function(extents, layer_names_vec, crs, raster_size=150){
  
  # rasterise the bounding boxes 
  # res <- 0.0005
  # NN input requires standard image size. 570x550 is approximately a resolution of 0.00045
  if (raster_size > 1){
    y_pixel <- raster_size
    x_pixel <- raster_size
    rasters <- setNames(lapply(extents, function(i) {
      raster(ext = i, ncols=x_pixel, nrows=y_pixel, crs = crs)
    }), layer_names_vec)
  } else {
    rasters <- setNames(lapply(extents, function(i) {
      raster(ext = i, resolution=raster_size, crs = crs)
    }), layer_names_vec)
  }
  return(rasters)
}


rasterise_sites <- function(kml_data, is_standardised=1, raster_size=150){
  if (is_standardised == 1){
    extent_data <- standardise_extents(kml_data)
    location <- "standardised_extent"
  } else {
    extent_data <- kml_data
    location <- "varying_extent"
  }
  
  # res <- 0.0005
  # NN input requires standard image size. 570x550 is approximately a resolution of 0.00045
  y_pixel <- raster_size
  x_pixel <- raster_size
  for(i in 1:length(kml_data)){
    site_names <- kml_data[[i]]$Name
    site_numbers <- site_names_to_numbers(site_names)
    kml_data[[i]]$site_number <- site_numbers
    site_raster <- st_rasterize(kml_data[[i]], st_as_stars(st_bbox(extent_data[[i]]), field = "site_number", nx = x_pixel, ny = y_pixel))
    file_name <- names(kml_data[i])
    modified_file_name <- gsub("/", "_", file_name)
    site_raster <- as(site_raster, "Raster")
    writeRaster(site_raster, filename = paste("CNN\\", raster_size, "\\", location,  "\\sites_as_rasters\\", modified_file_name, sep=""), format = "GTiff", overwrite = TRUE)
    site_raster <- NA
  }
}


rasterise_sites_reef_encoded <- function(kml_data, layer_names_vec, is_standardised=1, raster_size=150){
  
  if (is_standardised == 1){
    extent_data <- standardise_extents(kml_data)
    location <- "standardised_extent"
  } else {
    extent_data <- kml_data
    location <- "varying_extent"
  }
  # res <- 0.0005
  # NN input requires standard image size. 570x550 is approximately a resolution of 0.00045
  y_pixel <- raster_size
  x_pixel <- raster_size
  for(i in 1:length(kml_data)){
    site_names <- kml_data[[i]]$Name
    site_numbers <- site_names_to_numbers(site_names)
    reef_numbers <- as.numeric(gsub("[^0-9.]", "", layer_names_vec[[i]]))
    encoded_reef_site_numbers <- as.numeric(sapply(site_numbers, function (x) paste(reef_numbers, x, sep = "")))
    kml_data[[i]]$site_number <- encoded_reef_site_numbers
    site_raster <- st_rasterize(kml_data[[i]], st_as_stars(st_bbox(extent_data[[i]]), field = "site_number", nx = x_pixel, ny = y_pixel))
    file_name <- names(kml_data[i])
    modified_file_name <- gsub("/", "_", file_name)
    site_raster <- as(site_raster, "Raster")
    writeRaster(site_raster, filename = paste("CNN\\", raster_size, "\\", location, "\\sites_as_rasters_encoded\\", modified_file_name, sep=""), format = "GTiff", overwrite = TRUE)
    site_raster <- NA
  }
}

xth_smallest <- function(x, x_values) {
  # Function to find the xth smallest value in a vector without sorting. This 
  # allows for the second closest sites etc to be determined. Likely unnecessary 
  #in production was used for testing purposes
  
  sorted_uniques <- sort(x)
  xth_smallest_values <- sorted_uniques[x_values]
  xth_smallest_indices <- which(x %in% xth_smallest_values)
  if(length(xth_smallest_indices > 1)){
    xth_smallest_indices <- xth_smallest_indices[1]
  }
  output <- data.frame(matrix(ncol = (2*length(x_values))))
  colnames(output) <- c("Nearest Site", "Distance to Site")
  output[1,] <- c(xth_smallest_indices, xth_smallest_values)
  return(output)
  
}


send_error_email <- function(oauth_path, to_email, content, subject = "Fatal Error in CCIP Control Data Workflow") {
  tryCatch({
    if (file.info(oauth_path)$isdir) {
      files <- list.files(oauth_path, full.names = TRUE)
      if (length(files) > 0) {
        file_info <- file.info(files)
        oauth_path <- files[which.max(file_info$mtime)]
      }
    }
    auth <- fromJSON(oauth_path)
    gmail_auth(scope = "compose", id = auth$installled$client_id, secret = auth$installled$client_secret)
    # Format email with the error message
    error_message <- paste("Error in R Script:\n", conditionMessage(content))
    mime <- create_mime(
      To(to_email),
      Subject(subject),
      body = error_message
    )
    
    # Send the email
    send_message(mime)
    cat("Error email sent.\n")
  }, error = function(e) {
    print("Error email failed.\n")
  })
}


