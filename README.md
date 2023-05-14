# Control Data Reusable Workflow

<img src="https://camo.githubusercontent.com/0058ce9713cb93a553c2f23207afbb49b1b852a70a4a24de20e2e816c58b299e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f6c6966656379636c652d6578706572696d656e74616c2d6f72616e67652e737667" alt="Lifecycle: experimental" data-canonical-src="https://img.shields.io/badge/lifecycle-experimental-orange.svg" style="max-width: 100%;">


The overall purpose of this workflow is to clean and wrangle control program data from GBRMPA so that it takes a standardised form to be utilised in research. This R code defines a data processing pipeline that imports, formats, and verifies data. It also creates a metadata report to document the pipeline. The `main()` function is the entry point of the pipeline. It takes as input the paths to the legacy and new data files, the geospatial site data, algorithm to assign nearest sites, and a flag indicating whether the data was exported from PowerBI or not.

The four major steps in this process are as follows:

  - Acquire control data
  - Convert data to standard legacy format 
  - Assign nearest site to manta tow data
  - Perform required mathematical operation

For further information see Reusable Digital Workflows Systems Diagrams and Reusable Digital Workflows Psudo Code Systems Diagrams



#### Function: `main(leg_path, new_cull, new_manta_tow, geospatial_sites, nearest_site_algorithm, is_powerBI_export)`

- **Input:**
    - `leg_path`: path to the legacy data file
    - `new_cull`: path to the new cull data file
    - `new_manta_tow`: path to the new manta tow data file
    - `geospatial_sites`: geospatial site data
    - `nearest_site_algorithm`: algorithm to assign nearest sites
    - `is_powerBI_export`: indicates if data was exported from PowerBI
- **Output:**
    - None
- **Description:**
    - This function is the main function that runs the data processing pipeline. It takes in several input parameters and uses them to call other functions to import, format, and verify data. It also creates a metadata report to document the data processing pipeline.

#### Function: `import_data(data, control_data_type, is_powerBI_export, sheet)`

- **Input:**
  - `data`: file path to the file containing the data
  - `control_data_type`: a string representing the type of control data contained within the dataframe
  - `is_powerBI_export`: a boolean indicating whether the file is a PowerBI export
  - `sheet`: an optional parameter which specifies the sheet number to read in the case of an `.xlsx` file
- **Output:**
    - dataframe containing imported data
- **Description:**
    - This function reads data from a file and returns a dataframe. It determines the file type and reads the file using the appropriate method. It also formats column names to remove special characters and adds any required columns to the dataframe.

#### Function: `format_control_data(current_df, legacy_df, control_data_type, section)`

- **Input:**
    - `current_df`: the data frame containing the data to be formatted
    - `legacy_df`: the data frame containing the expected legacy format
    - `control_data_type`: a string representing the type of control data contained within the dataframe
    - `section`: a string indicating the section of code that is currently being executed. This is utilised to identify when errors occur in the metadata report
- **Output:**
    - updated dataframe
- **Description:**
    - This function formats column names, order and type of a dataframe to match a legacy dataframe. It generates comments and returns an updated dataframe.

#### Function: `create_metadata_report(count)`

- **Input:**
    - `count`: an integer representing the number of reports generated thus far
- **Output:**
    - None
- **Description:**
    - This function creates a metadata report to document the data processing pipeline. It creates a file name and generates an XML template.

#### Function: `contribute_to_metadata_report(control_data_type, section, data, key)`

- **Input:**
    - `control_data_type`: a string representing the type of control data used
    - `section`: a string representing the section of the control data being formatted 
    - `data`: the data to add to the control data report
    - `key`: an optional parameter specifying the key for the data being added
- **Output:**
    - None
- **Description:**
    - This function adds a section to the metadata report from the information obtained in the previously executed function to the desired control data node.

#### Function: `outersect(x, y)`

- **Input:**
    - `x`: first vector
    - `y`: second vector
- **Output:**
    - elements of both vectors that are not in both of them
- **Description:**
    - This function returns the elements of two vectors that are not in both of them.

#### Function: `map_column_names(column_names)`

- **Input:**
    - `column_names`: column names to map
- **Output:**
    - a vector of mapped column names
- **Description:**
    - This function maps column names from one format to another. It uses a lookup table to map the column names.

#### Function: `add_required_columns(control_data_type, is_powerBI_export)`

- **Input:**
    - `control_data_type`: a string representing the type of control data contained within the dataframe
    - `is_powerBI_export`: a boolean indicating whether the file is a PowerBI export
- **Output:**
    - column names required for the control data
- **Description:**
    - This function adds any required columns to a dataframe. It uses input parameters to add any required columns to the dataframe.

#### Function: `verify_control_dataframe(new_data_df, legacy_data_df, control_data_type, ID_col, section, is_new)`

- **Input:**
    - `new_data_df`: dataframe containing new data
    - `legacy_data_df`: dataframe containing legacy data
    - `control_data_type`: a string representing the type of control data contained within the dataframe
    - `ID_col`: the name of the column containing unique IDs
    - `section`: a string representing the section of the control data being formatted
    - `is_new`: indicates if data is new or legacy
- **Output:**
    - verified dataframe
- **Description:**
    - This function finds discrepancies between new data and legacy data and handles them appropriately. It returns a verified dataframe.


#### Function: `set_data_type()`
- **Inputs:**
  - `data_df`: A data frame to be updated.
  - `control_data_type`: The type of control data.
- **Outputs:**
  - `data_df`: The updated data frame.
- **Description:**
    - This function sets the data type of each column of a data frame based on a lookup table stored in a CSV file. It returns the updated data frame.
    
#### Function: `update_IDs()`
- **Inputs:**
  - `new_data_df`: new data to be matched against
  - `legacy_data_df`: legacy data to be updated
  - `control_data_type`: the type of control data being processed
- **Outputs:**
  - Updated `legacy_data_df` with updated IDs
- **Description:**
  - This function attempts to update the IDs of the legacy data where the previous processing utilised data from a PowerBI export and therefore will have IDs of NA. This will find perfect matches (distance of zero) between the legacy data and new data, and alter the IDs accordingly. Once matches are found, IDs can then be altered. If there are multiple matches then they are left as is. Ultimately this means they will be treated as a new entry.

#### Function: `matrix_close_matches_vectorised()`
- **Inputs:**
  - `x`: data frame to search in
  - `y`: data frame to search against
  - `distance`: maximum distance from perfect for a match to be considered
- **Outputs:**
- A matrix containing:
    - `X_index` - the index of the row in `x` that matched
    - `Y_index` - the index of the row in `y` that matched
    - `Distance` - the distance between the matched rows
- **Description:**
  - This is a function that takes two matrices or data frames x and y and a distance value, and returns a matrix match_indices containing the indices of the rows in x and y that have non-perfect matches within the specified distance. The function first pre-allocates memory for the match_indices matrix assuming the worst-case scenario of y_rows * x_rows possible matches. If this allocation fails due to insufficient memory, it tries again with a smaller allocation of 10,000,000 rows. Then, the function iterates through each row in x and compares it to every row in y. For each value in the row of x, the function evaluates whether it matches the corresponding value in the row of y. These logical values are then appended to the matches matrix. After iterating over every column, there will be a matrix of size (y_rows, x_cols), where each row represents a row in y and each column represents a column in x. A perfect matching row in y will have a corresponding row in matches exclusively containing TRUE. Given that TRUE is equivalent to 1, the rowSums function is used to determine the number of non-perfect matches. If this number is less than or equal to the specified distance, the row index, column index and distance from perfect match are stored in match_indices. The function uses a custom vectorised function store_index_vec to append matches to match_indices. Finally, the na.omit function is used to remove rows with NA values from match_indices before it is returned.
  
#### Function: `match_vector_entries()`
- **Inputs:**
  - `current_vec`: vector to search in
  - `target_vec`: vector to search against
  - `section`: section of metadata report to write to
  - `check_mapped`: whether to check for pre-defined column name mappings
  - `correct_order`: whether to return vectors in the same order
- **Outputs:**
  - A list containing:
      - `current_vec` - The vector of strings that has been updated to include matches from the input `target_vec` 
      - `correct_order_indices` - a vector indicating the correct order of the input vector if needed at a later date
      - `original_order_indices` - a vector indicating the original order of the input vector
      - `metadata` - data frame containing information for the metadata report  
- **Description:**
  - This is a function in R that is designed to compare any two vectors and identify matching entries. The function takes in two vectors, current_vec and target_vec, and a few optional arguments. The first two arguments are mandatory, and they contain the two vectors that the function will compare. The check_mapped argument is a logical value that defaults to FALSE, and it controls whether the function should check for pre-defined mappings between column names or not. If check_mapped is TRUE, the function will use the map_column_names function to map non-matching column names to pre-defined column names. The correct_order argument is another logical value that defaults to FALSE, and it controls whether the function should return the matching entries in the order they appear in current_vec.The function first cleans the vector entries for easy comparison. It removes characters such as punctuation and spaces and converts all the text to lowercase. It then collects some metadata about the vectors, such as their length and whether their entries match. The function then sets a maximum distance for fuzzy string matching and identifies the current column names and indices that match a column name in the legacy format. It does this by comparing the cleaned vectors and counting how many matches there are.If the check_mapped argument is TRUE, the function checks for pre-defined mappings between column names. If there is a pre-defined mapping for a non-matching column name, the function uses it to update the column name. If there is no pre-defined mapping, the function finds the closest match within a specified distance with Levenshtein distances or by matching partial strings contained within column names. Finally, the function returns some metadata about the matching entries, including whether they are unique, whether there are any non-matching column names, and whether any column names are NA. The function also returns the matching entries, and if the correct_order argument is TRUE, it returns them in the order they appear in current_vec.
 
#### Function: `or4()`
- **Inputs:**
  - `x`: logical value or size n vector of logical values 
  - `y`: logical value or size n vector of logical values 
- **Outputs:**
  - A logical value or size n vector of logical values
- **Description:**
  - This is a custom Boolean OR function written in C for speed that works the same as the base R operator "|", with the exception that NA values are considered as TRUE. The function is called "or4", and it takes two logical vectors x and y as inputs. It first determines the length of the two vectors and assigns the larger length to the variable "n". It then allocates a new logical vector "ans" of length "n" using the "allocVector" function. The function then creates pointers to the logical values of vectors x, y, and the new vector "ans" using the "LOGICAL" macro. It then iterates through the "ans" vector, setting each element to the result of the OR operation between the corresponding elements of vectors x and y. The function also includes some logic to handle cases where the lengths of the input vectors are not equal. Specifically, it uses the variables "ix" and "iy" to keep track of the current index of vectors x and y, respectively. When the end of either vector is reached, the index is reset to 0, allowing the function to cycle through the values of the shorter vector again. Finally, the function unprotects the "ans" vector and returns it.

#### Operator: `%fin%`
- **Inputs:**
  - `x`: size n*m vector 
  - `y`: size a*b vector 
- **Outputs:**
  - size n vector 
- **Description:**
  - This is a faster implementation of the %in% operator that uses the fastmatch package. The fastmatch function is used to match the values in x to the values in table, and the resulting index is compared to 0. If it is greater than 0, then the value is found in the table and the function returns TRUE. If not, the function returns FALSE.
  
#### Function: `vectorised_separate_close_matches()`
- **Inputs:**
  - `close_match_rows`: A data frame containing the close matches between two data sets. It has 3 columns, where the first and second columns contain the row indices from the two data sets, and the third column contains the distances between these rows.
- **Outputs:**
  - The function returns a list containing 4 data frames that represent the separated close matches. Each of the data frames contains the row indices of the original data frames that correspond to the particular type of match. 
- **Description:**
  - The vectorised_separate_close_matches() function is used to separate the close matching rows between two data sets. This function separates the rows in a vectorized process, which involves using logical checks on vectors or matrices so Boolean operations can be used to separate the rows. This reduces computational time by two or three orders of magnitude, which is a worthy trade-off for the reduced readability. The function handles close matching rows in the following order:

      - Rows with one-one close matches
      - Rows with many-many perfect matches
      - Rows with one-many perfect matches
      - Rows with one-many non-perfect matches
      - Rows with many-many non-perfect matches
      - Rows with one-one and one-many non-perfect matches
      
  - This order matters, and it is not a definitive process. Therefore, the order needs to maximize the probability that a row from the new data set is matched with one from the previous data set. One-to-one matches and many-many perfect matches are the most likely to be correct and, therefore, are removed first. It is important that the next matches handled are one-many. This is to ensure a match is found for the "one," as its most likely match is one of the many with the smallest distance. Any rows with those indices can then be removed to prevent double handling. Once many-many rows have been handled, one-one or one-many relationships may have been formed and therefore can be handled repetitively until all matches have been found or no more can be found.

#### Function: `rec_group(stack, m2m_split, groups, group)`
- **Inputs**:
  - `stack`: a vector containing the data to be grouped
  - `m2m_split`: a list containing the index of the matching rows
  - `groups`: a list of matrices containing grouped data
  - `group`: an integer indicating the group number
- **Outputs**:
  - A list of matrices containing grouped data
- **Description**:
  - This function is designed to group sets of perfect matching rows from dataframe x and dataframe y to determine if they are mistakes or coincidental duplicates. It recursively groups data by iteratively adding the matching rows to a list of matrices (groups). The function returns the list of grouped matrices once all matching rows have been added to groups. The stack vector contains the data to be grouped, and the m2m_split list contains the index of the matching rows. The groups list of matrices contains the grouped data, and the group integer is used to keep track of the current group number.

  
### Function: `verify_RHISS()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that the data in the tide, bleaching and macroalgae columns of the data frame are valid. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_percentages()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that all percentage values in the data frame are between 0 and 100. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_na_null()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function checks if any values in `data_df` are NA or NULL, and flags those rows as invalid by adding a "TRUE" value to the "error_flag" column. 
    
### Function: `verify_integers_positive()`

- **Inputs:**
    - `data_df`: a data frame
    - `cols`: a vector of column names
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that all values in specified integer columns are positive integers.  The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `remove_leading_spaces()`

- **Inputs:**
    - `data_df`: a data frame
    - `cols`: a vector of column names
- **Outputs:**
    - `data_df` modified
- **Description:**
    - This function removes leading and trailing spaces from all entries in the specified data frame columns. The function returns the modified data frame.

### Function: `verify_coral_cover()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that all values in the "Hard Coral", "Soft Coral", and "Recently Dead Coral" columns of the data frame are valid coral cover descriptors ("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", or "5+"). The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_cots_scars()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that all values in the "COTS Scars" column of the data frame are valid ("a", "p", or "c"). The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_cohort_count()`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` (data frame) after altering a column called "error_flag"
- **Description:**
    - This function verifies that all values in the "Cohort Count" column of the data frame are valid integers. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `find_one_to_one_matches()`

- **Inputs:**
    - `close_match_rows`: a data frame with columns "x_index", "y_index" and "difference"
      - "x_index" is the row index of the row from dataframe x that has a close match to a row in dataframe y indicated by "y_index" with a distance of "distance"
      - "y_index" is the row index of the row from dataframe y that has a close match to a row in dataframe x indicated by "x_index" with a distance of "distance"
- **Outputs:**
    - None
- **Description:**
    - This function determines one-to-one matches between rows in two data frames, based on the output of the "find_close_matches" function. The function takes as input a data frame with columns "x_index", "y_index" and "difference", and determines which rows have one-to-one matches. A one-to-one match is defined as a row in the x_df data frame that has a single match in the y_df data frame, and vice versa. The function updates two global variables, "perfect_duplicate_indices" and "discrepancies_indices", which are data frames containing the row indices of perfect matches and discrepancies, respectively.
    
    
