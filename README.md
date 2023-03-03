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

#### Function: `add_required_columns(df, control_data_type, is_powerBI_export)`

- **Input:**
    - `df`: the dataframe to which the columns will be added
    - `control_data_type`: a string representing the type of control data contained within the dataframe
    - `is_powerBI_export`: a boolean indicating whether the file is a PowerBI export
- **Output:**
    - dataframe with added columns
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

#### Function: `find_close_matches()`
- **Inputs:**
  - `x`: data frame to search in
  - `y`: data frame to search against
  - `distance`: maximum distance from perfect for a match to be considered
- **Outputs:**
- A list of lists, with each sublist containing:
    - `X_index` - the index of the row in `x` that matched
    - `Y_index` - the index of the row in `y` that matched
    - `Distance` - the distance between the matched rows
- **Description:**
  - This function finds a list of all close matches between rows in `x` and `y` within a specified distance. The distance is the number of non-perfect column matches within a row.

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
  - This function compares any two vectors and identifies matching entries. This utilizes a number of common NLP techniques to match elements that are intended to be identical but not exhibit slight differences.
 
