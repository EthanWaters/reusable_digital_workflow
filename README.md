# Control Data Reusable Workflow

<img src="https://camo.githubusercontent.com/0058ce9713cb93a553c2f23207afbb49b1b852a70a4a24de20e2e816c58b299e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f6c6966656379636c652d6578706572696d656e74616c2d6f72616e67652e737667" alt="Lifecycle: experimental" data-canonical-src="https://img.shields.io/badge/lifecycle-experimental-orange.svg" style="max-width: 100%;">

## 1.0 Overview

The overall purpose of this workflow is to clean and wrangle control program data from GBRMPA so that it takes a standardised form to be utilised in research. This R code defines a data processing pipeline that imports, formats, and verifies data. It also creates a metadata report to document the pipeline. The `main()` function is the entry point of the pipeline. It takes as input the paths to the legacy and new data files, the geospatial site data, algorithm to assign nearest sites, and a flag indicating whether the data was exported from PowerBI or not.

The four major steps in this process are as follows:

  1. `Data Transformation` to legacy format .
  2. Perform `Error Checking & Processing`.
  3. Perform `Site Assignment` to control data if applicable.
  4. `Export` 

## Installation & Requirements
The system was developed with the following. See environment log for details of all packages installed in dev environment.

#### R Environment Information

- R version: 4.2.1 (2022-06-23 ucrt)
- Platform: x86_64-w64-mingw32
- Locale: LC_COLLATE=English_Australia.utf8;LC_CTYPE=English_Australia.utf8;LC_MONETARY=English_Australia.utf8;LC_NUMERIC=C;LC_TIME=English_Australia.utf8
- Timezone: Australia/Brisbane
- Date: Thu Nov 16 12:57:30 2023

#### Required Packages


| Package       | Version   |
| ------------- | --------- |
| tools         | 4.2.1     |
| installr      | 0.23.4    |
| readxl        | 1.4.1     |
| sets          | 1.0-21    |
| XML           | 3.99-0.13 |
| methods       | 4.2.1     |
| xml2          | 1.3.3     |
| rio           | 0.5.29    |
| dplyr         | 1.0.10    |
| stringr       | 1.4.1     |
| fastmatch     | 1.1-3     |
| lubridate     | 1.8.0     |
| rlang         | 1.1.0     |
| inline        | 0.3.19    |
| purrr         | 0.3.4     |
| jsonlite      | 1.8.7     |
| sf            | 1.0-14    |
| sp            | 1.5-0     |
| leaflet       | 2.1.2     |
| rgdal         | 1.6-7     |
| parallel      | 4.2.1     |
| raster        | 3.6-23    |
| terra         | 1.7-39    |
| units         | 0.8-0     |
| tidyverse     | 1.3.2     |
| tictoc        | 1.1       |
| tidyr         | 1.2.0     |
| ggplot2       | 3.4.2     |
| lwgeom        | 0.2-13    |
| stars         | 0.6-4     |
| stringr       | 1.4.1     |
| fasterize     | 1.0.4     |


`install.packages("tools", version = "4.2.1")`  
`install.packages("installr", version = "0.23.4")`  
`install.packages("readxl", version = "1.4.1")`  
`install.packages("sets", version = "1.0-21")`  
`install.packages("XML", version = "3.99-0.13")`  
`install.packages("methods", version = "4.2.1")`  
`install.packages("xml2", version = "1.3.3")`  
`install.packages("rio", version = "0.5.29")`  
`install.packages("dplyr", version = "1.0.10")`  
`install.packages("stringr", version = "1.4.1")`  
`install.packages("fastmatch", version = "1.1-3")`  
`install.packages("lubridate", version = "1.8.0")`  
`install.packages("rlang", version = "1.1.0")`  
`install.packages("inline", version = "0.3.19")`  
`install.packages("purrr", version = "0.3.4")`  
`install.packages("jsonlite", version = "1.8.7")`  
`install.packages("sf", version = "1.0-14")`  
`install.packages("sp", version = "1.5-0")`  
`install.packages("leaflet", version = "2.1.2")`  
`install.packages("rgdal", version = "1.6-7")`  
`install.packages("parallel", version = "4.2.1")`  
`install.packages("raster", version = "3.6-23")`  
`install.packages("terra", version = "1.7-39")`  
`install.packages("dplyr", version = "1.0.10")`  
`install.packages("units", version = "0.8-0")`  
`install.packages("tidyverse", version = "1.3.2")`  
`install.packages("tictoc", version = "1.1")`  
`install.packages("tidyr", version = "1.2.0")`  
`install.packages("ggplot2", version = "3.4.2")`  
`install.packages("lwgeom", version = "0.2-13")`  
`install.packages("stars", version = "0.6-4")`  
`install.packages("stringr", version = "1.4.1")`  
`install.packages("fasterize", version = "1.0.4")`  

## 3.1 Data Transformation

While an ideal scenario would involve a fully dynamic system capable of automatically determining mapping transformations from one version of a data set to the next, this proved unattainable due to the overlapping use of names in the new GBRMPA database with the old data set in a different context. To address this challenge, a compromise between modularity and robustness was sought. Instead of hard-coding numerous transformations, a solution was implemented using JSON configuration files to specify transformations which are then checked against the input with NLP techniques and dynamically changed to ensure semantic differences can still be effectively mapped. This approach allows for flexibility in handling future datasets. The configuration files mean that any dataset can specify a configuration file and then utilise the work flow to ensure consistent data output. 

## 3.2 Error Checking & Processing


## 3.3 Site Assignment


## 3.4 Export Data

For further information see Reusable Digital Workflows Systems Diagrams and Reusable Digital Workflows Psudo Code Systems Diagrams

## 4.0 Code Documentation

#### Function: `main(new_path, configuration_path, kml_path, leg_path)`

- **Input:**
    - `leg_path`: path to the legacy data file
    - `new_path`: path to the new control data file
    - `configuration_path`: path to the control data specific configuration file 
    - `kml_path`: path to kml file containing all cull sites on the reef
- **Output:**
    - None
- **Description:**
    - This function is the main function that runs the data processing pipeline, creates a metadata report to document noteworthy information, assigns sites to the data if relevent and then exports it for scientific use.

#### Function: `import_data(data, control_data_type, is_powerBI_export, sheet)`

- **Input:**
  - `data`: file path to the file containing data desired to be in dataframe format
  - `configuration`: dataframe containing metadata and column mappings for control data 
- **Output:**
    - dataframe containing imported data
- **Description:**
    - This function reads data from a file and returns a dataframe. It determines the file type and reads the file using the appropriate method. 

#### Function: `create_metadata_report(control_data_type)`

- **Input:**
    - `control_data_type`: Specifies the control data for use in file name generation.
- **Output:**
    - None
- **Description:**
    - This function creates an empty XML file for to later document metadata and warnings surrounding the pipeline.
    
#### Function: `contribute_to_metadata_report(data, key="Warning")`
- **Input:**
    - `data`: Matrix or dataframe containing strings that describe the location and warning/error that occurred
    - `key`: an optional parameter specifying the node to be inserted under. Warning by default, any string is valid.
- **Output:**
    - None
- **Description:**
    - This function adds information to the XML metadata report from the information obtained in the previously executed function to the desired control data node.
    
#### Function: `separate_control_dataframe(new_data_df, legacy_data_df, control_data_type)`
- **Input:**
    - `new_data_df`: New control data exported from GBRMPA
    - `legacy_data_df`: Control data that most recently passed through workflow. In legacy format.
    - `control_data_type`: Key word that specifies type of control data. Options: "manta_tow", "cull" or "RHISS" 
- **Output:**
    - None
- **Description:**
    - Separates the incoming control data into three categories, new, perfect duplicate and discrepancy. Can be done with the authoritative ID or without depending on its reliability. 
        - Separation assuming the ID is authoritative utilises identifiers that are constructed from the data within the rows and table joins to conclusivley separate all data. 
        - Separation assuming non-authoritative ID utilises `matrix_close_matches_vectorised` & `vectorised_separate_close_matches` to determine the number of variations in a row from the original legacy output compared with the new input. Most likely matches are then determined based on number of variations. 
        Given that it is not possible to definitively know if a change / discrepancy was intentional or not both new and change entries will pass through the same validation checks and if passed will be accepted as usable and assumed to be. If failed, assumed to be a QA change. If failed, the data will be flagged. Failed discrepancies will check the original legacy entry, which if failed will be left as is.

#### Function: `flag_duplicates(new_data_df)`

- **Input:**
    - `new_data_df`: column names to map
- **Output:**
    - `new_data_df`: dataframe with updated column "error_flag"
- **Description:**
    - New entries need to be checked for duplicates. If there is more than one duplicate it can be assumed to be an error and the error flag set. This will use a similar identifier new_entries df. This will only flag the duplicate versions of the row as an error as it still contains new information there has just been multiple instances of data entry. Additionally no new entry should be a duplicate of any 'perfect duplicate' as legitimate duplicates would come from the same source and uploaded at the same time.

#### Function: `compare_discrepancies(new_data_df, legacy_data_df, discrepancies)`

- **Input:**
     - `new_data_df`: New control data exported from GBRMPA
     - `legacy_data_df`: Control data that most recently passed through workflow. In legacy format.
     - `discrepancies`: mapped indices indicating likely matches between `legacy_data_df` & `new_data_df` with variations in a number of columns.
- **Output:**
    - `output_df`: dataframe which contains original rows from `legacy_data_df` in place of any likely errors in `new_data_df` 
- **Description:**
    -New entries need to be checked for duplicates. If there is more than one duplicate it can be assumed to be an error and the error flag set. This will use a similar identifier new_entries df. This will only flag the duplicate versions of the row as an error as it still contains new information there has just been multiple instances of data entry. Additionally no new entry should be a duplicate of any 'perfect duplicate' as legitimate duplicates would come from the same source and uploaded at the same time.

#### Function: `map_column_names(column_names)`

- **Input:**
    - `new_data_df`: column names to map
- **Output:**
    - a vector of mapped column names
- **Description:**
    - This function maps column names from one format to another. It uses a lookup table to map the column names.

#### Function: `set_data_type(data_df, mapping)`
- **Inputs:**
  - `data_df`: A data frame to be updated.
  - `mapping`: dataframe that specifies a columns required data type. 
- **Outputs:**
  - `data_df`: The updated data frame.
- **Description:**
    - This function sets the data type of each column of a data frame based on mapping in a JSON file. It returns the updated data frame.
    
  
#### Function: `matrix_close_matches_vectorised(x, y, distance)`
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
  - This is a function that takes two matrices or data frames (x and y), and a distance value. The process returns a matrix `match_indices` containing the indices of the rows in x and y that have non-perfect matches within the specified distance. The function first pre-allocates memory for the match_indices matrix assuming the worst-case scenario of y_rows * x_rows possible matches. If this allocation fails due to insufficient memory, it tries again with a smaller allocation of 10,000,000 rows. Then, the function iterates through each row in x and compares it to every row in y in a vectoried manner. For each value in the row of x, the function evaluates whether it matches the corresponding value in the row of y. These logical values are then appended to the matches matrix. After iterating over every column, there will be a matrix of size (y_rows, x_cols), where each row represents a row in y and each column represents a column in x. A perfect matching row in y will have a corresponding row in matches exclusively containing TRUE. Given that TRUE is equivalent to 1, the rowSums function is used to determine the number of non-perfect matches. If this number is less than or equal to the specified distance, the row index, column index and distance from perfect match are stored in match_indices. The function uses a custom vectorised function store_index_vec to append matches to match_indices. Finally, the na.omit function is used to remove rows with NA values from match_indices before it is returned.
  
#### Operator: `%fin%`
- **Inputs:**
  - `x`: size n*m vector 
  - `y`: size a*b vector 
- **Outputs:**
  - size n vector 
- **Description:**
  - This is a faster implementation of the %in% operator that uses the fastmatch package. The fastmatch function is used to match the values in x to the values in table, and the resulting index is compared to 0. If it is greater than 0, then the value is found in the table and the function returns TRUE. If not, the function returns FALSE.
  
#### Function: `vectorised_separate_close_matches(close_match_rows)`
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
  
### Function: `verify_RHISS(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that the data in the tide, bleaching and macroalgae columns of the data frame are valid. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_voyage_dates(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - Check that voyage dates of observation are within in voyage dates and that none of the dates are NA. If Voyage dates are NA set start and end to min and max observation date. Check that voyage dates associated with a vessels voyage are unique (There should only be on departure and return date)

### Function: `
### Function: `verify_percentages(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all percentage values in the data frame are between 0 and 100. The function returns the input data frame `data_df` after altering a column called "error_flag"
(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all percentage values in the data frame are between 0 and 100. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_na_null(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function checks if any values in `data_df` are NA or NULL, and flags those rows as invalid by adding a "TRUE" value to the "error_flag" column. 
    
### Function: `verify_integers_positive(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all values in specified integer columns are positive integers.  The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `remove_leading_spaces(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` modified dataframe
- **Description:**
    - This function removes leading and trailing spaces from all entries in the specified data frame columns. The function returns the modified data frame.

### Function: `verify_coral_cover(data_df)`

- **Inputs:**
    - `data_df`: a data frame containing control data 
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all values in the "Hard Coral", "Soft Coral", and "Recently Dead Coral" columns of the data frame are valid coral cover descriptors ("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", or "5+"). The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_cots_scars(data_df)`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all values in the "COTS Scars" column of the data frame are valid ("a", "p", or "c"). The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `verify_cohort_count(data_df)`

- **Inputs:**
    - `data_df`: a data frame
- **Outputs:**
    - `data_df` dataframe with updated column "error_flag"
- **Description:**
    - This function verifies that all values in the "Cohort Count" column of the data frame are valid integers. The function returns the input data frame `data_df` after altering a column called "error_flag"

### Function: `find_one_to_one_matches(close_match_rows)`

- **Inputs:**
    - `close_match_rows`: a data frame or matrix with columns "x_index", "y_index" and "difference". This is the output of function `matrix_close_matches_vectorised`.
      - "x_index" is the row index of the row from dataframe x that has a close match to a row in dataframe y indicated by "y_index" with a distance of "distance"
      - "y_index" is the row index of the row from dataframe y that has a close match to a row in dataframe x indicated by "x_index" with a distance of "distance"
- **Outputs:**
    - None
- **Description:**
    - This function determines one-to-one matches between rows in two data frames, based on the output of the "find_close_matches" function. The function takes as input a data frame with columns "x_index", "y_index" and "difference", and determines which rows have one-to-one matches. A one-to-one match is defined as a row in the x_df data frame that has a single match in the y_df data frame, and vice versa. The function updates two global variables, "perfect_duplicate_indices" and "discrepancies_indices", which are data frames containing the row indices of perfect matches and discrepancies, respectively.
    
    # Function: `verify_entries(data_df, configuration)`

**Inputs:**
- `data_df`: Data frame containing control data to be verified.
- `configuration`: A configuration object containing metadata required for verification, including `ID_col` and `control_data_type`.

**Outputs:**
- `data_df`: Updated data frame with error flags added.

**Description:**
This function verifies the entries in the control data based on the provided configuration. It performs a series of checks on different aspects of the data, such as integers being positive, valid reef entries, valid percentages, latitude and longitude within specified ranges, and additional checks specific to the control data type (manta_tow, cull, or RHISS). Error flags are added to the data frame to indicate any issues found during verification.

# Function: `verify_lat_lng(data_df, max_val, min_val, columns, ID_col)`

**Inputs:**
- `data_df`: Data frame containing control data.
- `max_val`: Maximum value for latitude or longitude.
- `min_val`: Minimum value for latitude or longitude.
- `columns`: Columns to verify (e.g., c("Longitude", "Start Lng", "End Lng")).
- `ID_col`: Identifier column.

**Outputs:**
- `data_df`: Updated data frame with error flags added.

**Description:**
This function verifies latitude and longitude values in specified columns of the data frame. It checks if the values are within the specified range. If any values are out of range, error flags are added, and a warning message is generated.

# Function: `verify_scar(data_df)`

**Inputs:**
- `data_df`: Data frame containing control data.

**Outputs:**
- `data_df`: Updated data frame with error flags added.

**Description:**
This function checks for valid feeding scars in the "Feeding Scars" column of the data frame. It compares the values to a predefined set of valid scars ('a', 'p', 'c'). If invalid scars are found, error flags are added, and a warning message is generated.

# Function: `verify_tow_date(data_df)`

**Inputs:**
- `data_df`: Data frame containing control data.

**Outputs:**
- `data_df`: Updated data frame with error flags added.

**Description:**
This function approximates tow dates based on vessel and voyage if they do not exist. It identifies incomplete tow dates, estimates missing dates based on the same vessel and voyage, and sets error flags for rows with missing tow dates. Warning messages are generated for tow date estimations and rows with no tow dates.

# Function: `transform_data_structure(data_df, mappings, new_fields)`

**Inputs:**
- `data_df`: Data frame to be transformed.
- `mappings`: Data frame specifying mappings for existing columns.
- `new_fields`: Data frame specifying new fields to be added.

**Outputs:**
- `transformed_df`: Transformed data frame.

**Description:**
This function transforms the structure of the input data frame based on provided mappings and new fields. It maps existing columns, adds new fields with default values if necessary, and returns the transformed data frame.

# Function: `assign_nearest_method_c(kml_data, data_df, layer_names_vec, crs, raster_size=0.0005, x_closest=1, is_standardised=1, save_rasters=1)`

**Inputs:**
- `kml_data`: KML data containing reef polygons.
- `data_df`: Data frame containing manta tow entries.
- `layer_names_vec`: Vector of layer names.
- `crs`: Coordinate Reference System.
- `raster_size`: Size of the raster cells.
- `x_closest`: Number of closest sites to assign.
- `is_standardised`: Flag indicating whether to standardize extents.
- `save_rasters`: Flag indicating whether to save generated rasters.

**Outputs:**
- `updated_pts`: Updated data frame with the nearest site information added.

**Description:**
This function assigns nearest sites to manta tow entries using a method developed by Dr. Cameron Fletcher. It generates rasters, assigns nearest sites, and updates the input data frame with the nearest site information.

# Function: `get_centroids(data_df, crs, precision=0)`

**Inputs:**
- `data_df`: Data frame containing control data.
- `crs`: Coordinate Reference System.
- `precision`: Decimal places for rounding coordinates.

**Outputs:**
- `pts`: Spatial points representing the centroids of manta tows.

**Description:**
This function determines the centroids of manta tows based on latitude and longitude columns in the data frame. It creates geospatial points and returns them.

# Function: `assign_raster_pixel_to_sites(kml_data, layer_names_vec, crs, raster_size, x_closest=1, is_standardised=0)`

**Inputs:**
- `kml_data`: KML data containing reef polygons.
- `layer_names_vec`: Vector of layer names.
- `crs`: Coordinate Reference System.
- `raster_size`: Size of the raster cells.
- `x_closest`: Number of closest sites to assign.
- `is_standardised`: Flag indicating whether to standardize extents.

**Outputs:**
- `site_regions`: List of rasters with assigned nearest site values.

**Description:**
This function assigns raster pixel values based on the nearest site to manta tow entries. It creates rasters slightly larger than the bounding box of each layer in the KML file, assigns values based on distances to sites, and returns a list of site regions.

# Function: `site_names_to_numbers(site_names)`

**Inputs:**
- `site_names`: Vector of site names.

**Outputs:**
- Numeric vector representing site numbers.

**Description:**
This function extracts numeric site numbers from site names.

# Function: `simplify_reef_polyogns_rdp(kml_data)`

**Inputs:**
- `kml_data`: KML data containing reef polygons.

**Outputs:**
- `simplified_kml_data`: KML data with simplified reef polygons.

**Description:**
This function simplifies all reef polygons stored in a list retrieved from the KML file using the Ramer-Douglas-Peucker algorithm.

# Function: `polygon_rdp(polygon_points, epsilon=0.00001)`

**Inputs:**
- `polygon_points`: Matrix of polygon points.
- `epsilon`: Tolerance parameter for simplification.

**Outputs:**
- Simplified polygon matrix.

**Description:**
This function applies the Ramer-Douglas-Peucker algorithm to simplify a polygon based on a tolerance parameter.

# Function: `rdp(points, epsilon=0.00001)`

**Inputs:**
- `points`: Matrix of points.
- `epsilon`: Tolerance parameter for simplification.

**Outputs:**
- Simplified matrix of points.

**Description:**
This function applies the Ramer-Douglas-Peucker algorithm to simplify a line or set of points based on a tolerance parameter.

# Function: `perpendicularDistance(p, A, B)`

**Inputs:**
- `p`: Point coordinates.
- `A`: Start point of a line segment.
- `B`: End point of a line segment.
**Description:**
Calculate perpendicular distance of a point p from a line segment AB for RDP method.
