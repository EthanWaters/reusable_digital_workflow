#!/bin/bash

# Set the directory where you want to create the folders
baseDir=$(pwd)

# Function to create folders
createFolders() {
    if [ ! -d "$baseDir/Input/control_data" ]; then
        mkdir -p "$baseDir/Input/control_data"
    fi
    if [ ! -d "$baseDir/Input/reports" ]; then
        mkdir -p "$baseDir/Input/reports"
    fi
    if [ ! -d "$baseDir/Input/spatial_data" ]; then
        mkdir -p "$baseDir/Input/spatial_data"
    fi
    
    if [ ! -d "$baseDir/Output/control_data" ]; then
        mkdir -p "$baseDir/Output/control_data"
    fi
    if [ ! -d "$baseDir/Output/reports" ]; then
        mkdir -p "$baseDir/Output/reports"
    fi
    if [ ! -d "$baseDir/Output/spatial_data" ]; then
        mkdir -p "$baseDir/Output/spatial_data"
    fi
    
    if [ ! -d "$baseDir/Output/control_data/site_aggregation" ]; then
        mkdir -p "$baseDir/Output/control_data/site_aggregation"
    fi
    if [ ! -d "$baseDir/Output/control_data/unaggregated" ]; then
        mkdir -p "$baseDir/Output/control_data/unaggregated"
    fi
    
    # Create Auth folder
    if [ ! -d "$baseDir/Auth" ]; then
        mkdir "$baseDir/Auth"
    fi
    
    echo "Folders created successfully."
}

# Call the function to create folders
createFolders