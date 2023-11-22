# Use a base image with R installed
FROM rocker/r-ver:4.2.1

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the contents of the current directory into the container at /usr/src/app
COPY . /usr/src/app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev

# Install R packages with specific versions
RUN R -e "install.packages('tools', version = '4.2.1', dependencies=TRUE)"
RUN R -e "install.packages('installr', version = '0.23.4', dependencies=TRUE)"
RUN R -e "install.packages('readxl', version = '1.4.1', dependencies=TRUE)"
RUN R -e "install.packages('sets', version = '1.0-21', dependencies=TRUE)"
RUN R -e "install.packages('XML', version = '3.99-0.13', dependencies=TRUE)"
RUN R -e "install.packages('methods', version = '4.2.1', dependencies=TRUE)"
RUN R -e "install.packages('xml2', version = '1.3.3', dependencies=TRUE)"
RUN R -e "install.packages('rio', version = '0.5.29', dependencies=TRUE)"
RUN R -e "install.packages('dplyr', version = '1.0.10', dependencies=TRUE)"
RUN R -e "install.packages('stringr', version = '1.4.1', dependencies=TRUE)"
RUN R -e "install.packages('fastmatch', version = '1.1-3', dependencies=TRUE)"
RUN R -e "install.packages('lubridate', version = '1.8.0', dependencies=TRUE)"
RUN R -e "install.packages('rlang', version = '1.1.0', dependencies=TRUE)"
RUN R -e "install.packages('inline', version = '0.3.19', dependencies=TRUE)"
RUN R -e "install.packages('purrr', version = '0.3.4', dependencies=TRUE)"
RUN R -e "install.packages('jsonlite', version = '1.8.7', dependencies=TRUE)"
RUN R -e "install.packages('sf', version = '1.0-14', dependencies=TRUE)"
RUN R -e "install.packages('sp', version = '1.5-0', dependencies=TRUE)"
RUN R -e "install.packages('leaflet', version = '2.1.2', dependencies=TRUE)"
RUN R -e "install.packages('rgdal', version = '1.6-7', dependencies=TRUE)"
RUN R -e "install.packages('parallel', version = '4.2.1', dependencies=TRUE)"
RUN R -e "install.packages('raster', version = '3.6-23', dependencies=TRUE)"
RUN R -e "install.packages('terra', version = '1.7-39', dependencies=TRUE)"
RUN R -e "install.packages('dplyr', version = '1.0.10', dependencies=TRUE)"
RUN R -e "install.packages('units', version = '0.8-0', dependencies=TRUE)"
RUN R -e "install.packages('tidyverse', version = '1.3.2', dependencies=TRUE)"
RUN R -e "install.packages('tictoc', version = '1.1', dependencies=TRUE)"
RUN R -e "install.packages('tidyr', version = '1.2.0', dependencies=TRUE)"
RUN R -e "install.packages('ggplot2', version = '3.4.2', dependencies=TRUE)"
RUN R -e "install.packages('lwgeom', version = '0.2-13', dependencies=TRUE)"
RUN R -e "install.packages('stars', version = '0.6-4', dependencies=TRUE)"
RUN R -e "install.packages('stringr', version = '1.4.1', dependencies=TRUE)"
RUN R -e "install.packages('fasterize', version = '1.0.4', dependencies=TRUE)"

# Specify the command to run your R script
CMD ["Rscript", "process_control_data.R"]
