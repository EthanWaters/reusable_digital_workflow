@echo off
setlocal

REM Set the directory where you want to create the folders
set "baseDir=%CD%"

REM Function to create folders
:CreateFolders
if not exist "%baseDir%\Input\control_data" mkdir "%baseDir%\Input\control_data"
if not exist "%baseDir%\Input\reports" mkdir "%baseDir%\Input\reports"
if not exist "%baseDir%\Input\spatial_data" mkdir "%baseDir%\Input\spatial_data"

if not exist "%baseDir%\Output\control_data" mkdir "%baseDir%\Output\control_data"
if not exist "%baseDir%\Output\reports" mkdir "%baseDir%\Output\reports"
if not exist "%baseDir%\Output\spatial_data" mkdir "%baseDir%\Output\spatial_data"

echo Folders created successfully.

:end