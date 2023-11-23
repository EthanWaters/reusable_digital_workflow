@echo off
setlocal enabledelayedexpansion

:: Read current version from file
set /p current_version=<version.txt

:: Increment minor version
for /f "tokens=1,* delims=." %%a in ("!current_version!") do (
    set /a minor_version=%%b + 1
    set new_version=%%a.%%minor_version%%
)

:: Update version file
echo !new_version! > version.txt

:: Build and tag Docker image
docker build -t reusable_digital_workflow:!new_version! .
docker tag reusable_digital_workflow:!new_version! ghcr.io/ethanwaters/reusable_digital_workflow:!new_version!
docker push ghcr.io/ethanwaters/reusable_digital_workflow:!new_version!
pause