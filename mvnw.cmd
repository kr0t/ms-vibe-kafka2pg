@echo off
setlocal

set BASE_DIR=%~dp0
set WRAPPER_DIR=%BASE_DIR%.mvn\wrapper
set PROPERTIES_FILE=%WRAPPER_DIR%\maven-wrapper.properties

if not exist "%PROPERTIES_FILE%" (
  echo Missing %PROPERTIES_FILE%
  exit /b 1
)

for /f "tokens=1,* delims==" %%A in (%PROPERTIES_FILE%) do (
  if "%%A"=="distributionUrl" set DISTRIBUTION_URL=%%B
)

if "%DISTRIBUTION_URL%"=="" (
  echo distributionUrl is empty in %PROPERTIES_FILE%
  exit /b 1
)

for %%F in ("%DISTRIBUTION_URL%") do set ARCHIVE_NAME=%%~nxF
set ARCHIVE_PATH=%WRAPPER_DIR%\%ARCHIVE_NAME%
set MAVEN_DIR_NAME=%ARCHIVE_NAME:-bin.zip=%
set MAVEN_HOME=%WRAPPER_DIR%\%MAVEN_DIR_NAME%
set MVN_BIN=%MAVEN_HOME%\bin\mvn.cmd

if not exist "%MVN_BIN%" (
  if exist "%MAVEN_HOME%" rmdir /s /q "%MAVEN_HOME%"
  if exist "%ARCHIVE_PATH%" del /q "%ARCHIVE_PATH%"
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest '%DISTRIBUTION_URL%' -OutFile '%ARCHIVE_PATH%'"
  if errorlevel 1 exit /b 1
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Path '%ARCHIVE_PATH%' -DestinationPath '%WRAPPER_DIR%' -Force"
  if errorlevel 1 exit /b 1
)

call "%MVN_BIN%" %*
