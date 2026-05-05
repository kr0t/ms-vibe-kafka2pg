#!/bin/sh

set -eu

BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WRAPPER_DIR="$BASE_DIR/.mvn/wrapper"
PROPERTIES_FILE="$WRAPPER_DIR/maven-wrapper.properties"

if [ ! -f "$PROPERTIES_FILE" ]; then
  echo "Missing $PROPERTIES_FILE" >&2
  exit 1
fi

DISTRIBUTION_URL=$(grep '^distributionUrl=' "$PROPERTIES_FILE" | cut -d'=' -f2-)

if [ -z "$DISTRIBUTION_URL" ]; then
  echo "distributionUrl is empty in $PROPERTIES_FILE" >&2
  exit 1
fi

ARCHIVE_NAME=$(basename "$DISTRIBUTION_URL")
ARCHIVE_PATH="$WRAPPER_DIR/$ARCHIVE_NAME"
MAVEN_DIR_NAME=$(printf '%s' "$ARCHIVE_NAME" | sed 's/-bin\.zip$//')
MAVEN_HOME="$WRAPPER_DIR/$MAVEN_DIR_NAME"
MVN_BIN="$MAVEN_HOME/bin/mvn"

download() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$DISTRIBUTION_URL" -o "$ARCHIVE_PATH"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -q "$DISTRIBUTION_URL" -O "$ARCHIVE_PATH"
    return
  fi

  echo "Neither curl nor wget is available to download Maven." >&2
  exit 1
}

extract() {
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "$ARCHIVE_PATH" -d "$WRAPPER_DIR"
    return
  fi

  if command -v jar >/dev/null 2>&1; then
    (
      cd "$WRAPPER_DIR"
      jar xf "$ARCHIVE_PATH"
    )
    return
  fi

  echo "Neither unzip nor jar is available to extract Maven." >&2
  exit 1
}

if [ ! -x "$MVN_BIN" ]; then
  rm -rf "$MAVEN_HOME"
  rm -f "$ARCHIVE_PATH"
  download
  extract
fi

exec "$MVN_BIN" "$@"
