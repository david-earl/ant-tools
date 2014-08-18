#!/bin/bash

CWD=$(cd $(dirname "$0"); pwd)

MONO_SOURCE_DIR='/opt/mono-3.6.0'

platform=$(uname)

export PKG_CONFIG_PATH="$MONO_SOURCE_DIR/data/"
export C_INCLUDE_PATH="$MONO_SOURCE_DIR"

if [[ "$platform" == 'Linux' ]]; then
  echo "Building for linux..." 

  BIN_DIR="$CWD/bin/x86_64"

elif [[ "$platform" == 'Darwin' ]]; then
  echo "Building for OSX..." 

  BIN_DIR="$CWD/bin/darwing_x86_64"

  export CC="cc -lobjc -liconv -framework Foundation"

  ln -s /Library/Frameworks/Mono.framework/Versions/Current/lib/ "$MONO_SOURCE_DIR/../lib/"

elif [[ "$platform" == 'FreeBSD' ]]; then
  echo "Building for FreeBSD..." 
fi

xbuild /p:Configuration=Release ./AntReader/AntReader.sln

# switch to the directory of the target exe, which allows us to specify *.dll, which works around a mkbundle bug
cd AntReader/AntReader/bin/Release

if [ ! -d "$BIN_DIR" ]; then
  mkdir -p "$BIN_DIR"
fi


mkbundle --static --deps -o "$BIN_DIR/ant-tools" "$CWD/AntReader/AntReader/bin/Release/AntReader.exe" *.dll
