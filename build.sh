#!/bin/bash

EXE_NAME='ant-tools'

# THIS NEEDS TO BE SET FOR YOUR ENV
MONO_SOURCE_DIR='/opt/mono-3.6.0'

export PKG_CONFIG_PATH="$MONO_SOURCE_DIR/data/"
export C_INCLUDE_PATH="$MONO_SOURCE_DIR"


cwd=$(cd $(dirname "$0"); pwd)

platform=$(uname)

if [[ "$platform" == 'Linux' ]]; then
  echo "Building for linux..." 

  bin_dir="$cwd/bin/x86_64"

elif [[ "$platform" == 'Darwin' ]]; then
  echo "Building for OSX..." 

  bin_dir="$cwd/bin/darwin_x86_64"

  export CC="cc -lobjc -liconv -framework Foundation"

  ln -s /Library/Frameworks/Mono.framework/Versions/Current/lib/ "$MONO_SOURCE_DIR/../lib/"

elif [[ "$platform" == 'FreeBSD' ]]; then
  echo "Building for FreeBSD..." 
fi

if [ ! -d "$bin_dir" ]; then
  mkdir -p "$bin_dir"
fi

bin_path="$bin_dir/$EXE_NAME"

xbuild /p:Configuration=Release ./AntReader/AntReader.sln

# switch to the directory of the target exe, which allows us to specify *.dll, which works around a mkbundle bug
cd AntReader/AntReader/bin/Release

mkbundle --static --deps -o "$bin_path" "$cwd/AntReader/AntReader/bin/Release/AntReader.exe" *.dll

cd -

ln -sf $bin_path $EXE_NAME
