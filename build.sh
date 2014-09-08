#!/bin/sh

EXE_NAME='ant-tools'

# If YOU DON'T HAVE A WORKING MONO DEV ENV, THIS MAY NEED TO BE SET FOR YOUR ENV
MONO_SOURCE_DIR='/path/to/source'

if [ -z `command -v mono` ]; then
    echo "Can't find mono--please check that mono is installed."
    kill -INT $$
fi

if [ -z `echo $PKG_CONFIG_PATH | grep "mono"` ]; then
    if [ "$MONO_SOURCE_DIR" == '/path/to/source' ]; then
        echo "Can't find mono source dir--please update 'MONO_SOURCE_DIR' in 'build.sh'"
        kill -INT $$
    fi

    export PKG_CONFIG_PATH="$MONO_SOURCE_DIR/data/"
    export C_INCLUDE_PATH="$MONO_SOURCE_DIR"
fi

platform=$(uname)

if [[ "$platform" == 'Linux' ]]; then
  cwd=$(readlink -f $(dirname ${BASH_SOURCE[0]}))

  echo "Building for linux..." 

  bin_dir="$cwd/bin/x86_64"

elif [[ "$platform" == 'Darwin' ]]; then
  echo "Building for OSX..." 

  cwd=$(cd "$(dirname "$0")"; pwd)

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

xbuild /p:Configuration=Release ./AntTools/AntTools.sln

# switch to the directory of the target exe, which allows us to specify *.dll, which works around a mkbundle bug
cd AntTools/AntTools.ConsoleApp/bin/Release

mkbundle --static --deps -o "$bin_path" "$cwd/AntTools/AntTools.ConsoleApp/bin/Release/AntTools.exe" *.dll

cd -

ln -sf $bin_path $EXE_NAME
