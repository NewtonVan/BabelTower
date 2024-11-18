clangd:
	cmake -B build -DCMAKE_TOOLCHAIN_FILE=toolchain.cmake -DCMAKE_BUILD_TYPE=Debug

install: clangd
	cmake --build build --parallel $(nproc) --target install