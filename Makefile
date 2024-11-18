clangd:
	cmake -B build -DCMAKE_TOOLCHAIN_FILE=toolchain.cmake -DCMAKE_BUILD_TYPE=Debug

install: clangd
	cmake --build build --parallel $(nproc) --target install

bench:
	sudo python3 harness.py