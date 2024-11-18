clangd:
	cmake -B build -DCMAKE_TOOLCHAIN_FILE=toolchain.cmake -DCMAKE_BUILD_TYPE=Debug

install: clangd
	cmake --build build --parallel $(nproc) --target install

bench:
	sudo python3 tools/scripts/harness.py

analysis:
	python3 tools/scripts/analysis.py