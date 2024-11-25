cfg_debug:
	cmake -B build -DCMAKE_TOOLCHAIN_FILE=toolchain.cmake -DCMAKE_BUILD_TYPE=Debug

install_debug: cfg_debug
	cmake --build build --parallel $(nproc) --target install

bench:
	sudo python3 tools/scripts/harness.py

analysis:
	python3 tools/scripts/analysis.py

cfg_release:
	cmake -B build -DCMAKE_TOOLCHAIN_FILE=toolchain.cmake -DCMAKE_BUILD_TYPE=Release

install_release: cfg_release
	cmake --build build --parallel $(nproc) --target install

clean:
	rm -rf build
	rm babel_tower