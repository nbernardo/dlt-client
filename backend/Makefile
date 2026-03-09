UV := $(shell command -v uv 2> /dev/null)

install: install-deps extensions

install-deps:
ifeq ($(UV),)
	pip install -r requirements.txt
else
	uv pip install -r requirements.txt
endif

extensions: install-deps
	python setup_extensions.py

.PHONY: install install-deps extensions