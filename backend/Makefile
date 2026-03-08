install: pip_install extensions

pip_install:
	pip install -r requirements.txt

extensions: pip_install
	python setup_extensions.py

.PHONY: install pip_install extensions