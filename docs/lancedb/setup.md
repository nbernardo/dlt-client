# Catalog Setup Changes

## Dependencies

`requirements.txt` has been updated with the following new entries:

```
lancedb
duckdb>=1.4.0
```

> ⚠️ **Action required:** If you already have the environment set up, you must re-run the environment setup before starting the backend. Skipping this step will cause the backend to fail at runtime.
>
> The recommended way is via `make install` — see below.  
> If you re-run/update dependencies installation via `pip install -r requirements.txt`, you must also run `python setup_extensions.py` afterwards and before starting the backend.

---

## New Files

### `setup_extensions.py`

Installs the DuckDB lance community extension. This is a one-time step required when setting up the environment — the extension is downloaded and cached locally so it does not impact pipeline runs.

### `Makefile`

The `Makefile` is now the recommended way to set up the environment. It runs both the `requirements.txt` installation and `setup_extensions.py` in the correct order in a single command.

---

## Setting Up the Environment

```bash
make install
```

This will:
1. Install all dependencies from `requirements.txt`
2. Run `setup_extensions.py` to install the DuckDB lance extension

---

## Installing Make

If `make` is not available on your system, install it first:

**Mac:**
```bash
# via Xcode Command Line Tools
xcode-select --install

# or via Homebrew
brew install make
```
> Full docs: https://developer.apple.com/xcode/resources/

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install build-essential
```
> Full docs: https://www.gnu.org/software/make/manual/

**Windows:**
```bash
# via Chocolatey
choco install make

# via Scoop
scoop install make

# via winget
winget install GnuWin32.Make
```
> Full docs: https://gnuwin32.sourceforge.net/packages/make.htm  
> Alternative: use WSL (Windows Subsystem for Linux) and follow the Linux instructions above.