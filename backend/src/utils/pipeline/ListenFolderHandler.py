import time
import threading
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ListenFolderHandler(FileSystemEventHandler):

    def __init__(self, allowed_extensions=None):
        self.allowed_extensions = allowed_extensions or {'.json', '.xml', '.csv'}
        self._in_progress = set()
        self._lock = threading.Lock()


    def _schedule_check(self, path_str: str):
        file_path = Path(path_str)

        # Ignore hidden/temp upload files (e.g., .filepart, .tmp, .DS_Store)
        if file_path.name.startswith(".") or file_path.suffix.lower() not in self.allowed_extensions:
            return

        # Prevent duplicate threads if both on_created and on_modified fire for the same upload
        with self._lock:
            if file_path in self._in_progress:
                return
            self._in_progress.add(file_path)

        threading.Thread(target=self._wait_until_ready, args=(file_path,), daemon=True).start()


    def _wait_until_ready(self, file_path: Path, cb = None):
        """Waits until FTP/SFTP or web upload finishes writing bytes before processing."""
        try:
            last_size = -1
            while True:
                if not file_path.exists():
                    return  # File was deleted or renamed mid-transfer
                current_size = file_path.stat().st_size
                if current_size > 0 and current_size == last_size:
                    break  # File size stopped growing -> transfer complete
                last_size = current_size
                time.sleep(1.0)  # Check every second for slow (e.g. FTP/SFTP streams)

            print(f"[READY FOR INGESTION] {file_path} ({last_size} bytes)")
            if cb: cb()

        finally:
            with self._lock:
                self._in_progress.discard(file_path)


    # 1. Standard creation (e.g. CLI copy, Python script write, basic web upload)
    def on_created(self, event):
        if not event.is_directory:
            self._schedule_check(event.src_path)


    # 2. Chunked writes (e.g. FTP/SFTP streaming data into an open file handle)
    def on_modified(self, event):
        if not event.is_directory:
            self._schedule_check(event.src_path)


    # 3. Atomic renames (e.g. macOS Finder paste, SFTP clients writing .filepart -> .xml)
    def on_moved(self, event):
        if not event.is_directory:
            self._schedule_check(event.dest_path)



def watch_folder(folder_path: str):
    path = Path(folder_path).resolve()
    path.mkdir(parents=True, exist_ok=True)

    observer = Observer()
    observer.schedule(ListenFolderHandler(), path=str(path), recursive=False)
    observer.start()
    print(f"Listening to {path} for uploads, (e.g. FTP/SFTP, and local transfers...)")

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()