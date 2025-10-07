import os
import shutil
from datetime import datetime

class FileVersionManager:
    def __init__(self, base_dir="."):
        self.set_base_dir(base_dir)
    
    def set_base_dir(self, base_dir):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _get_next_version(self, filename):
        name, ext = os.path.splitext(filename)
        max_version = 0
        
        if os.path.exists(self.base_dir):
            for f in os.listdir(self.base_dir):

                if f.startswith(name + '_v') and f.endswith(ext):
                    try:
                        version_part = f[len(name + '_v'):len(f) - len(ext)]
                        version_num = int(version_part)
                        max_version = max(max_version, version_num)
                    except ValueError:
                        continue
        
        return (1 if max_version == 0 else max_version) + 1
    
    def save_version(self, file_path, content, comment="", write_log = True):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        
        filename = os.path.basename(file_path)
        name, ext = os.path.splitext(filename)
        version_num = self._get_next_version(filename)
        version_name = f"{name}_v{version_num}{ext}"
        version_path = os.path.join(self.base_dir, version_name)
        meta_path = os.path.join(self.base_dir, name)
        
        with open(version_path, 'x+', encoding='utf-8') as f:
            f.write(str(content))
        
        meta_file = f"{meta_path}.meta"
        open_type = 'x+' if not os.path.exists(meta_file) else 'r+'

        if(write_log):
            with open(meta_file, open_type) as f:
                content = f.read()
                f.seek(0)
                f.write(f"original: {file_path}\n")
                f.write(f"version: {version_num}\n")
                f.write(f"timestamp: {datetime.now().isoformat()}\n")
                f.write(f"comment: {comment}\n")

                if(len(content.strip()) > 0):
                    f.write(f'----------------------------------------\n\n{content}')
        
        return version_name
    

    def list_versions(self, filename):
        name, ext = os.path.splitext(filename)
        versions = []
        
        if os.path.exists(self.base_dir):
            for f in os.listdir(self.base_dir):
                if f.endswith('.meta'):
                    continue
                
                if f.startswith(name + '_v') and f.endswith(ext):
                    try:
                        version_part = f[len(name + '_v'):len(f) - len(ext)]
                        int(version_part)
                        versions.append(f)
                    except ValueError:
                        continue
        
        return sorted(versions, key=lambda x: int(x[len(name + '_v'):len(x) - len(ext)]))
    

    def get_latest_version(self, filename):
        versions = self.list_versions(filename)
        return versions[-1] if versions else None
    

    def restore_version(self, version_name, target_path):
        version_path = os.path.join(self.base_dir, version_name)
        if not os.path.exists(version_path):
            raise FileNotFoundError(f"Version {version_name} not found")
        
        shutil.copy2(version_path, target_path)
        return target_path
    

    def get_version_info(self, version_name):
        meta_file = os.path.join(self.base_dir, f"{version_name}.meta")
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as f:
                return f.read()
        return "No metadata available"
    

if __name__ == "__main__":
    # vm = FileVersionManager('/file/path/')
    
    # Save a version
    # version = vm.save_version("/file/path/fl.txt", "Previous to this one we had the vehicle category", '4th generations')
    # print(f"Saved version: {version}")
    
    # List versions
    # versions = vm.list_versions("fl.txt")
    # print(f"Versions: {versions}")
    
    # Restore a version
    # vm.restore_version(version, "restored_example.txt")
    ...