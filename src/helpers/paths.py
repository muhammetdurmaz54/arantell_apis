import platform
from pathlib import Path

def project_root() -> Path:
    return str(Path(__file__).parent.parent.parent)

def get_directory(directory):
    if platform.system() != "Windows":
        package_path = project_root() + f"/{directory}/"
    else:
        package_path = project_root() + f"\\{directory}\\"
    return package_path

def get_package_directory():
    return get_directory("packages")

def get_asssets_directory():
    return get_directory("assets")

def get_docs_directory():
    return get_directory("docs")
