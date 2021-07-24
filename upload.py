import pathlib
import sys

from gdrive.gdrive import GDrive

gdrive = GDrive (auth_base_path = pathlib.Path.home () / ".gdriver")
folder_id = sys.argv [1]
for target_file in sys.argv [2:]:
    gdrive.upload (dest_folder_id = folder_id, file_path = pathlib.Path (target_file), chunk_size_bytes = 1 * 1024 * 1024)