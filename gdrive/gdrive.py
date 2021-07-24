import json
import mimetypes
import pathlib
from typing import Optional, Union, BinaryIO

from progress.bar import Bar
import requests

class GDrive:
    def __init__ (self, auth_base_path: pathlib.Path):
        self.creds_file_path = auth_base_path / "credentials.json"
        self.token_file_path = auth_base_path / "token.json"
    def get_from_creds (self, *args, **kwargs) -> str: return self._get_from (*args, **kwargs, _path = self.creds_file_path, _root = "installed")
    def get_from_token (self, *args, **kwargs) -> str: return self._get_from (*args, **kwargs, _path = self.token_file_path)
    def save_to_creds (self, *args, **kwargs) -> None: return self._save_to (*args, **kwargs, _path = self.creds_file_path, _root = "installed")
    def save_to_token (self, *args, **kwargs) -> None: return self._save_to (*args, **kwargs, _path = self.token_file_path)
    def _get_from (self, key: str, _path: pathlib.Path, _root: Optional [str] = None) -> str:
        with open (_path, "r") as in_file:
            val = json.load (in_file)
            if _root is not None: val = val [_root]
            return val [key]
    def _save_to (self, key: str, val: str, _path: pathlib.Path, _root: Optional [str] = None) -> None:
        with open (_path, "r") as in_file:
            data = json.load (in_file)
        container = data
        if _root is not None: container = container [_root]
        container [key] = val
        with open (_path, "w") as out_file:
            json.dump (data, out_file)
    def upload (self, dest_folder_id: str, file_path: pathlib.Path):
        if file_path.is_dir ():
            subdir_id = self._make_subdir (dest_folder_id, file_path.name)
            for subitem in file_path.iterdir ():
                self.upload (dest_folder_id = subdir_id, file_path = subitem)
        else:
            self._upload (dest_folder_id = dest_folder_id, file_path = file_path)
    def _make_subdir (self, parent_folder_id: str, folder_name: str) -> str:
        return self._make_request ("POST", url = "https://www.googleapis.com/drive/v3/files", is_json = True, json = {
            "parents": [parent_folder_id],
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder"
        }) ["id"]
    def _upload (self, dest_folder_id: str, file_path: pathlib.Path):
        mime_type = mimetypes.guess_type (file_path.name) [0]
        if mime_type is None: mime_type = "application/octet-stream"
        initial_response = self._make_request ("POST", url = "https://www.googleapis.com/upload/drive/v3/files", is_json = False, params = {"uploadType": "resumable"}, json = {
            "parents": [dest_folder_id],
            "name": file_path.name,
            "mimeType": mime_type
        })

        session_uri = initial_response.headers ["Location"]
        file = open (file_path, "rb")

        total_size: int = file_path.stat ().st_size
        chunk_size_bytes = total_size // 100

        chunk_count = total_size // chunk_size_bytes
        last_chunk_size = total_size % chunk_size_bytes
        has_last_chunk = last_chunk_size > 0
        progress_bar = Bar (file_path.name, max = total_size, suffix = "%(percent).1f%% - %(eta)ds")
        for chunk_index in range (chunk_count):
            chunk_start = chunk_size_bytes * chunk_index
            chunk_end = (chunk_start + chunk_size_bytes) - 1
            self._upload_chunk (session_uri = session_uri, chunk_size = chunk_size_bytes, chunk_start = chunk_start, chunk_end = chunk_end, total_size = total_size, file = file)
            progress_bar.next (chunk_size_bytes)
        if has_last_chunk:
            self._upload_chunk (session_uri = session_uri, chunk_size = last_chunk_size, chunk_start = total_size - last_chunk_size, chunk_end = total_size - 1, total_size = total_size, file = file)
            progress_bar.next (last_chunk_size)
        progress_bar.finish ()
    def _upload_chunk (self, session_uri: str, chunk_size: int, chunk_start: int, chunk_end: int, total_size: int, file: BinaryIO) -> int:
        file.seek (chunk_start)
        chunk_data = file.read (chunk_size)
        response = self._make_request ("POST", url = session_uri, is_json = False, headers = {
            "Content-Length": str (chunk_size),
            "Content-Range": f"bytes {chunk_start}-{chunk_end}/{total_size}"
        }, data = chunk_data)
        response.raise_for_status ()
        return response.status_code
    def _make_request (self, *args, **kwargs):
        try:
            return self.__make_request (*args, **kwargs)
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code != 401: raise
            refresh_response = requests.post (self.get_from_creds ("token_uri"), params = {
                "client_id": self.get_from_creds ("client_id"),
                "client_secret": self.get_from_creds ("client_secret"),
                "refresh_token": self.get_from_token ("refresh_token"),
                "grant_type": "refresh_token"
            })
            refresh_response.raise_for_status ()
            refresh_json = refresh_response.json ()
            self.save_to_token ("access_token", refresh_json ["access_token"])
            self.save_to_token ("token_type", refresh_json ["token_type"])

            return self.__make_request (*args, **kwargs)
    def __make_request (self, method: str, url: str, is_json: bool, **kwargs) -> Union [requests.Response, dict]:
        headers = {f"Authorization": f"{self.get_from_token ('token_type')} {self.get_from_token ('access_token')}"}
        if "headers" in kwargs:
            headers.update (kwargs ["headers"])
            del kwargs ["headers"]
        response = requests.request (method = method, url = url, headers = headers, **kwargs)
        response.raise_for_status ()
        return response.json () if is_json else response