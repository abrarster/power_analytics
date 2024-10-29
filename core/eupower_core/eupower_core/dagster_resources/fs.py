from __future__ import annotations

import os
import logging
import pandas as pd
from dagster import ConfigurableResource
from typing import Optional

logger = logging.getLogger(__name__)


class FilesystemResource(ConfigurableResource):
    root_folder: str

    def get_writer(self, relative_folder: str) -> FsWriter:
        return FsWriter(self.root_folder, relative_folder)

    def get_reader(self, relative_folder: Optional[str] = None) -> FsReader:
        return FsReader(self.root_folder, relative_folder)


class FsBase:

    def __init__(self, root_folder: str, relative_folder: Optional[str] = None):
        self.root_folder = root_folder
        self.relative_folder = relative_folder
        if self.relative_folder is None:
            self.base_path = self.root_folder
        else:
            if self.relative_folder[-1] == "/":
                self.relative_folder = self.relative_folder.rstrip("/")
            self.base_path = "{root}/{relative_folder}".format(
                root=self.root_folder, relative_folder=self.relative_folder
            )


class FsWriter(FsBase):

    def __enter__(self):
        print(f"Entering FsWriter at {self.base_path}")
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Exiting FsWriter at {self.base_path}")

    def write_file(self, file_name: str, content: str | bytes | pd.DataFrame) -> None:
        os.makedirs(self.base_path, exist_ok=True)
        full_path = f"{self.base_path}/{file_name}"
        if isinstance(content, pd.DataFrame):
            content.to_csv(full_path, index=False)
        else:
            if isinstance(content, str):
                mode = "w"
            elif isinstance(content, bytes):
                mode = "wb"
            else:
                raise NotImplementedError
            with open(full_path, mode) as file:
                logger.info(f"Writing {full_path}")
                file.write(content)


class FsReader(FsBase):

    def list_files(self) -> list[str]:
        return list(os.listdir(self.base_path))
