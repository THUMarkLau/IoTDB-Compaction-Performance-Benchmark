import os
from math import floor
from time import time

from requests import get
from log4py import Logger

Logger.set_level("INFO")

URLS = {
    "non_file_overlap": "https://cloud.tsinghua.edu.cn/f/b08a9c7915944ff08a18/?dl=1",
    "non_file_overlap_aligned": "https://cloud.tsinghua.edu.cn/f/8484c06f42fe4785bf54/?dl=1",
    "non_chunk_overlap": "https://cloud.tsinghua.edu.cn/f/79f947090da749c78254/?dl=1",
    "non_chunk_overlap_aligned": "https://cloud.tsinghua.edu.cn/f/3f627e0393fd47caab3a/?dl=1",
    "multi_page_overlap": "https://cloud.tsinghua.edu.cn/f/730e04e2661546d19178/?dl=1",
    "multi_page_overlap_aligned": "https://cloud.tsinghua.edu.cn/f/e82d3e1b6883452eb6f2/?dl=1",
    "massive_page_overlap": "https://cloud.tsinghua.edu.cn/f/f43271c0a2164ac49b7e/?dl=1",
    "massive_page_overlap_aligned": "https://cloud.tsinghua.edu.cn/f/33f130acae8d4fe1bf1a/?dl=1",
}


class TestDataManager:
    def __init__(self, data_dir="data"):
        self._data_dir = data_dir
        self._log = Logger.get_logger(__name__)
        if not os.path.exists(self._data_dir):
            os.mkdir(self._data_dir)

    def download_test_data(self, name="all"):
        if name == "all":
            for name, url in URLS:
                self._download_one_data_set(url, name)
        elif name in URLS:
            self._download_one_data_set(URLS[name], name)
        else:
            self._log.error("Cannot find", name, "in", str(URLS.keys()), ", failed to download")

    def _download_one_data_set(self, url, name):
        if os.path.exists(os.path.join(self._data_dir, name + ".7z")):
            return

        with get(url, stream=True) as r:
            if r.headers.get('Content-Length'):
                b = int(r.headers.get('Content-Length')) / 1024 / 1024
                print("ResourceSize:", b, "MB")
            else:
                print("ResourceSize: 0 MB")

            print('-' * 32)
            chunk_size = 1048

            print('-' * 32)
            print("Downloading")
            c = url.split(".")
            a = time()
            with open(os.path.join(self._data_dir, name + ".7z"), "wb") as code:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    code.write(chunk)
                    code.flush()
                code.close()
            a = time() - a
            if a != 0:
                print("Write Speed", floor((b / a) * 100) / 100, "MB/s")
            else:
                print("Write Speed：0 MB/s")
            print('-' * 32)


if __name__ == '__main__':
    manager = TestDataManager("test")
    manager.download_test_data("non_file_overlap")
