import os
import sys
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

MD5_SUM = {
    "non_file_overlap": "6b44c7d6e5a7a184b553070029332be2"
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
            self._log.error("Cannot find " + name + " in " + str(URLS.keys()) + ", failed to download")

    def _download_one_data_set(self, url, name):
        if os.path.exists(os.path.join(self._data_dir, name + ".7z")):
            return

        md5_right = MD5_SUM[name]

        with get(url, stream=True) as r:
            file_size = 0
            if r.headers.get('Content-Length'):
                file_size = int(r.headers.get('Content-Length'))
                b = file_size / 1024 / 1024
                self._log.info("ResourceSize: " + str(b) + " MB")
            else:
                self._log.info("ResourceSize: 0 MB")

            chunk_size = 2048

            self._log.info("Downloading " + os.path.join(self._data_dir, name + ".7z"))
            a = time()
            total_size = 0
            chunk_num = 0
            with open(os.path.join(self._data_dir, name + ".7z"), "wb") as code:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    code.write(chunk)
                    code.flush()
                    total_size += len(chunk)
                    chunk_num += 1
                    if chunk_num % 1000 == 0:
                        self._log.info("Downloading " + name + ".7z %.2f MB/ %.2fMB %.2f %% " % (
                            total_size / 1024 / 1024, file_size / 1024 / 1024, total_size / file_size * 100.0))
                code.close()
            a = time() - a
            if a != 0:
                self._log.info("Write Speed" + str(floor((b / a) * 100) / 100) + " MB/s")
            else:
                self._log.info("Write Speed：0 MB/s")

        md5_check = os.popen("md5sum " + os.path.join(self._data_dir, name + ".7z"))
        md5_sum = md5_check.read().split(" ")[0]
        if md5_sum != md5_right:
            self._log.error("Wanted md5: " + md5_right +", but get md5: " + md5_sum)

    def unzip(self, name="all"):
        if name == "all":
            for n in URLS.keys():
                self._unzip_one_data_set(n)
        elif name in URLS:
            self._unzip_one_data_set(name)

    def _unzip_one_data_set(self, name):
        if os.path.exists(os.path.join(self._data_dir, name + ".7z")):
            if os.path.exists(os.path.join(self._data_dir, name)):
                return
            self._log.info("Unzipping " + name + ".7z")
            os.system(
                "7z x " + os.path.join(self._data_dir, name + ".7z") + " -r -o" + os.path.join(self._data_dir,
                                                                                               name))


if __name__ == '__main__':
    manager = TestDataManager(sys.argv[1])
    manager.download_test_data(sys.argv[2])
    manager.unzip()
