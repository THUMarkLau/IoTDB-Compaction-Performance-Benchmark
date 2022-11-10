import os
import sys
from math import floor
from time import time

from requests import get
from log4py import Logger

Logger.set_level("INFO")
SKIP_MK5 = False

URLS = {
    "non_file_overlap": ["https://cloud.tsinghua.edu.cn/f/b08a9c7915944ff08a18/?dl=1"],
    "non_file_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/8484c06f42fe4785bf54/?dl=1"],
    "non_chunk_overlap": ["https://cloud.tsinghua.edu.cn/f/79f947090da749c78254/?dl=1"],
    "non_chunk_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/3f627e0393fd47caab3a/?dl=1"],
    "multi_page_overlap": ["https://cloud.tsinghua.edu.cn/f/730e04e2661546d19178/?dl=1"],
    "multi_page_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/e82d3e1b6883452eb6f2/?dl=1"],
    "massive_page_overlap": ["https://cloud.tsinghua.edu.cn/f/f43271c0a2164ac49b7e/?dl=1"],
    "massive_page_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/33f130acae8d4fe1bf1a/?dl=1"],
}

MD5_SUM = {
    "non_file_overlap": ["6b44c7d6e5a7a184b553070029332be2"],
    "non_file_overlap_aligned": ["a37fb632a3a0b0f7fedc9c5d1e2f29ea"],
    "non_chunk_overlap": ["8fea761e96b7e6266b27380a2c1fd196"],
    "non_chunk_overlap_aligned": ["5f045a3d1e00fe3d689b72a6b7fc2b65"],
    "multi_page_overlap": ["03133b9515f52ce381e8771ec2de7df8"],
    "multi_page_overlap_aligned": ["7fd7d9ea568ba26fae7f24e4c0c7e59c"],
    "massive_page_overlap": ["873c7ca87891a14c438048c51f4564a8"],
    "massive_page_overlap_aligned": ["2b95f2bd41e2e3c66e9478b5e5fe5888"]
}

FILE_NAMES = {
    "non_file_overlap": ["non_file_overlap.7z"],
    "non_file_overlap_aligned": ["non_file_overlap_aligned.7z"],
    "non_chunk_overlap": ["non_chunk_overlap.7z"],
    "non_chunk_overlap_aligned": ["non_chunk_overlap_aligned.7z"],
    "multi_page_overlap": ["multi_page_overlap.7z"],
    "multi_page_overlap_aligned": ["multi_page_overlap_aligned.7z"],
    "massive_page_overlap": ["massive_page_overlap.7z"],
    "massive_page_overlap_aligned": ["massive_page_overlap_aligned.7z"]
}


class TestDataManager:
    def __init__(self, data_dir="data"):
        self._data_dir = data_dir
        self._log = Logger.get_logger(__name__)
        if not os.path.exists(self._data_dir):
            os.mkdir(self._data_dir)

    def download_test_data(self, dataset_name="all"):
        if dataset_name == "all":
            for dataset_name in URLS.keys():
                while not self._download_one_data_set(dataset_name, URLS[dataset_name], FILE_NAMES[dataset_name]):
                    os.remove(os.path.join(self._data_dir, dataset_name + ".7z"))
        elif dataset_name in URLS.keys():
            while not self._download_one_data_set(dataset_name, URLS[dataset_name], FILE_NAMES[dataset_name]):
                os.remove(os.path.join(self._data_dir, dataset_name + ".7z"))
        else:
            self._log.error("Cannot find " + dataset_name + " in " + str(URLS.keys()) + ", failed to download")

    def _download_one_data_set(self, dataset_name, urls, names):
        if os.path.exists(os.path.join(self._data_dir, names[0])):
            if not SKIP_MK5:
                md5_right = MD5_SUM[dataset_name]
                for idx, name in enumerate(names):
                    md5_check = os.popen("md5sum " + os.path.join(self._data_dir, name))
                    md5_sum = md5_check.read().split(" ")[0]
                    if md5_sum != md5_right[idx]:
                        return False
            return True

        md5_right = MD5_SUM[dataset_name]
        for idx, url in enumerate(urls):
            with get(url, stream=True) as r:
                file_size = 0
                if r.headers.get('Content-Length'):
                    file_size = int(r.headers.get('Content-Length'))
                    b = file_size / 1024 / 1024
                    self._log.info("ResourceSize: " + str(b) + " MB")
                else:
                    self._log.info("ResourceSize: 0 MB")

                chunk_size = 2048

                self._log.info("Downloading " + os.path.join(self._data_dir, names[idx]))
                a = time()
                total_size = 0
                chunk_num = 0
                with open(os.path.join(self._data_dir, names[idx]), "wb") as code:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        code.write(chunk)
                        code.flush()
                        total_size += len(chunk)
                        chunk_num += 1
                        if chunk_num % 1000 == 0:
                            self._log.info("Downloading " + names[idx] + " %.2f MB/ %.2fMB %.2f %% " % (
                                total_size / 1024 / 1024, file_size / 1024 / 1024, total_size / file_size * 100.0))
                    code.close()
                a = time() - a
                if a != 0:
                    self._log.info("Write Speed" + str(floor((b / a) * 100) / 100) + " MB/s")
                else:
                    self._log.info("Write Speed：0 MB/s")

        if not SKIP_MK5:
            for idx, name in enumerate(names):
                md5_check = os.popen("md5sum " + os.path.join(self._data_dir, name))
                md5_sum = md5_check.read().split(" ")[0]
                if md5_sum != md5_right[idx]:
                    self._log.error("Wanted md5: " + md5_right + ", but get md5: " + md5_sum)
                    return False
        return True

    def unzip(self, name="all"):
        if name == "all":
            for n in URLS.keys():
                self._unzip_one_data_set(n)
        elif name in URLS:
            self._unzip_one_data_set(name)

    def _unzip_one_data_set(self, dataset_name):
        file_names = FILE_NAMES[dataset_name]
        for file_name in file_names:
            if not os.path.exists(os.path.join(self._data_dir, file_name)):
                return
        if os.path.exists(os.path.join(self._data_dir, dataset_name)):
            return
        self._log.info("Unzipping " + dataset_name + ".7z")
        os.system(
            "7z x " + os.path.join(self._data_dir, file_names[0]) + " -r -o" + os.path.join(self._data_dir,
                                                                                                       dataset_name))


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        SKIP_MK5 = bool(sys.argv[3])
    manager = TestDataManager(sys.argv[1])
    manager.download_test_data(sys.argv[2])
    manager.unzip()
