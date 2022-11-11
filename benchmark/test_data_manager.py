import os
import sys
from math import floor
from time import time

from requests import get
from log4py import Logger

Logger.set_level("INFO")
SKIP_MK5 = True

URLS = {
    # "non_file_overlap": ["https://cloud.tsinghua.edu.cn/f/b08a9c7915944ff08a18/?dl=1"],
    # "non_file_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/8484c06f42fe4785bf54/?dl=1"],
    # "non_chunk_overlap": ["https://cloud.tsinghua.edu.cn/f/79f947090da749c78254/?dl=1"],
    # "non_chunk_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/3f627e0393fd47caab3a/?dl=1"],
    # "multi_page_overlap": ["https://cloud.tsinghua.edu.cn/f/730e04e2661546d19178/?dl=1"],
    # "multi_page_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/e82d3e1b6883452eb6f2/?dl=1"],
    # "massive_page_overlap": ["https://cloud.tsinghua.edu.cn/f/f43271c0a2164ac49b7e/?dl=1"],
    # "massive_page_overlap_aligned": ["https://cloud.tsinghua.edu.cn/f/33f130acae8d4fe1bf1a/?dl=1"],
    "non_chunk_overlap_1G": ["https://cloud.tsinghua.edu.cn/f/592813e35d94412690a3/?dl=1"],
    "non_chunk_overlap_2G": ["https://cloud.tsinghua.edu.cn/f/0eba3799311a4786a697/?dl=1"],
    "non_chunk_overlap_3G": ["https://cloud.tsinghua.edu.cn/f/8fa6f4f6195d45a48ea1/?dl=1",
                             "https://cloud.tsinghua.edu.cn/f/050cb5c4668e40d389d5/?dl=1"],
    "non_chunk_overlap_4G": ["https://cloud.tsinghua.edu.cn/f/3278c03ce41740579b87/?dl=1",
                             "https://cloud.tsinghua.edu.cn/f/5cbf109c602e4ad8ade8/?dl=1"],
    "non_chunk_overlap_1seq_9unseq": ["https://cloud.tsinghua.edu.cn/f/1af3aaf69ffb433e9123/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/761cba4b3c38403dbede/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/816fc6a236ec4d1a8d51/?dl=1"],
    "non_chunk_overlap_3seq_7unseq": ["https://cloud.tsinghua.edu.cn/f/15a952fe7846407a89bc/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/f7642ba5e7564dcd96d1/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/c3a6f6d529754562ac9b/?dl=1"],
    "non_chunk_overlap_7seq_3unseq": ["https://cloud.tsinghua.edu.cn/f/cb123c383723465599c1/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/1e7d38687d4042098663/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/6a3b6566d6ce433c8dcc/?dl=1"],
    "non_chunk_overlap_9seq_1unseq": ["https://cloud.tsinghua.edu.cn/f/e3b67fc945504a80b59e/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/bc8de8196da042fa8df1/?dl=1",
                                      "https://cloud.tsinghua.edu.cn/f/17bb2277894743f9a7c1/?dl=1"],
}

MD5_SUM = {
    "non_file_overlap": ["6b44c7d6e5a7a184b553070029332be2"],
    "non_file_overlap_aligned": ["a37fb632a3a0b0f7fedc9c5d1e2f29ea"],
    "non_chunk_overlap": ["8fea761e96b7e6266b27380a2c1fd196"],
    "non_chunk_overlap_aligned": ["5f045a3d1e00fe3d689b72a6b7fc2b65"],
    "multi_page_overlap": ["03133b9515f52ce381e8771ec2de7df8"],
    "multi_page_overlap_aligned": ["7fd7d9ea568ba26fae7f24e4c0c7e59c"],
    "massive_page_overlap": ["873c7ca87891a14c438048c51f4564a8"],
    "massive_page_overlap_aligned": ["2b95f2bd41e2e3c66e9478b5e5fe5888"],
    "non_chunk_overlap_1G": ["edbba822a37c126be3f276ac3d6ff9c0"],
    "non_chunk_overlap_2G": ["22f89c74c0c82eb00bff32c03e7749c3"],
    "non_chunk_overlap_3G": ["b08afaa19890cf459ed727864d9c3e7b", "60c0663ea672c42c031e70c4ee078b43"],
    "non_chunk_overlap_4G": ["83eed4b42d1ed35992111ad8999888c1", "a8052b9205e77164e68ec699ff3fabcd"],
    "non_chunk_overlap_9seq_1unseq": ["24b5893a103bca1b31147b5d56015e7b", "12640c92a6721c6864c6824502819581",
                                      "9fe8f7d89ab4ad6c142330d5e381136b"],
    "non_chunk_overlap_7seq_3unseq": ["6e03ec167d5c810c6c00baf6afe5e3eb", "417ddd11a6efac83c290e2fad4403182",
                                      "1716785c34dd616ca50962daa6910e83"],
    "non_chunk_overlap_3seq_7unseq": ["12cf542955f08925920c54591241f7dd", "152c5d2d7d06d28c760959fd3041ea41",
                                      "3d74c76ac8ba349af12c3f3811b53269"],
    "non_chunk_overlap_1seq_9unseq": ["88d5a62e146cbc7001d9812176dfd6b7", "5662161b5c45c546dc10bf4b8128f2bd",
                                      "378bf4d46392c1b8a50d208d176e7659"]
}

FILE_NAMES = {
    "non_file_overlap": ["non_file_overlap.7z"],
    "non_file_overlap_aligned": ["non_file_overlap_aligned.7z"],
    "non_chunk_overlap": ["non_chunk_overlap.7z"],
    "non_chunk_overlap_aligned": ["non_chunk_overlap_aligned.7z"],
    "multi_page_overlap": ["multi_page_overlap.7z"],
    "multi_page_overlap_aligned": ["multi_page_overlap_aligned.7z"],
    "massive_page_overlap": ["massive_page_overlap.7z"],
    "massive_page_overlap_aligned": ["massive_page_overlap_aligned.7z"],
    "non_chunk_overlap_1G": ["non_chunk_overlap_1G.7z.001"],
    "non_chunk_overlap_2G": ["non_chunk_overlap_2G.7z"],
    "non_chunk_overlap_3G": ["non_chunk_overlap_3G.7z.001", "non_chunk_overlap_3G.7z.002"],
    "non_chunk_overlap_4G": ["non_chunk_overlap_4G.7z.001", "non_chunk_overlap_4G.7z.002"],
    "non_chunk_overlap_1seq_9unseq": ["non_chunk_overlap_1seq_9unseq.7z.001", "non_chunk_overlap_1seq_9unseq.7z.002",
                                      "non_chunk_overlap_1seq_9unseq.7z.003"],
    "non_chunk_overlap_3seq_7unseq": ["non_chunk_overlap_3seq_7unseq.7z.001", "non_chunk_overlap_3seq_7unseq.7z.002",
                                      "non_chunk_overlap_3seq_7unseq.7z.003"],
    "non_chunk_overlap_7seq_3unseq": ["non_chunk_overlap_7seq_3unseq.7z.001", "non_chunk_overlap_7seq_3unseq.7z.002",
                                      "non_chunk_overlap_7seq_3unseq.7z.003"],
    "non_chunk_overlap_9seq_1unseq": ["non_chunk_overlap_9seq_1unseq.7z.001", "non_chunk_overlap_9seq_1unseq.7z.002",
                                      "non_chunk_overlap_9seq_1unseq.7z.003"]
}

GOOD_DATASET = {"non_file_overlap", "non_file_overlap_aligned", "non_chunk_overlap", "non_chunk_overlap_aligned",
                "multi_page_overlap", "multi_page_overlap_aligned", "massive_page_overlap",
                "massive_page_overlap_aligned"}


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

        # md5_right = MD5_SUM[dataset_name]
        for idx, url in enumerate(urls):
            while True:
                try:
                    if os.path.exists(os.path.join(self._data_dir, names[idx])):
                        break
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
                                    self._log.info(
                                        "Downloading " + names[idx] + " %.2f MB/ %.2fMB %.2f %% %d seconds remaining" % (
                                            total_size / 1024 / 1024, file_size / 1024 / 1024, total_size / file_size * 100.0,
                                            (time() - a) / (total_size / file_size) * (1 - total_size / file_size)))
                            code.close()
                        a = time() - a
                        if a != 0:
                            self._log.info("Write Speed" + str(floor((b / a) * 100) / 100) + " MB/s")
                        else:
                            self._log.info("Write Speed：0 MB/s")
                            break
                except Exception as e:
                    os.system("rm " + os.path.join(self._data_dir, names[idx]))
        #
        # if not SKIP_MK5:
        #     for idx, name in enumerate(names):
        #         md5_check = os.popen("md5sum " + os.path.join(self._data_dir, name))
        #         md5_sum = md5_check.read().split(" ")[0]
        #         if md5_sum != md5_right[idx]:
        #             self._log.error("Wanted md5: " + md5_right + ", but get md5: " + md5_sum)
        #             return False
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
        # if dataset_name not in GOOD_DATASET:
        #     dir_name = os.popen("ls " + os.path.join(self._data_dir, dataset_name)).read().split("\n")[0]
        #     os.system("mv " + os.path.join(self._data_dir, dataset_name, dir_name) + " " + os.path.join(self._data_dir,
        #                                                                                                 dataset_name,
        #                                                                                                 "data"))


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        SKIP_MK5 = bool(sys.argv[3])
    manager = TestDataManager(sys.argv[1])
    manager.download_test_data(sys.argv[2])
    manager.unzip()
