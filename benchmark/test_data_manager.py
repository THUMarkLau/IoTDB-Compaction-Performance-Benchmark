import os
import sys

from log4py import Logger
from requests import get

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
    "non_chunk_overlap_1G": ["6bf028528f837877aa0afd191987a830"],
    "non_chunk_overlap_2G": ["937d27f4727fae2fa8d72f5311208357"],
    "non_chunk_overlap_3G": ["61af6fd3c41f9827d7e2bf4eb8d27756", "2cf0198316ea4a70d12ec00f8445c686"],
    "non_chunk_overlap_4G": ["925d81afe50973b532c965f60b608d69", "e61ee5e49242cfe001da60780fe6966f"],
    "non_chunk_overlap_9seq_1unseq": ["5e4d8ef7f6d7264ec86e8b9d17e9bd7d", "7abffff0292c75ad52eef52da21f8a8b",
                                      "7181266ee46f0897d7c28c1ab236feb6"],
    "non_chunk_overlap_7seq_3unseq": ["650399f2d87c958f7914c239d1c6f069", "e3c20b28f05983fcd646e636ce8a120f",
                                      "e76c4168834147832bbca4260be897ed"],
    "non_chunk_overlap_3seq_7unseq": ["21287e76ef1228b482aac5a5bf59a6af", "470e602b6087df0741b7bbfd93b2ff41",
                                      "ac5f531a7fb8d81ffd80527b63cae4c1"],
    "non_chunk_overlap_1seq_9unseq": ["2ab754ca473af1d32b4923b522ddfc1e", "dcbad10220dd05abce0293f61086798b",
                                      "e34f5a5a1b309a68a22108cbc1cc459b"]
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
    "non_chunk_overlap_2G": ["non_chunk_overlap_2G.7z.001"],
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

        md5_right = MD5_SUM[dataset_name]
        for idx, url in enumerate(urls):
            while True:
                try:
                    if os.path.exists(os.path.join(self._data_dir, names[idx])):
                        md5_check = os.popen("md5sum " + os.path.join(self._data_dir, names[idx]))
                        md5_sum = md5_check.read().split(" ")[0]
                        if md5_sum == md5_right[idx]:
                            self._log.info(os.path.join(self._data_dir, names[idx]) + "exists and md5 is right, skip "
                                                                                      "it")
                            break
                        else:
                            self._log.info(os.path.join(self._data_dir, names[idx]) + "exists but md5 is wrong, delete "
                                                                                      "it and download again")
                            os.system("rm " + os.path.join(self._data_dir, names[idx]))
                    with get(url, stream=True) as r:
                        os.system("wget " + url + " -O " + os.path.join(self._data_dir, names[idx]))
                        md5_check = os.popen("md5sum " + os.path.join(self._data_dir, names[idx]))
                        md5_sum = md5_check.read().split(" ")[0]
                        if md5_sum == md5_right[idx]:
                            break
                        else:
                            self._log.error(
                                "Expect md5 for " + os.path.join(self._data_dir, names[idx]) + " is " + md5_right[
                                    idx] + ", but get " + md5_sum + ". Remove it and re-download again")
                            os.system("rm " + os.path.join(self._data_dir, names[idx]))
                except Exception as e:
                    os.system("rm " + os.path.join(self._data_dir, names[idx]))
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
