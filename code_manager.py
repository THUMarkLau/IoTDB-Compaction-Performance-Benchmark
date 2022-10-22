import os
import re
import shutil
import sys
import zipfile
from log4py import Logger

Logger.set_level("INFO")


class CodeManager:
    def __init__(self, repository, branch, commit):
        self.log = Logger.get_logger(__name__)
        self.repository = repository
        self.branch = branch
        self.commit = commit
        self.root = os.getcwd()
        self.is_compile = False
        self.work_dir = ""

    def clone_repository(self):
        if os.system("git clone " + self.repository) != 0:
            self.log.error("Failed to clone IoTDB from " + self.repository)
            exit(-1)
        os.chdir("iotdb")

        if self.branch != "master":
            if os.system("git checkout --track origin/" + self.branch) != 0:
                self.log.error("Failed to checkout branch " + self.branch)

    def compile(self):
        os.chdir(os.path.join(self.root, "iotdb"))
        self.is_compile = (os.system("mvn clean package -pl server -am -DskipTests") == 0)
        os.chdir(self.root)

    def move_server_to_root(self):
        os.chdir(os.path.join(self.root, "iotdb", "server", "target"))
        found_file = None
        files = os.listdir(".")
        pattern = "iotdb-server-\d.\d+.\d-SNAPSHOT.zip"
        for file in files:
            if len(re.findall(pattern, file)) != 0:
                found_file = file
                break

        if found_file:
            shutil.copy(found_file, self.root)
            os.chdir(self.root)
            self.work_dir = os.path.join(self.root, found_file.replace(".zip", ""))
            # unzip the file
            fz = zipfile.ZipFile(found_file, 'r')

            for file in fz.namelist():
                fz.extract(file, self.work_dir)

            os.chdir(self.work_dir)
        else:
            self.log.error("Cannot found compressed file of iotdb-server")
            exit(-1)

    def start_iotdb(self):
        self.log.info("Starting IoTDB..")
        os.chdir(os.path.join(self.root, self.work_dir))
        if sys.platform != "win32":
            cmd = os.path.join(os.getcwd(), "sbin/start-server.sh > /dev/null 2>&1 &")
            os.system(cmd)
        else:
            cmd = os.path.join(os.getcwd(), "sbin/start-server.bat")
            os.system(cmd)

    def kill_iotdb(self):
        jps = os.popen("jps")
        res = jps.read().split('\n')
        pid = None
        for r in res:
            if r.find("IoTDB") != -1:
                pid = r.split(" ")[0]
                break

        if pid is None:
            self.log.info("No IoTDB instance running")

        if sys.platform != "win32":
            cmd = "kill -9 " + pid
        else:
            cmd = "taskkill /pid " + pid + " -f"

        os.system(cmd)

    def clean(self):
        self.log.info("Cleaning up")
        self.log.info("Removing " + self.work_dir + ".zip")
        os.remove(os.path.join(self.root, self.work_dir + ".zip"))
        self.log.info("Removing " + os.path.join(self.root, self.work_dir))
        os.removedirs(os.path.join(self.root, self.work_dir))
