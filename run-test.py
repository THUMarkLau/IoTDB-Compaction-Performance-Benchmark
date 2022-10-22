import multiprocessing
import os
import re
import shutil
import sys
import time
import zipfile

from parser import args

repository = args.repository
branch = args.branch
commit = args.commit
pid = os.popen("jps")
iotdb_process = None
zip_file_name = ""


def clone_repository():
    # if os.system("git clone " + repository) != 0:
    #     print("Failed to clone IoTDB from " + repository)
    #     exit(-1)
    os.chdir("iotdb")
    #
    # if branch != "master":
    #     if os.system("git checkout --track origin/" + branch) != 0:
    #         print("Failed to checkout branch " + branch)


def compile_and_move_to_the_directory():
    # compilation
    # os.system("mvn clean package -pl server -am -DskipTests")

    # move the
    os.chdir("server/target/")

    found_file = None
    files = os.listdir(".")
    pattern = "iotdb-server-\d.\d+.\d-SNAPSHOT.zip"
    for file in files:
        if len(re.findall(pattern, file)) != 0:
            found_file = file
            break

    if found_file:
        shutil.copy(found_file, "../../../")
        os.chdir("../../../")
        # unzip the file
        fz = zipfile.ZipFile(found_file, 'r')

        for file in fz.namelist():
            fz.extract(file, found_file.replace(".zip", ""))

        os.chdir(found_file.replace(".zip", ""))
        global zip_file_name
        zip_file_name = found_file
    else:
        exit(-1)


def run_iotdb():
    print("Starting IoTDB..")
    if sys.platform != "win32":
        cmd = os.path.join(os.getcwd(), "sbin/start-server.sh")
        os.system(cmd)
    else:
        cmd = os.path.join(os.getcwd(), "sbin/start-server.bat")
        print("Running " + cmd)
        os.system(cmd)


def start_up_iotdb():
    global iotdb_process
    iotdb_process = multiprocessing.Process(target=run_iotdb)
    iotdb_process.start()


def kill_iotdb():
    jps = os.popen("jps")
    res = jps.read().split('\n')
    for r in res:
        if r.find("IoTDB") != -1:
            pid = r.split(" ")[0]
            break

    if sys.platform != "win32":
        cmd = "kill -9 " + pid
    else:
        cmd = "taskkill /pid " + pid + " -f"

    os.system(cmd)


def clean_up():
    iotdb_process.kill()
    kill_iotdb()
    time.sleep(100)
    os.chdir("..")
    os.removedirs(zip_file_name.replace(".zip", ""))


if __name__ == '__main__':
    clone_repository()
    compile_and_move_to_the_directory()
    start_up_iotdb()
    time.sleep(5)
    clean_up()
