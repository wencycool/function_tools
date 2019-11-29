#!/usr/bin/python
# coding=utf8

import subprocess
import time
import sys
from ftplib import FTP
import os
import re


class CommandRunError(Exception):
    pass


class FileExistsError(Exception):
    pass


class FileNotExistsError(Exception):
    pass


# 设置与shell交互窗口,默认超时时间为2分钟
def command_run(command, timeout=30):
    proc = subprocess.Popen(command, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    poll_seconds = .250
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() is None:
        time.sleep(poll_seconds)
    if proc.poll() is None:
        if float(sys.version[:3]) >= 2.6:
            proc.terminate()
    stdout, stderr = proc.communicate()
    return str(stdout) + str(stderr), proc.returncode


# 获取当前实例数据库列表返回list
def get_database():
    db_list = []
    db_name = ""
    stdout, recode = command_run("db2 list db directory")
    if recode != 0:
        raise CommandRunError(stdout)
    for each_line in stdout.split("\n"):
        fields = map(lambda x: str(x).strip(), each_line.split("="))
        if len(fields) == 2 and fields[0] == "Database alias":
            db_name = fields[1]
        elif len(fields) == 2 and fields[0] == "Directory entry type" and db_name != "":
            db_list.append(db_name)
            db_name = ""
    return db_list


# 获取当前数据库版本和license信息
class Db2licm(object):
    def __init__(self):
        self.ProductName = ""
        self.LicenseType = ""
        self.ExpData = ""
        self.ProductIden = ""
        self.Version = ""
        self.__get_db2licm()

    def __get_db2licm(self):
        stdout, err = command_run("db2licm -l")
        if err != 0:
            raise CommandRunError(stdout)
        for line in stdout.split("\n"):
            fields = map(lambda x: str(x).strip().strip("\""), line.split(":"))
            if len(fields) == 2 and fields[0] == "Product name":
                self.ProductName = fields[1]
            elif len(fields) == 2 and fields[0] == "License type":
                self.LicenseType = fields[1]
            elif len(fields) == 2 and fields[0] == "Expiry date":
                self.ExpData = fields[1]
            elif len(fields) == 2 and fields[0] == "Product identifier":
                self.ProductIden = fields[1]
            elif len(fields) == 2 and fields[0] == "Version information":
                self.Version = fields[1]
                break


# 利用FTP从远程下载文件
class FTPDownload(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def __ftp_connect(self):
        ftp = FTP()
        ftp.set_debuglevel(0)  # 不调试
        ftp.connect(host=self.host, port=21)
        ftp.login(self.username, self.password)
        return ftp

    # filenames如果需要同时传多个文件，那么需要用逗号分开
    def download_file(self, ftp_file_path, dst_file_path, filenames):
        buffer_size = 10240
        ftp = self.__ftp_connect()
        file_list = ftp.nlst(ftp_file_path)
        need_download_file_list = map(lambda x: str(x).strip(), filenames.split(","))
        for filename in need_download_file_list:
            for each_file_name in file_list:
                if each_file_name == filename:
                    ftp_file = os.path.join(ftp_file_path, each_file_name)
                    write_file = os.path.join(dst_file_path, each_file_name)
                    if os.path.exists(write_file):
                        raise FileExistsError("file:%s exists!" % write_file)
                    with open(write_file, "wb") as f:
                        ftp.retrbinary("RETR {0}".format(ftp_file), f.write, buffer_size)
                        f.close()
        ftp.quit()
        return True


# 获取文件系统相关信息
class MountPoint(object):
    def __init__(self):
        self.FileSystem = ""
        self.TotalSize = 0
        self.UsedSize = 0
        self.AvalSize = 0
        self.UsedRatio = 0
        self.MPoint = ""


class MountPoints(object):
    def __init__(self):
        self.mountpoints = self.__get_mount_point_list()

    def __get_mount_point_list(self):
        mp_list = []
        stdout, recode = command_run("df -P")
        if recode != 0:
            raise CommandRunError(stdout)
        for line in stdout.split("\n"):
            if line.find("Filesystem") > -1:
                continue
            fields = re.compile(r'\s+').split(line, 5)
            if len(fields) != 6:
                continue
            mp = MountPoint()
            mp.FileSystem = fields[0]
            mp.TotalSize = int(fields[1])
            mp.UsedSize = int(fields[2])
            mp.AvalSize = int(fields[3])
            mp.UsedRatio = int(fields[4].rstrip("%"))
            mp.MPoint = fields[5]
            mp_list.append(mp)
        return mp_list

    def get_mount_poin_info(self,dir):
        mp_dict = {}
        for l in self.mountpoints:
            mp_dict[l.MPoint] = l
        cursor = os.path.dirname(dir)
        while 1:
            if cursor == "" or cursor == "/":
                return mp_dict["/"]
            if cursor in mp_dict:
                return mp_dict[cursor]
            else:
                cursor = os.path.dirname(cursor)
        return MountPoint()


if __name__ == "__main__":
    print get_database()
    licm = Db2licm()
    print licm.Version
    mp = MountPoints()
    print mp.mountpoints[0].MPoint
    print mp.get_mount_poin_info("/test/sdfa/tf").MPoint

