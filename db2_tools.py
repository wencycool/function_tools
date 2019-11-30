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


class SQLError(Exception):
    pass


class FileExistsError(Exception):
    pass


class FileNotExistsError(Exception):
    pass


def command_run(command, timeout=5):
    proc = subprocess.Popen(command, bufsize=40960, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    poll_seconds = .250
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() is None:
        time.sleep(poll_seconds)
    if proc.poll() is None:
        if float(sys.version[:3]) >= 2.6:
            proc.terminate()
    stdout, stderr = proc.communicate()
    print stdout
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


class Db2Info(object):
    def __init__(self, dbname):
        self.dbname = dbname
        self.__connect()

    def __connect(self):
        stdout, recode = command_run("db2 connect to %s" % self.dbname)
        if recode != 0:
            raise CommandRunError("Cannot connect to %s,msg:%s" % (self.dbname, stdout))

    # 返回执行结果列表
    def __command(self, sql):
        lines = os.popen("db2 -ec -s -x +p \"%s\"" % sql).readlines()
        lines = map(lambda x: str(x).strip(), lines)
        recode = int(lines[-1])
        result = lines[:-1]
        if recode < 0:
            raise SQLError(result)
        return result

    # 查询当前时间戳
    def get_current_timestamp(self):
        return self.__command("values current timestamp")[0].strip()

    # 返回数据库参数字典
    def get_db_cfg(self):
        class dbcfg:
            name = ""
            value = ""  # 内存中值
            value_flags = ""  # NONE AUTOMATIC
            isint = False
        dbcfg_dict = {}
        lines = self.__command("select name,value,value_flags,datatype from TABLE(SYSPROC.DB_GET_CFG(-1)) with ur")
        for line in lines:
            fields = line.split()
            if len(fields) != 4:
                continue
            cfg = dbcfg()
            cfg.name, cfg.value, cfg.value_flags, datatype = fields
            if datatype in ['INTEGER', 'BIGINT']:
                cfg.isint = True

            dbcfg_dict[cfg.name] = cfg
        return dbcfg_dict

    # 返回实例参数字典
    def get_dbm_cfg(self):
        class dbmcfg:
            name = ""
            value = ""  # 内存中值
            value_flags = ""  # NONE AUTOMATIC
            isint = False
        dbmcfg_dict = {}
        lines = self.__command("select name,value,value_flags,datatype from TABLE(SYSPROC.DB_GET_CFG(-1)) with ur")
        for line in lines:
            fields = line.split()
            if len(fields) != 4:
                continue
            cfg = dbmcfg()
            cfg.name, cfg.value, cfg.value_flags, datatype = fields
            if datatype in ['INTEGER', 'BIGINT']:
                cfg.isint = True

            dbmcfg_dict[cfg.name] = cfg
        return dbmcfg_dict


if __name__ == "__main__":
    try:
        dbinfo = Db2Info("sample")
        print dbinfo.get_current_timestamp()
        print dbinfo.get_db_cfg()["APPLHEAPSZ".lower()].value, dbinfo.get_db_cfg()["APPLHEAPSZ".lower()].value_flags
    except CommandRunError, e:
        print e
    finally:
        print("Done")
