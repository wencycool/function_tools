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


def run_sql(sql):
    lines = os.popen("db2 -ec -s -x +p \"%s\"" % sql).readlines()
    lines = map(lambda x: str(x).strip(), lines)
    recode = int(lines[-1])
    result = lines[:-1]
    if recode < 0:
        raise SQLError(result)
    return result


class Db2Info(object):
    def __init__(self, dbname):
        self._dbname = dbname
        self.__connect()

    def __connect(self):
        stdout, recode = command_run("db2 connect to %s" % self._dbname)
        if recode != 0:
            raise CommandRunError("Cannot connect to %s,msg:%s" % (self._dbname, stdout))

    # 查询当前时间戳
    def get_current_timestamp(self):
        return run_sql("values current timestamp")[0].strip()

    # 返回数据库参数字典
    def get_db_cfg(self):
        class dbcfg:
            name = ""
            value = ""  # 内存中值
            value_flags = ""  # NONE AUTOMATIC
            isint = False
        dbcfg_dict = {}
        lines = run_sql("select name,value,value_flags,datatype from TABLE(SYSPROC.DB_GET_CFG(-1)) with ur")
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
        lines = run_sql("select name,value,value_flags,datatype from TABLE(SYSPROC.DB_GET_CFG(-1)) with ur")
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


class MonGetPkgCacheStmt(Db2Info):
    # 除了字段名其它要以下划线开头,字段中不能有空格和换行符,不可以有stmt_text
    def __init__(self):
        self.EXECUTABLE_ID = None
        self.NUM_EXEC_WITH_METRICS = None
        self.TOTAL_ACT_TIME = None
        self.TOTAL_ACT_WAIT_TIME = None
        self.TOTAL_CPU_TIME = None
        self.POOL_READ_TIME = None
        self.POOL_WRITE_TIME = None
        self.PLANID = None
        self.STMTID = None
        self.SEMANTIC_ENV_ID = None

    # 获取综合查询SQL语句
    def __get_sql_order_by_unions(self, n):
        cols = ",".join(self.__get_cols())
        sql = '''select %s from (select * from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by NUM_EXEC_WITH_METRICS desc fetch first %d rows only with ur) a
                    union 
                  select %s from (select * from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by TOTAL_ACT_TIME desc fetch first %d rows only with ur) b
                    union
                  select %s from (select * from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by TOTAL_CPU_TIME desc fetch first %d rows only with ur) c
                    union
                  select %s from (select * from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by ROWS_READ desc fetch first %d rows only  with ur) d''' % \
            (cols, n, cols, n, cols, n, cols, n)
        return sql

    # 获取执行次数最多的topSQL
    def __get_sql_order_by_executions(self, n):
        cols = ",".join(self.__get_cols())
        sql = "select %s from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by NUM_EXEC_WITH_METRICS desc fetch first %d rows only with ur " % (cols, n)
        return sql

    # 获取执行时间最多的topSQL
    def __get_sql_order_by_act_time(self, n):
        cols = ",".join(self.__get_cols())
        sql = "select %s from table(mon_get_pkg_cache_stmt(null,null,null,null)) as t order by TOTAL_ACT_TIME desc fetch first %d rows only with ur " % (cols, n)
        return sql

    # 获取字段
    def __get_cols(self):
        return sorted([i for i in self.__dict__ if not i.startswith("_")])

    # 获取查询结果集
    def __get_result(self, sql):
        m_list = []
        lines = run_sql(sql)
        for line in lines:
            tmp = MonGetPkgCacheStmt()
            fields = line.split()
            cols = tmp.__get_cols()
            if len(fields) != len(tmp.__get_cols()):
                continue
            for i in xrange(len(cols)):
                if isinstance(fields[i], float):
                    setattr(tmp, cols[i], float(fields[i]))
                elif isinstance(fields[i], int):
                    setattr(tmp, cols[i], int(fields[i]))
                else:
                    setattr(tmp, cols[i], fields[i])
            m_list.append(tmp)
        return m_list

    # 结合STMTID、SEMANTIC_ENV_ID来定位SQL
    def get_top_stmt_dict_by_stmt_id(self, n=1000):
        stmt_dict = {}
        for m in self.__get_result(self.__get_sql_order_by_unions(n)):
            stmt_dict[(m.STMTID, m.SEMANTIC_ENV_ID)] = m
        return stmt_dict

    # 获取执行次数最多的TOPSQL语句
    def get_top_stmt_list_by_exections(self, n=1000):
        return self.__get_result(self.__get_sql_order_by_executions(n))

    # 获取执行时间最长的TOPSQL语句
    def get_top_stmt_list_by_actime(self,n=1000):
        return self.__get_result(self.__get_sql_order_by_act_time(n))


if __name__ == "__main__":
    try:
        Db2Info("sample")
        m = MonGetPkgCacheStmt()
        for l in m.get_top_stmt_dict_by_stmt_id().values():
            print l.STMTID, l.EXECUTABLE_ID, l.NUM_EXEC_WITH_METRICS
        for l1 in m.get_top_stmt_list_by_exections():
            print l1.EXECUTABLE_ID, l1.NUM_EXEC_WITH_METRICS
    except CommandRunError, e:
        print e
    finally:
        print("Done")
