import fcntl
import hashlib
import os
import re
import signal
import subprocess
import time
import uuid

from flask import jsonify, make_response

from ARENA_config import write_config, remove_config
from DB import DB
from assistant import get_dbname


def _assemble_config(num, s, c, cost, lam, mode: str, gt_filter, join_num, edit_flag) -> str:
    """
    assemble the config to a string
    """
    return "{};{};{};{};{};{};{};{};{}".format(num, s, c, cost, lam, mode, gt_filter, join_num, edit_flag)


def _hash(s: str) -> str:
    """
    get the sha256 of a string
    """
    temp = s + '\n'
    return hashlib.sha256(temp.encode('ascii')).hexdigest()


class SQLStatement:

    @staticmethod
    def _clear(s: str) -> str:
        """
        clear the sql
        """
        s = s.strip()

        if s[:7] != 'explain':
            s = 'explain ' + s

        if s[-1] != ';':
            s = s + ';'

        return s

    @staticmethod
    def _join_num(sql: str) -> int:
        """
        calculating the number of join
        """
        pat = re.compile(r"[0-9a-zA-Z]+ = [a-zA-Z0-9]+")
        a = pat.findall(sql)
        return len(a)

    def __init__(self, statement: str):
        self.original = statement
        self.statement = SQLStatement._clear(statement)
        self.sha256 = _hash(self.statement)
        self.join_num = SQLStatement._join_num(self.statement)

        # because we change the source code of DB, the optimizer_samples_number won't work.
        # we will get the whole plan space regardless of this value.
        self.pre_statement = "set optimizer_sample_plans=on;set optimizer_samples_number=20000;"

    def final_sql(self) -> str:
        """
        pre_statement let DB perform ARENA related algorithms.
        """
        return self.pre_statement + self.statement


class Query:
    @staticmethod
    def make_query(sql: str, conf: str):
        return "{};{}".format(sql, conf)

    def __init__(self, req):
        """
        Args:
            req: request object in flask
        """
        form = req.form
        self.connect_db = get_dbname()
        self.sql = SQLStatement(form['statement'])
        if form['mode'] == 'false':
            self.mode = 'B'
        else:
            self.mode = 'I'
            self.pid = req.cookies.get('pid')
        self.s = form['s_weight']
        self.c = form['c_weight']
        self.cost = form['cost_weight']
        self.lam = form['lambda']
        self.num = form['number']
        self.filter = form['filter']
        self.edit_flag = 0
        if form['editFlag'] != 'false':
            self.edit_flag = 1
        self.conf = _assemble_config(self.num, self.s, self.c, self.cost, self.lam, self.mode, self.filter,
                                     self.sql.join_num, self.edit_flag)
        self.query_str = Query.make_query(self.sql.statement, self.conf)
        self.sha256 = _hash(self.query_str)

    def run(self):
        if self.mode == 'B':
            return self.b_apq()
        elif self.mode == 'I':
            return self.i_apq()

    def b_apq(self):
        print("B-TIPS")
        db = DB()
        res = None

        if db.insert_new_query(self.sha256, self.sql.statement, self.conf):
            print("wait for database")
            for i in range(6):
                res = db.get_query_result(self.sha256)
                if len(res) > 0:
                    db.close()
                    return jsonify(res)
                time.sleep(10)

        write_config(self.sql.sha256, self.conf, self.sha256)

        try:
            print("execute")
            complete = subprocess.run(["psql", self.connect_db, "-c", self.sql.final_sql()],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print("finished")
            if complete.stderr:
                raise NameError(complete.stderr)
        except Exception as ex:
            print(ex)
            res = None
        else:
            time.sleep(0.2)
            for i in range(100):
                res = db.get_query_result(self.sha256)
                if len(res) > 0:
                    break
                time.sleep(0.6)
        finally:
            remove_config(self.sql.sha256)
            db.close()
            if res is None or len(res) < 1 or res[0][0] is None:
                return jsonify({"status": False})
            else:
                return jsonify(res)

    def i_apq(self):
        def new_i_apq():
            """
            create a new I-TIPS query
            """
            write_config(self.sql.sha256, self.conf, temp_file_name)
            res = None

            try:
                p = subprocess.Popen(["psql", self.connect_db, "-c", self.sql.final_sql()],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                _, stderr = p.communicate(timeout=0.05)
                if len(stderr) > 0:
                    raise NameError(stderr)
            except subprocess.TimeoutExpired:
                try:
                    _wait_for_file(full_path)
                except NameError as ex:
                    print("wait file wrong : ", ex)
                else:
                    res, self.pid = _get_message_from_file(full_path)
                    DB.insert_pid(int(self.pid), full_path)
            except NameError as ex:
                os.remove('/tmp/{}'.format(temp_file_name))
            finally:
                remove_config(self.sql.sha256)

            if res is None:
                return jsonify({"status": False})
            else:
                res = make_response(jsonify({"data": res, "flag": {"clear": True, 'end': False}}))
                res.set_cookie('pid', self.pid, max_age=3600)
                return res

        def continue_i_apq():
            """
            Continue an existing I-TIPS
            """
            res = None
            os.kill(int(self.pid), signal.SIGUSR1)
            time.sleep(0.3)

            try:
                _wait_for_file(full_path)
            except NameError as ex:
                print("wait fill wrong", ex)
            else:
                res, _ = _get_message_from_file(full_path)

            if res is None:
                return jsonify({"status": False})
            else:
                if res.strip() == 'no more':
                    return jsonify({'flag': {'end': True, 'clear': False}})
                else:
                    return jsonify({"data": res, 'flag': {'end': False, 'clear': False}})

        distinct_message = '{};{};{};{}'.format(self.sha256, uuid.uuid4().hex, os.getpid(), time.time())
        temp_file_name = _hash(distinct_message)
        full_path = '/tmp/{}'.format(temp_file_name)

        db = DB()
        if self.pid is None or not db.exist_pid(self.pid):
            with open(full_path, 'w') as _:
                pass
            return new_i_apq()
        else:
            full_path = db.get_file_name(self.pid)
            return continue_i_apq()


def _wait_for_file(file_name):
    if not os.path.exists(file_name):
        raise NameError("file doesn't exist")

    while True:
        if os.path.getsize(file_name) < 10:
            time.sleep(0.01)
            continue
        break

    fin = open(file_name)
    fcntl.lockf(fin.fileno(), fcntl.LOCK_SH)
    fcntl.lockf(fin.fileno(), fcntl.LOCK_UN)
    fin.close()


def _get_message_from_file(file_name):
    with open(file_name) as fin:
        content = [line.strip() for line in fin if line.strip()[0] != '#']
    return content[:2]
