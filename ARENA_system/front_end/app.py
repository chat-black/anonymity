import datetime
import functools
import os

from flask import Flask, jsonify, render_template, request, make_response, wrappers
from flask_cors import *

import ARENA_config
from DB import DB
from QUERY import Query
from assistant import get_all_ip

app = Flask(__name__)
CORS(app, supports_credentials=True)

DB.init_db()
ARENA_config.init()


def set_uid(f):
    @functools.wraps(f)
    def wrapper(*args, **kw):

        uid = request.cookies.get('uid')
        if uid is None:
            uid = DB.get_new_uid()
            ok = False
        else:
            ok = True

        res = f(*args, **kw)

        if not ok:
            if type(res) != wrappers.Response:
                res = make_response(res)
            outdate = datetime.datetime.today() + datetime.timedelta(days=30)
            res.set_cookie("uid", str(uid), expires=outdate)
        return res

    return wrapper


@app.route("/sqlFile")
@set_uid
def sql_file():
    file_list = os.listdir("sql_file")
    print(file_list)
    return jsonify(file_list)


@app.route("/sqlFile/<filename>")
@set_uid
def load_file(filename):
    with open('sql_file/' + filename, encoding='utf-8') as fin:
        content = fin.read()
        return content


@app.route("/databaseInfo/<database_name>")
@set_uid
def load_database_info(database_name):
    db = DB()
    res = db.get_db_info(database_name)
    db.close()
    return jsonify(res)


@app.route("/search", methods=["POST"])
@set_uid
def search():
    print("execute TIPS")
    query = Query(request)
    res = query.run()
    return res


@app.route("/")
@set_uid
def index():
    return render_template('index.html')


@app.route("/suggest", methods=['POST'])
@set_uid
def receive_suggest():
    data = request.form
    uid = data['uid']
    if len(uid) > 0:
        uid = int(uid)

    DB.insert_suggest(uid, data['suggestion'])
    return jsonify("OK")


@app.route('/arena_inner/', methods=["POST"])
def insert_result():
    print("inner result")
    addr = request.remote_addr
    local_ip = get_all_ip()
    db = DB()
    if addr in local_ip:
        sha256 = request.form["hash"]
        sid = db.get_query_id(sha256)
        db.close()
        if sid is None:
            return jsonify("OK")

        data = {int(key): value for key, value in request.form.items() if key != "hash"}
        DB.insert_query_result(sid, data)
        return jsonify("OK")


@app.route('/arena_inner/conf', methods=['POST'])
def inner_get_config():
    print("inner config")
    sha256 = request.form['hash']
    temp = ARENA_config.get_config(sha256)
    if len(temp) > 0:
        temp = temp[0]
        ARENA_config.remove_config(sha256)
        return jsonify(temp[0] + ';' + temp[1])
    else:
        return jsonify("###")


@app.route('/arena_inner/i_stop/<pid>', methods=['GET'])
def inner_delete_pid(pid):
    db = DB()
    file_name = db.get_file_name(int(pid))
    db.close()

    DB.delete_pid(int(pid))
    if os.path.isfile(file_name):
        os.remove(file_name)
    return jsonify('OK')


if __name__ == '__main__':
    app.run(host='0.0.0.0')
