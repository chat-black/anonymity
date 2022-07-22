"""
Automatically test different sql statements, execute each query under sql/ in turn,
and move the information output by the algorithm to the target location
"""
import numpy as np
import subprocess
import os
import pandas as pd
import re
import shutil

# two variables to be written to /tmp/gtArg
number_threshold = 50000
dist_threshold = 1

def write_config():
    """
    write number_threshold and dist_threshold to /tmp/gtArg
    """
    with open('/tmp/gtArg', 'w') as fout:
        fout.write('{} {} 1000000 1000000\n'.format(number_threshold, dist_threshold))

def get_dist_list():
    """
    get the distance between Group Tree and QEP
    """
    with open('/tmp/gtDist') as fin:
        content = [line.strip() for line in fin if len(line.strip()) > 0]

    res = [float(line.split(' ')[-1]) for line in content]
    return res

def get_res(target_dir):
    with open('{}/result.txt'.format(target_dir)) as fin:
        content = [float(line.split(':')[-1].strip()) for line in fin if line[0] == '*']
    res = [float(target_dir)] + content
    return res  # [threshold, plan number, plan interestingness, total time]

def generate_data(target_dir):
    """
    Organize data under different thresholds into a csv file
    """
    pre_dir = os.getcwd()
    os.chdir(target_dir)

    num_pattern = re.compile(r'^0[0-9.]+')
    dir_list = [i for i in os.listdir() if os.path.isdir(i) and num_pattern.match(i)]
    dir_list.sort(key=lambda x: float(x))
    res = []
    for d in dir_list:
        res.append(get_res(d))

    res = np.array(res)
    res_dist = {'threshold': res[:, 0], 'number': res[:, 1], 'distance': res[:, 2], 'time': res[:, 3]}
    res_dist = pd.DataFrame(res_dist)
    res_dist.to_csv('result.csv', header=True, index=False, encoding='utf-8')

    os.chdir(pre_dir)

def get_info(file):
    file = file.strip()
    file = file.split('.')[0]
    return file.split('_')

def get_sql(file):
    file = 'sql/'+file
    with open(file) as fin:
        content = fin.read()

    content.strip()
    if content[:2] != 'ex':
        content = 'explain ' + content

    pre_statement = "set optimizer_sample_plans=on;set optimizer_samples_number=50000;"

    return pre_statement + content

def run_sql(db_name, sql):
    complete = subprocess.run(['psql', str(db_name), '-c', sql], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if complete.stderr:
        raise NameError(complete.stderr)

def move_result(target_dir, target_name, change_fun=None):
    pre_dir = os.getcwd()
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy('/home/{}/timeRecord.txt'.format(os.getlogin()), target_dir+'/'+target_name)
    os.chdir(target_dir)
    if change_fun is not None:
        change_fun(target_name)

    os.chdir(pre_dir)

def one_sql_main(file_name, filter_method):
    """
    deal with one sql
    Args:
        filter_method: GFP or GFP&cost(Cost)
    """
    global number_threshold
    global dist_threshold
    number_threshold = 1  # make sure the GFP strategy will run
    dist_threshold = 1

    db_name, _  = get_info(file_name)
    sql = get_sql(file_name)

    print('Initializing...')
    write_config()
    try:
        run_sql(db_name, sql)
    except NameError as ex:
        print(file_name, "wrong", ex)
        return
    
    max_dist = max(get_dist_list())+0.01
    dist_threshold = 0.0
    while dist_threshold < max_dist:
        print("dist_threshold: ", dist_threshold)
        write_config()
        try:
            run_sql(db_name, sql)
        except NameError as ex:
            print(file_name, ":", dist_threshold, ":", ex)
            return
        else:
            move_result("{}/{}/{}".format(file_name.split('.')[0], filter_method, round(dist_threshold,2)), 'result.txt')
        dist_threshold += 0.01

    if dist_threshold > max_dist:
        generate_data("{}/{}".format(file_name.split('.')[0], filter_method))

def main():
    files = os.listdir('sql')
    for file in files:
        print('execute ', file)
        # one_sql_main(file, 'Cost')  # 'GFP' or 'Cost'
        one_sql_main(file, 'GFP')  # 'GFP' or 'Cost'

if __name__ == '__main__':
    main()