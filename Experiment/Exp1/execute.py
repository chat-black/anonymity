"""
Automatically test different sql statements, execute each query under sql/ in turn,
and move the information output by the algorithm to the target location
"""
import subprocess
import os
import shutil

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
    shutil.copy('/home/wang/timeRecord.txt', target_dir+'/'+target_name)
    os.chdir(target_dir)
    if change_fun is not None:
        change_fun(target_name)

    os.chdir(pre_dir)

def cf_tips_effect(file):
    with open(file) as fin:
        content = fin.readlines()
    for i, line in enumerate(content):
        if line[:3] == "*to":
            content[i] = line.strip('*')
    
    with open(file, 'w') as fout:
        for line in content:
            fout.write(line)

def cf_tips_efficiency(file):
    with open(file) as fin:
        content = fin.readlines()
    for i, line in enumerate(content):
        if line[:2] == "*p":
            content[i] = line.strip('*')
    
    with open(file, 'w') as fout:
        for line in content:
            fout.write(line)

def main():
    files = os.listdir('sql')
    for file in files:
        print('execute ', file)
        sql = get_sql(file)
        db_name, sql_id = get_info(file)
        try:
            run_sql(db_name, sql)
        except NameError as ex:
            print(file, ' wrong', ex)
        else: 
        # you need to decide which code can be executed based on the currently executing algorithm

            # TIPS-H algorithm
            # move_result('effect/'+str(db_name)+'/'+str(sql_id), 'tips.txt', cf_tips_effect)
            # move_result('efficiency/'+str(db_name)+'/'+str(sql_id), 'tips.txt', cf_tips_efficiency)

            # TIPS-B algorithm
            # move_result('efficiency/'+str(db_name)+'/'+str(sql_id), 'tips_b.txt', cf_tips_efficiency)

            # Random algorithm
            # move_result('effect/'+str(db_name)+'/'+str(sql_id), 'random.txt', cf_tips_effect)
            # move_result('efficiency/'+str(db_name)+'/'+str(sql_id), 'random.txt', cf_tips_efficiency)

            # Cost algorithm
            move_result('effect/'+str(db_name)+'/'+str(sql_id), 'cost.txt', cf_tips_effect)
            move_result('efficiency/'+str(db_name)+'/'+str(sql_id), 'cost.txt', cf_tips_efficiency)

if __name__ == '__main__':
    main()