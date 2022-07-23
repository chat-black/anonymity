"""
Automatically test different sql statements, execute each query under sql/ in turn,
and move the information output by the algorithm to the target location
"""
import subprocess
import os
import shutil

sample_number = 1000

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


def write_config():
    """
    update the /tmp/gtArg
    """
    with open('/tmp/gtArg', 'w') as fout:
        fout.write('1000000000 1000000000 11 {}\n'.format(sample_number))


def main():
    files = os.listdir('sql')
    for file in files:
        print('execute ', file)
        shutil.copy('LapsCost', '/tmp/LapsCost')
        shutil.copy('plan_id.txt', '/tmp/plan_id.txt')
        sql = get_sql(file)
        db_name, _ = get_info(file)
        global sample_number
        for sample_number in range(1000, 6000, 1000):
            print("  ", sample_number)
            write_config()
            try:
                run_sql(db_name, sql)
            except NameError as ex:
                print(file, ' wrong', ex)
            else:
                move_result('{}/'.format(sample_number), 'result.txt')


if __name__ == '__main__':
    main()