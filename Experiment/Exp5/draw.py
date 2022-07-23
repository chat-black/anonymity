"""
Go to each subfolder under effect and efficiency and draw a comparison diagram of different algorithms
"""
import os
import re
import subprocess

def get_all_dirs():
    num_start_pat = re.compile(r'^[0-9].*')
    res = [i for i in os.listdir() if os.path.isdir(i) and num_start_pat.match(i)]
    return res

def main():
    dirs = get_all_dirs()
    pre_dir = os.getcwd()
    for d in dirs:
        print(d)
        os.chdir(d)
        complete = subprocess.run(['python3', 'draw.py'], stdout=subprocess.PIPE)
        try:
            complete.check_returncode()
        except Exception as ex:
            print(d, "wrong", ex)
        os.chdir(pre_dir)


if __name__ == '__main__':
    main()
