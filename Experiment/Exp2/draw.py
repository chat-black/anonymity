"""
Go to each subfolder under effect and efficiency and draw a comparison diagram of different algorithms
"""
import subprocess
import os

def get_all_dirs():
    def get_dirs(target):
        pre_dir = os.getcwd()
        os.chdir(target)
        sub_res = [target + '/' + i for i in os.listdir() if os.path.isdir(i)]
        os.chdir(pre_dir)
        return sub_res

    all_target = ['construct', 'kernel']
    res = []
    for i in all_target:
        res += get_dirs(i)

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
