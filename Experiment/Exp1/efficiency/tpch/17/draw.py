"""
draw the figure
"""
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator


marker = ['o', 's', '^', '*']
def read_data(file_name):
    with open(file_name, encoding='utf-8') as fin:
        content = [line.strip() for line in fin]
        content = [line for line in content if len(line) == 0 or line[0] == '*']
    
    while len(content) > 0 and content[0] == '':
        content = content[1:]
    
    index = [i for i,line in enumerate(content) if line == '']
    index.append(len(content)+10)

    pre = 0
    res = []
    for now in index:
        temp = tuple(content[pre:now])
        res.append(temp)
        pre = now + 1

    return res


def clear_data(data):
    def get_num(single_data):
        line = single_data[0]
        line = line.split(':')[-1]
        line = line.strip()
        return int(line)

    def get_time(single_data):
        sub_res = 0.0
        line = single_data[1].split(':')[-1]
        sub_res += float(line.strip())
        return sub_res

    res = [(get_num(i), get_time(i)) for i in data]
    return res


def draw(data1, data2, data3, data4):
    num = [i[0] for i in data1]
    time1 = [i[1] for i in data1]
    time2 = [i[1] for i in data2]
    time3 = [i[1] for i in data3]
    time4 = [i[1] for i in data4]
    plt.plot(num, time4, marker=marker[0], color='#10B57B', label='B-TIPS-B', lw=2, markersize=12, linestyle='--')
    plt.plot(num, time1, marker=marker[0], color='#EB4169', label='B-TIPS-H', lw=2, markersize=12)
    plt.plot(num, time2, marker=marker[1], color='#00838d', label='Cost', lw=2, markersize=12)
    plt.plot(num, time3, marker=marker[2], color='#f28442', label='Random', lw=2, markersize=12)

    ax = plt.gca()
    ax.xaxis.set_major_locator(MultipleLocator(10))

    plt.legend(fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    plt.xlabel('number of alternative plans', fontsize=22, fontweight='bold')
    plt.ylabel('time (ms)', fontsize=22, fontweight='bold')
    plt.tight_layout()

    plt.savefig('efficiency.png', dpi=300)


if __name__ == '__main__':
    data1 = clear_data(read_data('tips.txt'))
    data2 = clear_data(read_data('cost.txt'))
    data3 = clear_data(read_data('random.txt'))
    data4 = clear_data(read_data('tips_b.txt'))
    draw(data1, data2, data3, data4)
    