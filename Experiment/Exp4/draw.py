import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker
marker = ['o', 's', '*', '+']

def get_res(dir: str):
    """
    go to dir, read the csv file and return the data
    """
    cur_dir = os.getcwd()
    os.chdir(dir)

    target_file = [i for i in os.listdir() if len(i) > 3 and i[-3:] == 'csv']
    target_file = target_file[0]
    df = pd.read_csv(target_file, encoding='utf-8', index_col=False)
    res = dict(df)
    os.chdir(cur_dir)

    for key, value in res.items():
        res[key] = list(value)
    return res


def draw_time(x, time_gt, time_cost):
    """
    comparison of the run time
    """
    time_gt = [i for i in time_gt]
    time_cost = [i for i in time_cost]

    plt.cla()
    b_color = '#00838d'
    o_color = '#f28442'

    plt.plot(x, time_cost, color=o_color, lw=3, linestyle='--', label='GFP & Cost', marker=marker[1], markersize=12, markevery=10)
    plt.plot(x, time_gt, color=b_color, lw=3, label='GFP', marker=marker[0], markersize=12, markevery=10)

    plt.legend(fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    plt.xlabel('distance threshold', fontsize=22, fontweight='bold')
    plt.ylabel('time(ms)', fontsize=20, fontweight='bold')
    plt.tight_layout()
    plt.savefig('time.png', dpi=300)


def draw_number(x, number_gt, number_cost):
    """
    comparison of the filterd plan percentage
    """
    temp = 51871
    number_gt = [number_gt[-1]-i for i in number_gt]
    number_cost = [number_cost[-1]-i for i in number_cost]

    plt.cla()
    b_color = '#00838d'
    o_color = '#f28442'

    plt.plot(x, number_cost, color=o_color, lw=3, linestyle='--', label='GFP & Cost', marker=marker[1], markersize=12, markevery=10)
    plt.plot(x, number_gt, color=b_color, lw=3, label='GFP', marker=marker[0], markersize=12, markevery=10)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=temp, decimals=1))

    plt.legend(fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    plt.xlabel('distance threshold', fontsize=22, fontweight='bold')
    plt.ylabel('filtered plan percentage', fontsize=20, fontweight='bold')
    plt.tight_layout()
    plt.savefig('filterd_percentage.png', dpi=300)


def draw_dist(x, dist_gt, dist_cost):
    """
    comparison of plan interestingness
    """
    plt.cla()
    b_color = '#00838d'
    o_color = '#f28442'
    plt.plot(x, dist_cost, color=o_color, lw=3, linestyle='--', label='GFP & Cost', marker=marker[1], markersize=12, markevery=10)
    plt.plot(x, dist_gt, color=b_color, lw=3, label='GFP', marker=marker[0], markersize=12, markevery=10)

    plt.legend(fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    plt.xlabel('distance threshold', fontsize=22, fontweight='bold')
    plt.ylabel('plan interestingness', fontsize=22, fontweight='bold')
    plt.tight_layout()
    plt.savefig('plan_interestingness.png', dpi=300)


def draw_main(target_dir):
    pre_dir = os.getcwd()
    os.chdir(target_dir)
    gfp = get_res('GFP')
    cost = get_res('Cost')
    draw_time(gfp['threshold'], gfp['time'], cost['time'])
    draw_number(gfp['threshold'], gfp['number'], cost['number'])
    draw_dist(gfp['threshold'], gfp['distance'], cost['distance'])
    os.chdir(pre_dir)


if __name__ == '__main__':
    draw_main("tpch_22")
