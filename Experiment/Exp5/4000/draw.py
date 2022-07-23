import matplotlib.pyplot as plt

def get_data():
    """
    get the data fron result.txt
    """
    with open('result.txt') as fin:
        content = [float(line.strip('* \t\n')) for line in fin if len(line) > 0 and line[0] == '*']
    l = len(content) // 2
    rd = content[:l]
    aos = content[l:2*l]
    return rd, aos


def draw(rd, laps):
    """
    Args:
        rd: the result of random
        laps: the result of LAPS
    """
    marker = ['o', 's', '^', '*']
    xlist = list(range(1, len(rd)+1))
    plt.plot(xlist, rd, marker=marker[0], color='#00838d', label='Random', lw=2, markersize=12)
    plt.plot(xlist, laps, marker=marker[1], color='#f28442', label='Laps', lw=2, markersize=12)

    plt.legend(fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    plt.xlabel('number of alternative plans', fontsize=22, fontweight='bold')
    plt.ylabel('plan interestingness', fontsize=22, fontweight='bold')
    plt.tight_layout()

    plt.savefig('Laps.png', dpi=300)


def main():
    rd, aos = get_data()
    draw(rd, aos)

if __name__ == '__main__':
    main()