//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC Corp.
//
//	@filename:
//		CEngine.cpp
//
//	@doc:
//		Implementation of optimization engine
//---------------------------------------------------------------------------
//************************ARENA**************************/
//	1、当前 ARENA 找到 k 个结果的方法是优化过的方法
//	2、后缀树计算与自己的距离采用优化过的方法
//************************ARENA**************************/
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/Edge.h"
#include "gpopt/engine/Sender.h"
#include "gpopt/engine/json.hpp"
#include "gpopt/engine/JSON.h"
#include "gpos/io/ARENAstream.h"
#include "ARENA_global.h"
#include <thread>
#include <mutex>
#include <queue>
#include <utility>
#include <chrono>

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/exception.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CPattern.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalMotionGather.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CBinding.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CJob.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CMemo.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"
#include "gpopt/xforms/CXformFactory.h"
#include "naucrates/traceflags/traceflags.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sstream>
#include <queue>
#include <cassert>
#include <cmath>
#include <cfloat>
#include <cstdio>
#include <random>
#include <ctime>
#include <chrono>
#include <list>
#include <set>


//************************ARENA**************************/
// MinDist 结构 用于 FindK 函数中进行剪枝
struct MinDist {
	int index;  // plan 在数组中的索引
	std::size_t distNum;  // 当前的最小距离是找出 distNum 个结果后的值，当只有 QEP 时，distNum 是0
	double dist;  // 当前的最小距离

	MinDist() {
		index = 0;
		distNum = 0;
		dist = 0.0;
	}

	bool operator< (const MinDist other) const {
		return dist < other.dist;
	}

	bool operator> (const MinDist other) const {
		return dist > other.dist;
	}
};

struct TreeEdit
{

    struct mapT {
        int a;
        int b;
        bool ref;
    };

    enum matchT { DELA, DELB, MATCH };
    struct resultT
    {
        // 构造函数
        resultT(matchT match = MATCH, int cost = 10000) : match(match), cost(cost), map() {}

        matchT match;  // 匹配的形式，删除A的节点或者删除B的节点或者是 MATCH
        int cost;
        std::vector<mapT> map;
    };

    struct nodeT {
        int id;
        std::wstring type;
        int leaf;
        bool key;  // 这个是什么？
        int size;
    };

	    typedef std::vector<resultT> VR;
    typedef std::vector<VR> VVR;
    typedef std::vector<int> VI;
    typedef std::vector<VI> VVI;
    typedef std::vector<JSONValue*> VJ;
    typedef std::vector<bool> VB;
    typedef std::vector<nodeT> VN;

    // 类中的全局变量
    VVR dp, dp2;
    VN postA, postB;

    JSONValue* readJSON(std::string & json_str) {
        JSONValue* ans = JSON::Parse(json_str.c_str());
        return ans;
    }

    int getSize(JSONValue* root) {
        while (root->AsObject().at(L"children")->AsArray().size() > 0)
            root = root->AsObject().at(L"children")->AsArray().back();
        return static_cast<int>(root->AsObject().at(L"id")->AsNumber() + 1);
    }

    // leaf: 叶子节点的数量
    // key:
    // size:
    nodeT makeNode(JSONValue* node, int leaf, bool key, int size)
    {
        nodeT n;
        // 设置 type
        n.type = node->AsObject().at(L"type")->AsString();

        // 设置 id
        n.id = static_cast<int>(node->AsObject().at(L"id")->AsNumber());
	
        // 设置 id
        n.id = static_cast<int>(node->AsObject().at(L"id")->AsNumber());

        // 设置 leaf, key 和 size
        n.leaf = leaf;
        n.key = key;
        n.size = size;

        return n;
    }

    int postorder(JSONValue* node, VN& post, bool isKey)
    {
        const JSONArray& children = node->AsObject().at(L"children")->AsArray();  // 取得所有的子节点
        int ans = -1;
        int size = 1;
        for (std::size_t i = 0; i < children.size(); i++)  // 遍历每个子节点
        {
            int tmp = postorder(children[i], post, i > 0);  // 对子节点进行后序遍历 (i>0 才是key)
            size += post[post.size() - 1].size;  // 最后一个节点的 size，nodeT 的 size
            if (ans == -1)
                ans = tmp;
        }
        if (ans == -1)
            ans = post.size();  // vector 的 size

          // ans 代表叶子节点的数量
        post.push_back(makeNode(node, ans, isKey, size));  // node (int)leaf (bool)key (int)size
        return ans;
    }

    inline int match(const nodeT& a, const nodeT& b) {
        if (a.type != b.type) return 1;
        return 0;
    }

	    void matchTree(const VN& A, const VN& B, int a, int b)
    {
        dp2[0][0].cost = 0;
        int leafA = A[a].leaf, leafB = B[b].leaf;  // A节点的叶子数量
        for (int i = leafA; i <= a; i++)
        {
            dp2[i - leafA + 1][0].cost = dp2[i - leafA][0].cost + 1;
            dp2[i - leafA + 1][0].match = DELA;
        }
        for (int j = leafB; j <= b; j++)
        {
            dp2[0][j - leafB + 1].cost = dp2[0][j - leafB].cost + 1;
            dp2[0][j - leafB + 1].match = DELB;
        }

        for (int i = leafA; i <= a; i++) {
            for (int j = leafB; j <= b; j++) {
                int ai = i - leafA + 1, bj = j - leafB + 1;
                int leafI = A[i].leaf, leafJ = B[j].leaf;
                if (leafI == leafA && leafJ == leafB) {
                    dp2[ai][bj].cost = dp2[ai - 1][bj].cost + 1;
                    dp2[ai][bj].match = DELA;
                    if (dp2[ai][bj].cost > dp2[ai][bj - 1].cost + 1) {
                        dp2[ai][bj].cost = dp2[ai][bj - 1].cost + 1;
                        dp2[ai][bj].match = DELB;
                    }
                    int m = match(A[i], B[j]);
                    if (dp2[ai][bj].cost > dp2[ai - 1][bj - 1].cost + m) {
                        dp2[ai][bj].cost = dp2[ai - 1][bj - 1].cost + m;
                        dp2[ai][bj].match = MATCH;
                    }
                    dp[i][j] = dp2[ai][bj];
                }
                else {
                    dp2[ai][bj].cost = dp2[ai - 1][bj].cost + 1;
                    dp2[ai][bj].match = DELA;
                    if (dp2[ai][bj].cost > dp2[ai][bj - 1].cost + 1) {
                        dp2[ai][bj].cost = dp2[ai][bj - 1].cost + 1;
                        dp2[ai][bj].match = DELB;
                    }
                    if (dp2[ai][bj].cost > dp2[leafI - leafA][leafJ - leafB].cost + dp[i][j].cost) {
                        dp2[ai][bj].cost = dp2[leafI - leafA][leafJ - leafB].cost + dp[i][j].cost;
                        dp2[ai][bj].match = MATCH;
                    }
                }
            }
        }

        for (int ci = leafA; ci <= a; ci++) {
            for (int cj = leafB; cj <= b; cj++) {
                if (A[ci].leaf != leafA || B[cj].leaf != leafB) continue;
                int i = ci, j = cj;
                while (i >= leafA || j >= leafB) {
                    int ai = i - leafA + 1, bj = j - leafB + 1;
                    switch (dp2[ai][bj].match) {
                    case DELA: i--; break;
                    case DELB: j--; break;
                    case MATCH:
                        mapT mapEntry;
                        if (A[i].leaf == leafA && B[j].leaf == leafB && i == ci && j == cj) {
                            mapEntry.a = A[i].id;
                            mapEntry.b = B[j].id;
                            mapEntry.ref = false;
                            i--;
                            j--;
                        }
                        else {
                            mapEntry.a = i;
                            mapEntry.b = j;
                            mapEntry.ref = true;

							i -= A[i].size;
                            j -= B[j].size;
                        }
                        dp[ci][cj].map.push_back(mapEntry);
                    }
                }
            }
        }
    }

	 // 将 dp, dp2, postA, postB 等重置
    void reset()
    {
        VN().swap(postA);
        VN().swap(postB);
        VVR().swap(dp);
        VVR().swap(dp2);
    }

	// 代码的执行
	// a_size 代表树 A 的节点数量，b_size 代表树B的节点数量，用于归一化
    double operator()(std::string& json_a, std::string& json_b, int a_size, int b_size)
    {
        JSONValue* A = readJSON(json_a);
        JSONValue* rootA = A->AsObject().at(L"root");
        JSONValue* B = readJSON(json_b);
        JSONValue* rootB = B->AsObject().at(L"root");

        // 取得两个 root 的大小
        int na = getSize(rootA), nb = getSize(rootB);

        dp = VVR(na, VR(nb));  // 二维 vector，矩阵
        dp2 = VVR(na + 1, VR(nb + 1));

        postorder(rootA, postA, true);  // 后序遍历
        postorder(rootB, postB, true);

        for (int i = 0; i < na; i++)
        {
            if (!postA[i].key)
                continue;
            for (int j = 0; j < nb; j++)
            {
                if (!postB[j].key)
                    continue;
                matchTree(postA, postB, i, j);
            }
        }

        int ans = dp[na - 1][nb - 1].cost;

        // map<int, int> matching;
        // populateMatching(matching, dp[na - 1][nb - 1]);
        // cout << ans << " " << matching.size() << endl;
		double res = (2.0 * ans) / (a_size + b_size + ans);
        return res;
    }
};

//***********************DEBUG***************************/
static int suffixLinkUsage = 0;  // 统计后缀链的平均使用次数
bool suffixLinkFlag = false;  // 是否进行统计，默认不统计

std::ofstream fout_dist;  // 用于统计每个计划与最有计划之间的各种距离
bool foutDistFlag = false;  // 是否统计距离的相关信息

//************************ARENA**************************/


#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP 5000	 // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP \
	20	// estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

using namespace gpopt;
void DealWithPlan();
void readConfig();


// // 用于生成更加随机的 plan id 而构造的类
// struct RandomId
// {
// 	std::default_random_engine e;
// 	std::
// 	bool init_flag;
// 
// 	void init()
// }


///**************************************************/
/// 一些会用到的全局变量
///**************************************************/

double max_cost = 1.0;  // 所有 plan 中最大的 cost 值
double s_weight = 0.33;  // 结构的差距所占的比重
double c_weight = 0.33;  // 内容的差异所占的比重
double cost_weight = 0.33;  // cost 的差异所占的比重
double lambda = 0.5;  // diff 所占的比重
std::size_t ARENA_k = 10;  // 默认选出10个 alternative plans

bool treeEdit_flag = false;  // 指示是否使用 treeEdit 计算结构相似度，默认不使用
bool bow_flag = false;  // 指示是否使用 bag of word 模型计算内容的相似度，默认不使用
char edgePrefix[2]={'[', ']'};  // 从节点指向某条边的前缀，只有 '[' 和 ']' 两种可能


//**************************************************/
// 全局变量，用于记录每个 plan 当前的最近距离
//      key: plan 在 vector 中的 index
//      value: min_dist
// 
// new_add：新加入的 plan 的 id
//**************************************************/
// std::unordered_map<int, double> dist_record;
int new_add = 0;
std::unordered_set<ULLONG> find_index;
int counter = 0;



///**************************************************/
///
/// 编辑距离相关的代码
///
///**************************************************/
inline int min(int x, int y) { return x <= y ? x : y; }

double editDist(std::vector<std::string*> word1, std::vector<std::string*> word2)
{
	std::size_t n = word1.size();
	std::size_t m = word2.size();

	// 有一个字符串为空串
	if (n * m == 0)
	{
		return 1.0;
	}

	// DP 数组
	int D[100][100];

	// 边界状态初始化
	for (std::size_t i = 0; i < n + 1; i++) {
		D[i][0] = i;
	}
	for (std::size_t j = 0; j < m + 1; j++) {
		D[0][j] = j;
	}

	// 计算所有 DP 值
	for (std::size_t i = 1; i < n + 1; i++) {
		for (std::size_t j = 1; j < m + 1; j++) {
			int left = D[i - 1][j] + 1;
			int down = D[i][j - 1] + 1;
			int left_down = D[i - 1][j - 1];
			if ((*word1[i - 1]) != (*word2[j - 1])) left_down += 1;
			D[i][j] = min(left, min(down, left_down));

		}
	}
	return 2.0 * D[n][m] / (n + m + D[n][m]);
}


///**************************************************/
///
/// 与 suffix tree 相关的代码
///
///**************************************************/
struct  Key
{
	int nodeId;
	char c;  // 因为字符集一定是 ascii 字符集，所以使用 char 类型
};

// 节点类型的定义
struct Node
{
	int suffixNode;  // suffix link 节点
	int lvs;  // 叶子节点的数量

	Node() : suffixNode(0), lvs(0) {};

	~Node() { }
};


/*
* 构建一颗 suffix tree 所需的全局信息
*
* 树的结构通过 nodeArray , edgeHash 以及 node2edge 来维护
* 	nodeArray 用来存储所有的节点
*	node2edge 记录父节点指向的所有子节点信息
*/
struct suffixTreeContext
{
	// 用于记录 Matching Statistic 的数据结构
	struct matchStatistc
	{
		std::size_t length;  // 匹配的长度
		int ceil;  // 匹配的节点

		matchStatistc() : length(0), ceil(0) {};
	};

	Node* nodeArray;  // 存储节点的列表
	int inputLength;
	int noOfNodes;
	double self_kernel;
	std::string s;
	std::unordered_map<long, Edge> edgeHash;  // 记录边的映射表：key由两部分构成（1、开始节点  2、节点后的一个字符）
	std::unordered_map<int, std::unordered_set<int>> node2edge;  // 记录每个顶点对应的子节点编号列表

	suffixTreeContext() : nodeArray(NULL), inputLength(0), noOfNodes(0), self_kernel(0.0) {};

	void init(std::string s_in)
	{
		s = s_in + '$';  // 增加尾部字符 '$'，防止节点计数出错
		inputLength = s.size() - 1;
		noOfNodes = 1;
		nodeArray = new Node[2 * inputLength];
	}

	~suffixTreeContext()
	{
		delete[] nodeArray;
	}

	// node is the starting node and c is the ASCII input char.
	inline static long returnHashKey(int nodeId, char c)
	{
		long temp = nodeId;
		temp = temp << 8;
		return temp + c;
	}

	Edge findEdge(int node, char c)
	{
		long key = returnHashKey(node, c);
		std::unordered_map<long, Edge>::const_iterator search = edgeHash.find(key);
		if (search != edgeHash.end()) {
			return edgeHash.at(key);
		}

		return Edge();  // 没有找到边，则返回一条新边
	};

	void insert(Edge e)
	{
			long key = returnHashKey(e.startNode, s[e.startLabelIndex]);
			edgeHash.insert(std::make_pair(key, e));
			node2edge[e.startNode].insert(e.endNode);
	}

	void remove(Edge e)
	{
			long key = returnHashKey(e.startNode, s[e.startLabelIndex]);
			node2edge[e.startNode].erase(e.endNode);
			edgeHash.erase(key);
	}

	// 计算每个节点下的叶子节点的数量
	int calculate_node_num(int nodeId = 0)
	{
		// 如果这个节点下面还有子节点
		if (node2edge.find(nodeId) != node2edge.end() && node2edge[nodeId].size())
		{
			// 递归地计算下面所有子节点的个数
			for (auto id : node2edge[nodeId])
			{
				nodeArray[nodeId].lvs += calculate_node_num(id);
			}
		}
		// 该节点就是叶子节点，值为1
		else
		{
			nodeArray[nodeId].lvs = 1;
		}
		return nodeArray[nodeId].lvs;
	}

	// 判断字符串是否为 树结构
	bool judge_tree(std::string& str)
	{
		if (str.size() % 2 == 0  && str.size() > 0)
		{
			int lbracket = 0;  // 左括号的数量
			int rbracket = 0;
			std::vector<char> stack = { '$' };
			for(auto c: str)
			{
				if (lbracket == rbracket && lbracket != 0)  // 已经找到树结构，并且还有字符
					return false;
				if (lbracket < rbracket)
					return false;
				switch (c)
				{
				case '[':
					lbracket++;
					break;
				case ']':
					rbracket++;
					break;
				default:
					return false;
				}
			}

			if (lbracket == rbracket && lbracket > 0) // 存在树结构
				return true;
			else
				return false;
		}
		else
			return false;
	}

	// 寻找某一字符串在该后缀树中匹配的最大长度以及 ceil 节点
	// start 和 end 用于指示子串的内容，两端都包含
	void getMaxLength(std::string &target, std::size_t start, std::size_t end, matchStatistc & ms)
	{
		Edge e = findEdge(0, target[start]);
		if (e.startNode != -1)  // 找到了这条边
		{
			std::size_t i = 0;
			int iter = 0;
			while (start + i <= end)
			{
				iter = 0;
				// 在这条边上尽量多的匹配
				while (e.startLabelIndex + iter <= e.endLabelIndex)
				{
					if (s[e.startLabelIndex + iter] == target[start + i])  // 当前字符匹配，移动到下一个字符
					{
						++iter; ++i;
					}
					else // 字符已经不匹配，更新 len 并且返回
					{
						ms.length += iter;
						ms.ceil = e.endNode;
						return;
					}
				}

				// 上面的一条边已经匹配完，寻找该边的 endNode 的子边
				ms.length += e.endLabelIndex - e.startLabelIndex + 1;
				ms.ceil = e.endNode;
				e = findEdge(e.endNode, target[start + i]);
				if (-1 == e.startNode)  // 没有找到
				{
					return;
				}
			}
		}
		else
		{
			// 没有找到这条边
			ms.ceil = 0;
			ms.length = 0;
		}
	}

	// 计算 other 关于当前后缀树的 matching statistic 信息
	void generateMatchStatistic(suffixTreeContext & other, std::vector<matchStatistc> & ms)
	{
		std::string temp_s = other.s.substr(0, other.s.size()-1);
		for (std::size_t i = 0; i < other.s.size(); ++i)
		{
			getMaxLength(temp_s, i, temp_s.size() - 1, ms[i]);
			// std::cout << i << ": " << ms[i].length << "\t" << ms[i].ceil << std::endl;
		}
	}

	// 计算 other 关于当前后缀树的 matching statistic 信息（线性时间复杂度的算法）
	void generateMatchStatistic_fast(suffixTreeContext& other, std::vector<matchStatistc>& ms)
	{
		int startNode = 0;  // 初始是从根节点开始
		std::size_t j = 0, k = 0;
		std::string& T = other.s;
		T.pop_back();
		Edge e;
		for (std::size_t i = 0; i < T.size(); ++i)
		{
			e = findEdge(startNode, T[j]);
			while (j < k && e.startNode != -1 && j + e.size() <= k)  // 3.1 部分的代码
			{
				startNode = e.endNode;
				j += e.size();
				e = findEdge(startNode, T[j]);
			}

			if (j == k)  // 3.2 
			{
				while (e.startNode != -1 && T[k] == s[e.startLabelIndex + k -j] && k < T.size())
				{
					++k;
					if (j + e.size() == k)
					{
						startNode = e.endNode;
						j = k;
						e = findEdge(startNode, T[j]);
					}
				}
			}

			// 设置相应的值
			ms[i].length = k - i;
			if (j == k)
			{
				ms[i].ceil = startNode;
			}
			else
			{
				ms[i].ceil = findEdge(startNode, T[j]).endNode;
			}

			// 为下一轮迭代做准备
			if (0 == startNode)
			{
				if (j == k && k < T.size())
				{
					++j; ++k;
				}
				else
				{
					++j;
				}
			}
			else
			{
				startNode = nodeArray[startNode].suffixNode;
				if (suffixLinkFlag)
					suffixLinkUsage++;
			}
		}
		T.append(1, '$');
	}

	// 计算与自己之间的距离 (不能只是计算子串的个数，需要计算那些符合树结构的子串，因此需要传递前缀)
	// Args: rootNode 根节点的编号
	//		 prefix 当前节点的祖先节点已经有的前缀
	//		 lbracket_in 当前遇到的左括号的个数
	//		 rbracket_in 当前遇到的右括号的个数
	void distanceSelf(int rootNode, const std::string& prefix, int lbracket_in, int rbracket_in)
	{
		// 遍历每一种可能
		for(int j=0;j<2;j++)
		{
			char c = edgePrefix[j];
			Edge e = findEdge(rootNode, c);
			if(e.valid())  // 有效边
			{
				int num = nodeArray[e.endNode].lvs;  // 在这条边上，子串出现的次数
				num = num * num;
				std::string pre = prefix;  // 避免修改 prefix
				int lbracket = lbracket_in;
				int rbracket = rbracket_in;
				bool treeFlag = false;  // 是否已经找到子树，找到子树后，这条边下的子边都不需要再继续遍历了

				for (int i=e.startLabelIndex; i <= e.endLabelIndex; i++)
				{
					pre += s[i];  // 每加一个字符都是一种子串的可能
					switch (s[i])
					{
					case '[':
						lbracket++;
						break;
					
					default:
						rbracket++;
						break;
					}
					if (lbracket == rbracket && !treeFlag)
					{
						treeFlag = true;
						self_kernel += num;
					}
				}
				if (!treeFlag)  // 没有找到子树才需要遍历
					distanceSelf(e.endNode, pre, lbracket, rbracket);  // 递归到子节点
			}
		}
	}

	// 计算两棵后缀树之间的距离
	double distance(suffixTreeContext& other)
	{
		std::vector<matchStatistc> ms;
		ms.resize(other.s.size());
		generateMatchStatistic(other, ms);
		// generateMatchStatistic_fast(other, ms);

		// 寻找所有的子字符串
		std::unordered_map<std::string, int> sub_string;
		std::unordered_map<std::string, int> substr_node_map;  // 每个子串与所在节点的对应关系
		for (std::size_t i = 0; i < other.s.size(); ++i)
		{
			std::size_t end = i + ms[i].length - 1;  // 子串的终点，包含该终点
			for (std::size_t j = i+1; j <= end; ++j)
			{
				std::string temp = other.s.substr(i, j-i+1);
				if (judge_tree(temp))  // 如果判断为子树结构
				{
					sub_string[temp]++;
					substr_node_map[temp] = ms[i].ceil;
				}
			}
		}

		// 根据所有子串计算最终距离
		double res = 0.0;
		for (auto& p : sub_string)
		{
			res += (double)p.second * nodeArray[substr_node_map[p.first]].lvs;  // p.second 取得该子串在 other 中出现的次数，substr_node_map 则取得子串在 当前字符串中出现的次数
		}
		
		// 标准化
		if (self_kernel == 0.0)  // 第一次计算与自己的距离，即计算 self_kernel 的时候
		{
			return res;
		}
		else
		{
			double temp  = self_kernel * other.self_kernel;
			temp = sqrt(temp);
			res = res / temp;
			return sqrt(1-res);
		}
	}


	// 一些收尾工作，包括：1、计算每个节点的叶子数量   2、计算自己与自己的相似度
	void finishing_work()
	{
		calculate_node_num();
		self_kernel = distance(*this);
	}

	void finishing_work1()
	{
		calculate_node_num();
	}

	void finishing_work2()
	{
		Edge e = findEdge(0, '[');
		if (e.valid())	// 有效边
		{
			int num = nodeArray[e.endNode].lvs;	 // 在这条边上，子串出现的次数
			num = num * num;
			std::string pre;
			int lbracket = 0;
			int rbracket = 0;
			bool treeFlag = false;
			for (int i = e.startLabelIndex; i <= e.endLabelIndex; i++)
			{
				pre += s[i];  // 每加一个字符都是一种子串的可能
				switch(s[i]){
					case '[':
						lbracket++;
						break;
					default:
						rbracket++;
						break;
				}
				if (lbracket == rbracket && !treeFlag)
				{
					treeFlag = true;
					self_kernel += num;
				}
			}
			if(!treeFlag)
				distanceSelf(e.endNode, pre, lbracket, rbracket);  // 递归到子节点
		}
	}

};

Edge::Edge(int start, int first, int last, suffixTreeContext* context) :
	startNode(start),
	startLabelIndex(first),
	endLabelIndex(last)
{
	endNode = context->noOfNodes++;
}


class suffixTree
{
public:
	int rootNode;   // Origin of the suffix tree
	int startIndex; // Starting index of the string represented. 字符串表示的开始索引
	int endIndex;   // End index of the string represented.  字符串表示的结束索引
	suffixTreeContext* context;

	suffixTree() :
		rootNode(0),
		startIndex(-1),
		endIndex(-1),
		context(NULL) {};

	suffixTree(int root, int start, int end, suffixTreeContext* context_in) :
		rootNode(root),
		startIndex(start),
		endIndex(end),
		context(context_in) {};

	// Real means that the suffix string ends at a node and thus the
	// remaining string on that edge would be an empty string.
	// 这意味着后缀树结束于一个节点
	bool endReal() { return startIndex > endIndex; }   // 开始索引大于结束索引

	// Img means that the suffixTree of current string ends on an imaginary
	// node, which means in between an edge. 
	// Img 意味着当前字符串的后缀树结束于一个想象中的节点，这意味着在边之间
	bool endImg() { return endIndex >= startIndex; }

	void init(int root, int start, int end, suffixTreeContext* context_in){  // 调试用，无实际用处（与构造函数相同）
		rootNode = root;
		startIndex = start;
		endIndex = end;
		context = context_in;
	}

	void migrateToClosestParent()
	{
		// If the current suffix tree is ending on a node, this condition is already met.
		// 如果当前的 suffic 树在一个节点终止，条件已经满足
		if (endReal())
		{
			//     cout << "Nothing needs to be done for migrating" << endl;
		}
		else {
			// 找到从根节点开始的这条边
			Edge e = context->findEdge(rootNode, context->s[startIndex]);
			// Above will always return a valid edge as we call this method after adding above.
			// 上面的语句总会返回一条有效的边，因为我们
			if (e.startNode == -1) {  // 虚拟节点
				std::cout << rootNode << " " << startIndex << " " << context->s[startIndex] << std::endl;
			}
			assert(e.startNode != -1);
			int labelLength = e.endLabelIndex - e.startLabelIndex;  // 标签的长度：end标签的索引 - start标签的索引，这个标签会不会是字符串中的字符位置

			// Go down
			while (labelLength <= (endIndex - startIndex))
			{ // endIndex与startIndex都是 suffixTree 中的变量，代表字符串的索引
				startIndex += labelLength + 1;  // ?
				rootNode = e.endNode;  // 边的结束节点
				if (startIndex <= endIndex)
				{
					e = context->findEdge(e.endNode, context->s[startIndex]);  // 找到从结束节点开始的边
					if (e.startNode == -1)
					{
						std::cout << rootNode << " " << startIndex << " " << context->s[startIndex] << std::endl;
					}
					assert(e.startNode != -1);
					labelLength = e.endLabelIndex - e.startLabelIndex;
				}
			}
		}
	}
};


int breakEdge(suffixTree& s, Edge& e)
{
	// 将旧边移除
	s.context->remove(e);

	// 构建新的边: 开始节点(rootNode), 索引的第一个位置(startLabelIndex), 索引的最后一个位置()
	// 这是插入的最顶层的边
	Edge* newEdge = new Edge(s.rootNode, e.startLabelIndex,
		e.startLabelIndex + s.endIndex - s.startIndex, s.context);
	s.context->insert(*newEdge);

	// Add the suffix link for the new node.
	// 对于新的 node，增加 suffix link
	s.context->nodeArray[newEdge->endNode].suffixNode = s.rootNode;
	// 对原来的边进行更改（endLabelIndex不变，改变 startLabelIndex 为新插入的节点的结束节点）
	e.startLabelIndex += s.endIndex - s.startIndex + 1;
	e.startNode = newEdge->endNode;
	s.context->insert(e);
	return newEdge->endNode;
}

// 树剪枝的主要部分
// Args:
//		nodeId: 当前的节点 id
//		lbracket_in: 左括号的数量
//		rbracket_in: 右括号的数量
void purnTreeMain(suffixTreeContext& context, int nodeId, int lbracket_in, int rbracket_in)
{
	for (auto c : edgePrefix)
	{
		Edge e = context.findEdge(nodeId, c);
		bool purgeFlag = false;
		if (e.valid())
		{
			int lbracket = lbracket_in;
			int rbracket = rbracket_in;
			// 遍历其中的每个字符，直到符合树结构
			for (int i = e.startLabelIndex; i <= e.endLabelIndex; i++)
			{
				switch (context.s[i])
				{
				case '[':
					lbracket++;
					break;
				case ']':
					rbracket++;
					break;
				}

				if (lbracket == rbracket)  // 已经符合树结构，剪枝并收缩边
				{
					// 剪枝
					Edge tempE = context.findEdge(e.endNode, ']');
					if (tempE.valid())
					{
						context.remove(tempE);
					}
					tempE = context.findEdge(e.endNode, '[');
					if (tempE.valid())
					{
						context.remove(tempE);
					}
					
					// 收缩
					e.endLabelIndex = i;
					purgeFlag = true;
					break;
				}	
			}
			if (!purgeFlag)  // 没有进行剪枝
			{
				purnTreeMain(context, e.endNode, lbracket, rbracket);
			}
		}
	}
}


// 对树进行剪枝，去除其中不可能为树的结构
void purnTree(suffixTree& tree)
{
	// 首先去除一定不可能的 ']' 边
	auto& context = *(tree.context);
	Edge e = context.findEdge(tree.rootNode, ']');
	context.remove(e);

	// 迭代，去除每个不可能的边，同时对有些边进行收缩（因为有些边可能会出现已经符合树结构，但是仍然还有的情况）
	e = context.findEdge(tree.rootNode, '[');
	if (e.valid())
	{
		int lbracket = 0;
		int rbracket = 0;
		bool purgeFlag = false;
		// 遍历其中的每个字符，直到符合树结构
		for (int i = e.startLabelIndex; i <= e.endLabelIndex; i++)
		{
			switch (context.s[i])
			{
			case '[':
				lbracket++;
				break;
			case ']':
				rbracket++;
				break;
			}

			if (lbracket == rbracket)  // 已经符合树结构，剪枝并收缩边
			{
				// 剪枝
				Edge tempE = context.findEdge(e.endNode, ']');
				if (tempE.valid())
				{
					context.remove(tempE);
				}
				tempE = context.findEdge(e.endNode, '[');
				if (tempE.valid())
				{
					context.remove(tempE);
				}

				// 收缩
				e.endLabelIndex = i;
				purgeFlag = true;
				break;
			}
		}
		if (!purgeFlag)  // 没有进行剪枝
		{
			purnTreeMain(context, e.endNode, lbracket, rbracket);
		}
	}
}


/*
 * Main function which will carry out all the different phases of the Ukkonen's
 * algorithm. Through suffixTree we'll maintain the current position in the tree
 * and then add the prefix 0 -> lastIndex in the tree created in the previous
 * iteration.
 *
 * 主函数：将会执行所有不同的阶段。在构建 suffixTree 的过程中，我们将会在树中的当前位置
 * 之后增加前缀 0 -> lastIndex 到树中，这个树是在前一次迭代中创建的
 */
void carryPhase(suffixTree& tree, int lastIndex)
{
	// cout << "Phase " << lastIndex << " Adding " << Input.substr(0, lastIndex + 1) << endl;
	int parentNode; // 父节点
	// to keep track of the last encountered(遇到的) node.
	// Used for creating the suffix link.
	int previousParentNode = -1;
	while (true)
	{
		// First we try to match an edge for this, if there is one edge and all
		// other subsequent suffixs would already be there.
		// 首先，我们试图匹配这条边，如果这里存在一条边，所有随后的后缀都会在这里
		Edge e;
		parentNode = tree.rootNode;

		if (tree.endReal())  // 如果树在节点终止(endindex 比较小意味着只有当前字符需要加到后缀树上)
		{
			// 下面两条语句主要是检查是否有由根节点出发的 单一边，最后一个
			e = tree.context->findEdge(tree.rootNode, tree.context->s[lastIndex]);
			if (e.startNode != -1) // 这条边已经存在
				break;             // 跳出循环  ****** 只有这里可以跳出循环 *********
		}
		// If previoustree ends in between an edge, then we need to find that
		// edge and match after that.
		// 如果先前的树在两条边之间终止了，我们需要找到那条边，并且
		else
		{
			e = tree.context->findEdge(tree.rootNode, tree.context->s[tree.startIndex]);
			int diff = tree.endIndex - tree.startIndex;  // 当前索引与前一个插入索引的距离（下面 e.startLabelIndx+diff+1 检验这条边是否仍然满足）
			if (tree.context->s[e.startLabelIndex + diff + 1] == tree.context->s[lastIndex])
				// We have a match
				break;
			//If match was not found this way, then we need to break this edge
			// and add a node and insert the string.
			//      cout << " breaking edge " << endl;
			parentNode = breakEdge(tree, e);  // 在这里会设置 parent Node
		}

		// We have not matchng edge at this point, so we need to create a new
		// one, add it to the tree at parentNode position and then insert it
		// into the hash table.
		//
		// We are creating a new node here, which means we also need to update
		// the suffix link here. Suffix link from the last visited node to the
		// newly created node.
		//  cout << "adding new edge" << endl;
		// 
		//
		// 下面的代码表示在这一点没有匹配的边，所以我们需要创建一条新边。并且将它加入到树中
		// 双亲节点的位置，之后将其加入到 hash 表中。
		// 
		// 我们也会创建一个新的节点，这也意味着我们需要更新 suffix link。suffix link 从 last visited
		// 的节点指向 newly created node。这里在 parentNode 部分创建新的节点（parentNode大部分是根节点，在边需要分裂时则是新创建的节点）
		Edge* newEdge = new Edge(parentNode, lastIndex, tree.context->inputLength, tree.context);
		tree.context->insert(*newEdge);  // 将边插入到 hash 表中
		if (previousParentNode > 0)  // 不是根节点，构建 suffix link
			tree.context->nodeArray[previousParentNode].suffixNode = parentNode;
		previousParentNode = parentNode;  // 更新 previousParentNode

		// Move to next suffix, i.e. next extension.
		// 移动到下一个后缀
		if (tree.rootNode == 0)  // rootNode 是根节点
			tree.startIndex++;
		else
		{
			tree.rootNode = tree.context->nodeArray[tree.rootNode].suffixNode;
			// printf("using suffix link while adding %d %d\n",tree.rootNode, nodeArray[tree.rootNode].suffixNode);
		}
		tree.migrateToClosestParent();  // 移动到最近的 parent
	}

	if (previousParentNode > 0)
		tree.context->nodeArray[previousParentNode].suffixNode = parentNode;
	tree.endIndex++;
	tree.migrateToClosestParent();
}

///**************************************************/
///
/// plan树相关的代码
/// 
///**************************************************/

struct NodeData
{
	std::string name;
	double cost;
	NodeData(double cost_in = 0.0, std::string name_in = "")
	{
		name = name_in;
		cost = cost_in;
	}
};

struct PlanTreeNode
{
	using json = nlohmann::json;

	NodeData data;
	std::vector<PlanTreeNode*> child;

	// 构造函数
	PlanTreeNode(NodeData data_in)
	{
		data = data_in;
	}

	~PlanTreeNode()
	{
		for (std::size_t i = 0; i < child.size(); ++i)
			delete child[i];
	}


	// 在该节点添加一个孩子
	void push_back(PlanTreeNode* c)
	{
		child.push_back(c);
	}

	// 取得孩子节点的数量
	std::size_t size() const
	{
		return child.size();
	}

	// 取得某一个孩子节点
	PlanTreeNode* operator[](std::size_t i)
	{
		return i < size() ? child[i] : NULL;
	}

	friend std::ostream& operator<<(std::ostream& out, const PlanTreeNode &node)
	{
		out << node.data.name << "  " << node.data.cost << '\n';
		return out;
	}

	std::ostream& output(std::ostream& out, std::string prefix = "")
	{
		out << prefix << *this;
		for (std::size_t i = 0; i < size(); ++i)
		{
			child[i]->output(out, prefix + "\t");
		}
		return out;
	}

	// 取得字符串表示
	std::string get_string()  // 这个性能还能优化
	{
		if (!child.size())  // size == 0
			return std::string("[]");
		else
		{
			// 取得所有孩子节点的字符串表示
			std::vector<std::string> child_string;
			child_string.reserve(child.size());
			for (std::size_t i = 0; i < child.size(); ++i)
			{
				child_string.push_back(child[i]->get_string());
			}
			// 排序
			std::sort(child_string.begin(), child_string.end());
			std::string res("[");
			for (auto& s : child_string)
			{
				res += s;
			}
			return res + "]";
		}
	}

	json generate_json()
	{
		json res;
		res["name"] = data.name;
		res["cost"] = data.cost;
		res["child_flag"] = child.size() > 0;
		if (child.size())  // 存在孩子节点
		{
			res["child"] = json::array();
			for (std::size_t i = 0; i < child.size(); ++i)
				res["child"].push_back(child[i]->generate_json());
		}
		return res;
	}

	// 生成用于计算 tree edit distance 的字符串
	// target: 用于存储相应字符串的变量
	// current_id: 当前节点的 id
	void ted_json(std::string & target, int * current_id)
	{
		target += "{\"id\":" + std::to_string(*current_id) + ",\"type\":\"" + data.name + "\", \"children\":[";
		(*current_id)++;
		for (std::size_t i=0;i<child.size();++i)  // 遍历每个孩子节点
		{
			child[i]->ted_json(target, current_id);
			target += ", ";
		}
		if (child.size())
		{
			target.pop_back();
			target.pop_back();
		}
		target += "]}";
	}

	std::ostream& output_json(std::ostream& out)
	{
		json j;
		j = generate_json();
		out << j;
		return out;
	}
};


template <class T>
struct PlanTree
{
	using json = nlohmann::json;


	double offset;  // 与 best_plan 的结构和cost的偏离程度

	PlanTreeNode *root;
	std::string str_expression;
	bool init_flag;
	suffixTreeContext context;
	std::vector<std::string*> str_of_node;  // 节点内容构成的字符串，用于度量内容之间的差异
	// 这两个属性均与 treeEditDist 有关，使用其它方法时不需考虑
	std::string json_str;
	int node_num;
	double cost_log;

	suffixTree testTree;


	// 返回 expression 中的 name
	// 如果 name 以 CPhysical 开头，将其删除
	const char* get_name(T* exp)
	{
		if (NULL != exp)
		{
			const char * name_start = exp->Pop()->SzId();
			const char * base_str = "CPhysical";

			if(strlen(name_start) > 9)  // 确保 字符串一定比目标字符串长
			{
				for(int i=0;i<9;++i)
				{
					if (name_start[i] != base_str[i])
					{
						return name_start;
					}
				}
				return name_start+9;
			}
			else
			{
				return name_start;
			}
		}
		else
			return "";
	}

	double get_cost(T* exp)
	{
		if(NULL != exp && exp->Pop()->FPhysical())
		{
			return exp->Cost().Get();
		}
		else
			return 0.0;
	}

	void insert_child(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

			// 检查这个操作是否为 CPhysical 操作，因为这个操作，所以 PlanTree 不再适用于其它类型
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				// 需要检查名字是否是 Scan 类型，如果是，需要获得 relation 的名字
				int name_len = strlen(expression_name);

				// 字符串长度大于4，并且最后四个字母是 Scan，需要取得表名
				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					gpos::ARENAIOstream table_name;
					child_expression->Pop()->OsPrint(table_name);
					std::string temp = table_name.ss.str();
					std::size_t start = temp.find("Table Name: (");
					if(start)
					{
						std::size_t end = temp.find(")", start);
						start += 13;
						start = temp.find("\"", start) + 1;
						end = temp.find("\"", start);
						PlanTreeNode * table = new PlanTreeNode(NodeData(0.0, temp.substr(start, end-start)));
						cur->push_back(table);
					}
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child((*child_expression)[i], *cur);
				}
			}
		}
	}

	PlanTree()
	{
		offset = 0.0;
		root = NULL;
		init_flag = false;
	}

	~PlanTree()
	{
		if (NULL != root)
			delete root;
	}

	// 取得这棵树的字符串表示
	void repreOfStr()  // representation of string
	{
		if (init_flag)
		{
			str_expression = root->get_string();
		}
		else
		{
			std::cerr << "PlanTree not init\n";
		}
	}

	// 用于生成后缀树
	void generateSuffixTree()
	{
		repreOfStr();
		context.init(str_expression);
		suffixTree tree(0, 0, -1, &context);
		for (int i = 0; i <= context.inputLength; ++i)
			carryPhase(tree, i);
		context.finishing_work();  // 做一些收尾工作
	}

	// 用于生成由节点内容组成的字符串
	// 用于计算编辑距离
	void getNodeStr()
	{
		std::queue<PlanTreeNode*> node_queue;  // 采用先序遍历的方式来生成节点的内容
		PlanTreeNode* current_node = NULL;
		node_queue.push(root);
		while (!node_queue.empty())
		{
			current_node = node_queue.front();

			str_of_node.push_back(&(current_node->data.name));
			// 将所有孩子节点加入到队列节点
			for (std::size_t i = 0; i < current_node->child.size(); ++i)
			{
				node_queue.push(current_node->child[i]);
			}
			node_queue.pop();
		}
	}


	// 生成用于计算 tree edit dist 的 json 格式的字符串
	void TED_json()
	{
		json_str = "{\"root\":";
		node_num = 0;
		root->ted_json(json_str, &node_num);  // 传递 node_num 不仅可以对 节点数量进行计数，还能用于标注 id
		json_str += "}";

		// 用于调试
		// std::ofstream fout("/tmp/json_temp");
		// if (fout.is_open())
		// {
		// 	fout << json_str << std::endl;
		// 	fout << node_num;
		// 	fout.close();
		// }
	}

	// 根据 CExpressoin 的表达式构造一个 Plan 树
	void init(T* plan)
	{
		if (NULL != plan)
		{
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}
		}
		// cost_log = log(root->data.cost);  // 用于计算归一化的 cost 距离
		init_flag = true;
		if (treeEdit_flag)  // 如果需要使用 tree edit dist 作为结构和内容的差异
		{
			TED_json();
		}
		else  // 使用标准方式，即 subtree kernel 和 edit dist 的方式
		{
			generateSuffixTree();
			getNodeStr();
		}
	}

	void init(T* plan, int flag)  ////////////////////////////////////////////////// 用于测试不同初始化的时间
	{
		if (flag == 0 && NULL != plan)
		{
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}
		}
		// cost_log = log(root->data.cost);  // 用于计算归一化的 cost 距离
		init_flag = true;
		if (flag == 1)
		{
			// generateSuffixTree();
			// 分开检验各个部分的时间
			repreOfStr();
		}
		else if (flag == 2)
			getNodeStr();
		else if (flag == 3)
			context.init(str_expression);
		else if (flag == 4)
		{
			testTree.init(0, 0, -1, &context);
			for (int i = 0; i <= context.inputLength; ++i)
				carryPhase(testTree, i);
		}
		else if (flag == 5)
			context.finishing_work1();  // 做一些收尾工作
		else if (flag == 6)
		 	context.finishing_work2();  // 做一些收尾工作
		else if (flag == 7)  // 对后缀树进行剪枝，原方法不需要剪枝
			purnTree(testTree);
	}

	//****************** 距离相关的一些函数 ********************************/
	// 计算两棵树之间的距离
	double distance(PlanTree& other)
	{
		if (treeEdit_flag)  // 使用树编辑距离
		{
			TreeEdit te;
			double s_dist = te(json_str, other.json_str, node_num, other.node_num);  // s_dist 同时包含结构的差异与内容的差异
			double cost = cost_dist(other);
			return (1-lambda)*(offset + other.offset) / 2 +lambda * (s_dist * (s_weight + c_weight) +  cost * cost_weight);
		}
		else  // 不使用 树编辑距离
		{
			double s_dist = context.distance(other.context);  // 结构的距离
			double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
			double cost = cost_dist(other);
			return (1-lambda)*(offset + other.offset) / 2 +lambda * (s_dist * s_weight + c_dist * c_weight + cost * cost_weight);
		}
	}

	double distance_with_best(PlanTree &other)
	{
		if (treeEdit_flag)
		{
			TreeEdit te;
			double s_dist = te(json_str, other.json_str, node_num, other.node_num);
			double cost = cost_dist(other);
			offset = abs(s_dist - cost);  // offset 用来衡量偏离程度
			return (1-lambda)*offset / 2 +lambda * (s_dist * (s_weight + c_weight) + cost * cost_weight);
		}
		else
		{
			double s_dist = context.distance(other.context);  // 结构的距离
			double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
			double cost = cost_dist(other);
			offset = abs(0.5*(s_dist+c_dist) - cost);  // offset 用来衡量偏离程度
			if (foutDistFlag)
				fout_dist << s_dist << '\t' << c_dist << '\t' << cost << '\t' << offset << '\n';
			return (1-lambda)*offset / 2 +lambda * (s_dist * s_weight + c_dist * c_weight + cost * cost_weight);
		}
	}

	// 返回两者之间结构的差距
	double structure_dist(PlanTree& other)
	{
		if (treeEdit_flag)  // 树的编辑距离
		{
			TreeEdit te;
			return te(json_str, other.json_str, node_num, other.node_num);
		}
		else
		{
			return context.distance(other.context);
		}
	}

	// 返回两者之间内容的差距
	double content_dist(PlanTree& other)
	{
		if (treeEdit_flag)
		{
			return 0.0;
		}
		else
		{
			return editDist(str_of_node, other.str_of_node);
		}
	}

	// 返回两者之间 cost 的差异
	double cost_dist(PlanTree& other)
	{
		double res = abs(root->data.cost - other.root->data.cost) / max_cost;  // 没有取 log 的 cost 差异
		// double res = abs(cost_log - other.cost_log) / max_cost;  // 对 cost 取log，再计算差异
		return res;
	}
	//*****************************************************************/

	// 输出表达式的内容
	void write(std::ostream &out)
	{
		if (NULL != root)
			root->output(out);
	}

	// 以 json 的格式输出表达式的内容
	void write_json(std::ostream &out)
	{
		if (NULL != root)
			root->output_json(out);
	}
};

///**************************************************/
/// 一些会用到的全局变量
///**************************************************/
std::queue<CExpression *> plan_buffer;  // 用于存储所有 CExpressioin 的缓存，主线程用于向该变量中不断地
std::vector<PlanTree<CExpression>> plan_trees;  // 添加元素，辅助线程将其转换为 PlanTree
std::mutex m;  // 锁，用来在不同线程之间同步 plan_buffer



///**************************************************/
/// 用于将结果发送给服务器
/// 参数：
///		id 代表这个 plan tree 在 plan_trees 中的索引
///**************************************************/
void sendResult(int id)
{
	nlohmann::json j;
	j["id"] = counter+1;
	j["cost"] = plan_trees[id].root->data.cost; 

	if (counter == 0)  // best plan
	{
		j["s_dist"] = 0;
		j["c_dist"] = 0;
		j["cost_dist"] = 0; 
		j["relevance"] = 0;
	}
	else
	{
		j["s_dist"] = plan_trees[id].structure_dist(plan_trees[0]);
		j["c_dist"] = plan_trees[id].content_dist(plan_trees[0]);
		j["cost_dist"] = plan_trees[id].cost_dist(plan_trees[0]);
		j["relevance"] = plan_trees[id].offset; 
	}

	j["content"] = plan_trees[id].root->generate_json();
	std::string result= j.dump();
	Sender s;
    if(s.isStart())
    {
        s.send_result(ARENA_file_name, result, counter, s_weight, c_weight, lambda, treeEdit_flag, ARENA_k);
        s.Close();
    }

	// 负责将 counter + 1
	++counter;
}


//**************************************************/
//
// 用于找到 k 个最不一样的元素（k个不包含 best_plan)
// 
// plans 中的第一个元素代表 best_plan。 res 只存储相应的索引，是返回值
// 
//**************************************************/
template<class T>
void FindK(std::vector<T>& plans, std::vector<int>& res, std::size_t k, std::priority_queue<MinDist> & dist_record)
{
    res.push_back(0);  // res 的第一个元素是 QEP
	sendResult(0);  // 将数据发送

    // 总数比需要找到的数量少
    if (plans.size() <= k)
    {
		for (std::size_t i = 1; i < plans.size(); ++i)
        {
			if (plans[i].root->data.cost != plans[0].root->data.cost)
			{
				res.push_back(i);
				sendResult(i);  // 将数据发送
			}
        }
    }
    // 寻找 k 个 plan
    else
    {
		std::vector<MinDist> tempRemoved;  // 记录需要暂时移除的计划
		tempRemoved.reserve(plans.size() / 3);  // 默认先申请 1/3 的内存

        for (std::size_t i = 0; i < k; ++i)
		// 迭代 k 次
		// 每次都从 dist_record 的最大值处进行遍历，直到可以进行剪枝为止
        {
			MinDist maxValue;
			while(!dist_record.empty() && dist_record.top() > maxValue) {  // 当其非空并且顶部的元素的最小距离仍然大于最大值时
				if (dist_record.top().distNum == i) { // 记录的值就是正确的值 
					tempRemoved.push_back(maxValue);  // 记录前一个值
					maxValue = dist_record.top();  // 记录新的最大值
				} else {  // 最小距离的值不是最新的，需要更新最小距离
					MinDist topElement = dist_record.top();
					for(std::size_t j=topElement.distNum+1; j<=i ; j++) {
						double dist = plans[topElement.index].distance(plans[res[j]]);  // 此处计算的是两个plan间的距离；dist_record.top().index 代表当前最大元素在plans中的索引；res[j]代表第j个结果的索引
						topElement.distNum = j;  // 更新距离的记录
						if(dist < topElement.dist)
							topElement.dist = dist;
					}

					if (topElement > maxValue)  // 更新最大值
					{
						tempRemoved.push_back(maxValue);
						maxValue = topElement;
					} else {
						tempRemoved.push_back(topElement);
					}
				}

				dist_record.pop();
			}

			// 记录结果，更新堆
			res.push_back(maxValue.index);
			sendResult(maxValue.index);
			for(auto & md : tempRemoved)
				dist_record.push(md);
        }
    }
}


///**************************************************/
/// 用于将 CExpression * 转换 plantree 的线程
///**************************************************/
void CExpression2PlanTree()
{
	std::ofstream fout("/tmp/threa_debug");
	if (fout.is_open())
	{
		fout << "start: " << plan_buffer.size() << std::endl;

	while(true)
	{
		if (!plan_buffer.empty() and plan_buffer.front() == NULL)  // 不为空且，第一个元素是 NULL
		{
			fout << "NULL run\n";
			m.lock();
			plan_buffer.pop();
			m.unlock();
			break;
		}

		else if (plan_buffer.empty())  // 如果当前为空，循环等待
		{
			continue;
		}
		else
		{
			fout << "insert new element\n";
			plan_trees.push_back(PlanTree<CExpression>());
			plan_trees.back().init(plan_buffer.front());
			plan_buffer.front()->Release();
			m.lock();
			plan_buffer.pop();
			m.unlock();
		}
	}
	fout << "finish\n";
	fout.close();
	}
}

void reset_plan()
{
	std::ofstream fout("/tmp/best_plan.txt");
	fout.close();
}

void output_plan(CExpression * plan, int index = 0)
{
	PlanTree<CExpression> tree;
	tree.init(plan);
	std::ofstream fout("/tmp/best_plan.txt", std::ios_base::out|std::ios_base::app);
	if (fout.is_open())
	{
		fout << "/************************* " << index << " *************************/\n";
		tree.write(fout);
		fout << "/**********************************************************/\n\n";
		fout.close();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEngine::CEngine(CMemoryPool *mp)
	: m_mp(mp),
	  m_pqc(NULL),
	  m_search_stage_array(NULL),
	  m_ulCurrSearchStage(0),
	  m_pmemo(NULL),
	  m_pexprEnforcerPattern(NULL),
	  m_xforms(NULL),
	  m_pdrgpulpXformCalls(NULL),
	  m_pdrgpulpXformTimes(NULL),
	  m_pdrgpulpXformBindings(NULL),
	  m_pdrgpulpXformResults(NULL)
{
	m_pmemo = GPOS_NEW(mp) CMemo(mp);
	m_pexprEnforcerPattern =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));
	m_xforms = GPOS_NEW(mp) CXformSet(mp);
	m_pdrgpulpXformCalls = GPOS_NEW(mp) UlongPtrArray(mp);
	m_pdrgpulpXformTimes = GPOS_NEW(mp) UlongPtrArray(mp);
	m_pdrgpulpXformBindings = GPOS_NEW(mp) UlongPtrArray(mp);
	m_pdrgpulpXformResults = GPOS_NEW(mp) UlongPtrArray(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::~CEngine
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEngine::~CEngine()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	GPOS_DELETE(m_pmemo);
	CRefCount::SafeRelease(m_xforms);
	m_pdrgpulpXformCalls->Release();
	m_pdrgpulpXformTimes->Release();
	m_pdrgpulpXformBindings->Release();
	m_pdrgpulpXformResults->Release();
	m_pexprEnforcerPattern->Release();
	CRefCount::SafeRelease(m_search_stage_array);
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InitLogicalExpression
//
//	@doc:
//		Initialize engine with a given expression
//
//---------------------------------------------------------------------------
void
CEngine::InitLogicalExpression(CExpression *pexpr)
{
	GPOS_ASSERT(NULL == m_pmemo->PgroupRoot() && "Root is already set");
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CGroup *pgroupRoot =
		PgroupInsert(NULL /*pgroupTarget*/, pexpr, CXform::ExfInvalid,
					 NULL /*pgexprOrigin*/, false /*fIntermediate*/);
	m_pmemo->SetRoot(pgroupRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Init
//
//	@doc:
//		Initialize engine using a given query context
//
//---------------------------------------------------------------------------
void
CEngine::Init(CQueryContext *pqc, CSearchStageArray *search_stage_array)
{
	GPOS_ASSERT(NULL == m_pqc);
	GPOS_ASSERT(NULL != pqc);
	GPOS_ASSERT_IMP(0 == pqc->Pexpr()->DeriveOutputColumns()->Size(),
					0 == pqc->Prpp()->PcrsRequired()->Size() &&
						"requiring columns from a zero column expression");

	m_search_stage_array = search_stage_array;
	if (NULL == search_stage_array)
	{
		m_search_stage_array = CSearchStage::PdrgpssDefault(m_mp);
	}
	GPOS_ASSERT(0 < m_search_stage_array->Size());

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// initialize per-stage xform calls array
		const ULONG ulStages = m_search_stage_array->Size();
		for (ULONG ul = 0; ul < ulStages; ul++)
		{
			ULONG_PTR *pulpXformCalls =
				GPOS_NEW_ARRAY(m_mp, ULONG_PTR, CXform::ExfSentinel);
			ULONG_PTR *pulpXformTimes =
				GPOS_NEW_ARRAY(m_mp, ULONG_PTR, CXform::ExfSentinel);
			ULONG_PTR *pulpXformBindings =
				GPOS_NEW_ARRAY(m_mp, ULONG_PTR, CXform::ExfSentinel);
			ULONG_PTR *pulpXformResults =
				GPOS_NEW_ARRAY(m_mp, ULONG_PTR, CXform::ExfSentinel);
			for (ULONG ulXform = 0; ulXform < CXform::ExfSentinel; ulXform++)
			{
				pulpXformCalls[ulXform] = 0;
				pulpXformTimes[ulXform] = 0;
				pulpXformBindings[ulXform] = 0;
				pulpXformResults[ulXform] = 0;
			}
			m_pdrgpulpXformCalls->Append(pulpXformCalls);
			m_pdrgpulpXformTimes->Append(pulpXformTimes);
			m_pdrgpulpXformBindings->Append(pulpXformBindings);
			m_pdrgpulpXformResults->Append(pulpXformResults);
		}
	}

	m_pqc = pqc;
	InitLogicalExpression(m_pqc->Pexpr());

	m_pqc->PdrgpcrSystemCols()->AddRef();
	COptCtxt::PoctxtFromTLS()->SetReqdSystemCols(m_pqc->PdrgpcrSystemCols());
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::AddEnforcers
//
//	@doc:
//		Add enforcers to a memo group
//
//---------------------------------------------------------------------------
void
CEngine::AddEnforcers(
	CGroupExpression
		*pgexpr,  // belongs to group where we need to add enforcers
	CExpressionArray *pdrgpexprEnforcers)
{
	GPOS_ASSERT(NULL != pdrgpexprEnforcers);
	GPOS_ASSERT(NULL != pgexpr);

	for (ULONG ul = 0; ul < pdrgpexprEnforcers->Size(); ul++)
	{
		// assemble an expression rooted by the enforcer operator
		CExpression *pexprEnforcer = (*pdrgpexprEnforcers)[ul];
#ifdef GPOS_DEBUG
		CGroup *pgroup =
#endif	// GPOS_DEBUG
			PgroupInsert(pgexpr->Pgroup(), pexprEnforcer, CXform::ExfInvalid,
						 NULL /*pgexprOrigin*/, false /*fIntermediate*/);
		GPOS_ASSERT(pgroup == pgexpr->Pgroup());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertExpressionChildren
//
//	@doc:
//		Insert children of the given expression to memo, and copy the groups
//		they end up at to the given group array
//
//---------------------------------------------------------------------------
void
CEngine::InsertExpressionChildren(CExpression *pexpr,
								  CGroupArray *pdrgpgroupChildren,
								  CXform::EXformId exfidOrigin,
								  CGroupExpression *pgexprOrigin)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpgroupChildren);

	ULONG arity = pexpr->Arity();

	for (ULONG i = 0; i < arity; i++)
	{
		CGroup *pgroupChild = NULL;
		COperator *popChild = (*pexpr)[i]->Pop();
		if (popChild->FPattern() && CPattern::PopConvert(popChild)->FLeaf())
		{
			GPOS_ASSERT(NULL != (*pexpr)[i]->Pgexpr()->Pgroup());

			// group is already assigned during binding extraction;
			pgroupChild = (*pexpr)[i]->Pgexpr()->Pgroup();
		}
		else
		{
			// insert child expression recursively
			pgroupChild =
				PgroupInsert(NULL /*pgroupTarget*/, (*pexpr)[i], exfidOrigin,
							 pgexprOrigin, true /*fIntermediate*/);
		}
		pdrgpgroupChildren->Append(pgroupChild);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgroupInsert
//
//	@doc:
//		Insert an expression tree into the memo, with explicit target group;
//		the function returns a pointer to the group that contains the given
//		group expression
//
//---------------------------------------------------------------------------
CGroup *
CEngine::PgroupInsert(CGroup *pgroupTarget, CExpression *pexpr,
					  CXform::EXformId exfidOrigin,
					  CGroupExpression *pgexprOrigin, BOOL fIntermediate)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;
	GPOS_ASSERT_IMP(CXform::ExfInvalid != exfidOrigin, NULL != pgexprOrigin);

	CGroup *pgroupOrigin = NULL;

	// check if expression was produced by extracting
	// a binding from the memo
	if (NULL != pexpr->Pgexpr())
	{
		pgroupOrigin = pexpr->Pgexpr()->Pgroup();
		GPOS_ASSERT(NULL != pgroupOrigin && NULL == pgroupTarget &&
					"A valid group is expected");

		// if parent has group pointer, all children must have group pointers;
		// terminate recursive insertion here
		return pgroupOrigin;
	}

	// if we have a valid origin group, target group must be NULL
	GPOS_ASSERT_IMP(NULL != pgroupOrigin, NULL == pgroupTarget);

	// insert expression's children to memo by recursive call
	CGroupArray *pdrgpgroupChildren =
		GPOS_NEW(m_mp) CGroupArray(m_mp, pexpr->Arity());
	InsertExpressionChildren(pexpr, pdrgpgroupChildren, exfidOrigin,
							 pgexprOrigin);

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	CGroupExpression *pgexpr = GPOS_NEW(m_mp)
		CGroupExpression(m_mp, pop, pdrgpgroupChildren, exfidOrigin,
						 pgexprOrigin, fIntermediate);

	// find the group that contains created group expression
	CGroup *pgroupContainer =
		m_pmemo->PgroupInsert(pgroupTarget, pexpr, pgexpr);

	if (NULL == pgexpr->Pgroup())
	{
		// insertion failed, release created group expression
		pgexpr->Release();
	}

	return pgroupContainer;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertXformResult
//
//	@doc:
//		Insert a set of transformation results to memo
//
//---------------------------------------------------------------------------
void
CEngine::InsertXformResult(
	CGroup *pgroupOrigin, CXformResult *pxfres, CXform::EXformId exfidOrigin,
	CGroupExpression *pgexprOrigin,
	ULONG ulXformTime,	// time consumed by transformation in msec
	ULONG ulNumberOfBindings)
{
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(NULL != pgroupOrigin);
	GPOS_ASSERT(CXform::ExfInvalid != exfidOrigin);
	GPOS_ASSERT(NULL != pgexprOrigin);

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics) &&
		0 < pxfres->Pdrgpexpr()->Size())
	{
		(void) m_xforms->ExchangeSet(exfidOrigin);
		(*m_pdrgpulpXformCalls)[m_ulCurrSearchStage][exfidOrigin] += 1;
		(*m_pdrgpulpXformTimes)[m_ulCurrSearchStage][exfidOrigin] +=
			ulXformTime;
		(*m_pdrgpulpXformBindings)[m_ulCurrSearchStage][exfidOrigin] +=
			ulNumberOfBindings;
		(*m_pdrgpulpXformResults)[m_ulCurrSearchStage][exfidOrigin] +=
			pxfres->Pdrgpexpr()->Size();
	}

	CExpression *pexpr = pxfres->PexprNext();
	while (NULL != pexpr)
	{
		CGroup *pgroupContainer =
			PgroupInsert(pgroupOrigin, pexpr, exfidOrigin, pgexprOrigin,
						 false /*fIntermediate*/);
		if (pgroupContainer != pgroupOrigin &&
			FPossibleDuplicateGroups(pgroupContainer, pgroupOrigin))
		{
			m_pmemo->MarkDuplicates(pgroupOrigin, pgroupContainer);
		}

		pexpr = pxfres->PexprNext();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FPossibleDuplicateGroups
//
//	@doc:
//		Check whether the given memo groups can be marked as duplicates. This is
//		true only if they have the same logical properties
//
//---------------------------------------------------------------------------
BOOL
CEngine::FPossibleDuplicateGroups(CGroup *pgroupFst, CGroup *pgroupSnd)
{
	GPOS_ASSERT(NULL != pgroupFst);
	GPOS_ASSERT(NULL != pgroupSnd);

	CDrvdPropRelational *pdprelFst =
		CDrvdPropRelational::GetRelationalProperties(pgroupFst->Pdp());
	CDrvdPropRelational *pdprelSnd =
		CDrvdPropRelational::GetRelationalProperties(pgroupSnd->Pdp());

	// right now we only check the output columns, but we may possibly need to
	// check other properties as well
	return pdprelFst->GetOutputColumns()->Equals(pdprelSnd->GetOutputColumns());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the root group
//
//---------------------------------------------------------------------------
void
CEngine::DeriveStats(CMemoryPool *pmpLocal)
{
	CWStringDynamic str(m_mp);
	COstreamString oss(&str);
	oss << "\n[OPT]: Statistics Derivation Time (stage " << m_ulCurrSearchStage
		<< ") ";
	CHAR *sz = CUtils::CreateMultiByteCharStringFromWCString(
		m_mp, const_cast<WCHAR *>(str.GetBuffer()));

	{
		CAutoTimer at(sz, GPOS_FTRACE(EopttracePrintOptimizationStatistics));
		// derive stats on root group
		CEngine::DeriveStats(pmpLocal, m_mp, PgroupRoot(), NULL /*prprel*/);
	}

	GPOS_DELETE_ARRAY(sz);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the group
//
//---------------------------------------------------------------------------
void
CEngine::DeriveStats(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
					 CGroup *pgroup, CReqdPropRelational *prprel)
{
	CGroupExpression *pgexprFirst = CEngine::PgexprFirst(pgroup);
	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(pgexprFirst);
	exprhdl.DeriveStats(pmpLocal, pmpGlobal, prprel, NULL /*stats_ctxt*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprFirst
//
//	@doc:
//		Return the first group expression in a given group
//
//---------------------------------------------------------------------------
CGroupExpression *
CEngine::PgexprFirst(CGroup *pgroup)
{
	CGroupExpression *pgexprFirst = NULL;
	{
		// group proxy scope
		CGroupProxy gp(pgroup);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(NULL != pgexprFirst);

	return pgexprFirst;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::EolDamp
//
//	@doc:
//		Damp optimization level
//
//---------------------------------------------------------------------------
EOptimizationLevel
CEngine::EolDamp(EOptimizationLevel eol)
{
	if (EolHigh == eol)
	{
		return EolLow;
	}

	return EolSentinel;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimizeChild
//
//	@doc:
//		Check if parent group expression needs to optimize child group expression.
//		This method is called right before a group optimization job is about to
//		schedule a group expression optimization job.
//
//		Relation properties as well the optimizing parent group expression is
//		available to make the decision. So, operators can reject being optimized
//		under specific parent operators. For example, a GatherMerge under a Sort
//		can be prevented here since it destroys the order from a GatherMerge.
//---------------------------------------------------------------------------
BOOL
CEngine::FOptimizeChild(
	CGroupExpression *pgexprParent, CGroupExpression *pgexprChild,
	COptimizationContext *pocChild,
	EOptimizationLevel eolCurrent  // current optimization level in child group
)
{
	GPOS_ASSERT(NULL != PgroupRoot());
	GPOS_ASSERT(PgroupRoot()->FImplemented());
	GPOS_ASSERT(NULL != pgexprChild);
	GPOS_ASSERT_IMP(NULL == pgexprParent,
					pgexprChild->Pgroup() == PgroupRoot());

	if (pgexprParent == pgexprChild)
	{
		// a group expression cannot optimize itself
		return false;
	}

	if (pgexprChild->Eol() != eolCurrent)
	{
		// child group expression does not match current optimization level
		return false;
	}

	COperator *popChild = pgexprChild->Pop();

	if (NULL != pgexprParent &&
		COperator::EopPhysicalSort == pgexprParent->Pop()->Eopid() &&
		COperator::EopPhysicalMotionGather == popChild->Eopid())
	{
		// prevent (Sort --> GatherMerge), since Sort destroys order maintained by GatherMerge
		return !CPhysicalMotionGather::PopConvert(popChild)->FOrderPreserving();
	}


	return COptimizationContext::FOptimize(m_mp, pgexprParent, pgexprChild,
										   pocChild, UlSearchStages());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPruneWithDPEStats
//
//	@doc:
//		Determine if a plan rooted by given group expression can be safely
//		pruned during optimization when stats for Dynamic Partition Elimination
//		are derived
//
//---------------------------------------------------------------------------
BOOL
CEngine::FSafeToPruneWithDPEStats(CGroupExpression *pgexpr,
								  CReqdPropPlan *,	// prpp
								  CCostContext *pccChild, ULONG child_index)
{
	GPOS_ASSERT(GPOS_FTRACE(EopttraceDeriveStatsForDPE));
	GPOS_ASSERT(GPOS_FTRACE(EopttraceEnableSpacePruning));

	if (NULL == pccChild)
	{
		// group expression has not been optimized yet

		CDrvdPropRelational *pdprel =
			CDrvdPropRelational::GetRelationalProperties(
				pgexpr->Pgroup()->Pdp());
		if (0 < pdprel->GetPartitionInfo()->UlConsumers())
		{
			// we cannot bound cost here because of possible DPE that can happen below the operator
			return false;
		}

		return true;
	}

	// first child has been optimized
	CExpressionHandle exprhdl(m_mp);
	exprhdl.Attach(pgexpr);
	ULONG ulNextChild = exprhdl.UlNextOptimizedChildIndex(child_index);
	CDrvdPropRelational *pdprelChild =
		CDrvdPropRelational::GetRelationalProperties(
			(*pgexpr)[ulNextChild]->Pdp());
	if (0 < pdprelChild->GetPartitionInfo()->UlConsumers())
	{
		// we cannot bound cost here because of possible DPE that can happen for the unoptimized child
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPrune
//
//	@doc:
//		Determine if a plan rooted by given group expression can be safely
//		pruned during optimization
//
//---------------------------------------------------------------------------
BOOL
CEngine::FSafeToPrune(
	CGroupExpression *pgexpr, CReqdPropPlan *prpp, CCostContext *pccChild,
	ULONG child_index,
	CCost *pcostLowerBound	// output: a lower bound on plan's cost
)
{
	GPOS_ASSERT(NULL != pcostLowerBound);
	*pcostLowerBound = GPOPT_INVALID_COST;

	if (!GPOS_FTRACE(EopttraceEnableSpacePruning))
	{
		// space pruning is disabled
		return false;
	}

	if (GPOS_FTRACE(EopttraceDeriveStatsForDPE) &&
		!FSafeToPruneWithDPEStats(pgexpr, prpp, pccChild, child_index))
	{
		// stat derivation for Dynamic Partition Elimination may not allow non-trivial cost bounds

		return false;
	}

	// check if container group has a plan for given properties
	CGroup *pgroup = pgexpr->Pgroup();
	COptimizationContext *pocGroup =
		pgroup->PocLookupBest(m_mp, UlSearchStages(), prpp);
	if (NULL != pocGroup && NULL != pocGroup->PccBest())
	{
		// compute a cost lower bound for the equivalent plan rooted by given group expression
		CCost costLowerBound =
			pgexpr->CostLowerBound(m_mp, prpp, pccChild, child_index);
		*pcostLowerBound = costLowerBound;
		if (costLowerBound > pocGroup->PccBest()->Cost())
		{
			// group expression cannot deliver a better plan for given properties and can be safely pruned
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Pmemotmap
//
//	@doc:
//		Build tree map on memo
//
//---------------------------------------------------------------------------
MemoTreeMap *
CEngine::Pmemotmap()
{
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();

	if (NULL == m_pmemo->Pmemotmap())
	{
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_mp) COptimizationContext(
			m_mp, PgroupRoot(), m_pqc->Prpp(),
			GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(
				m_mp)),	 // pass empty required relational properties initially
			GPOS_NEW(m_mp)
				IStatisticsArray(m_mp),	 // pass empty stats context initially
			0							 // ulSearchStageIndex
		);

		m_pmemo->BuildTreeMap(poc);
		optimizer_config->GetEnumeratorCfg()->SetPlanSpaceSize(
			m_pmemo->Pmemotmap()->UllCount());

		poc->Release();
	}

	return m_pmemo->Pmemotmap();
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ApplyTransformations
//
//	@doc:
//		Applies given set of xforms to group expression and insert
//		results to memo
//
//---------------------------------------------------------------------------
void
CEngine::ApplyTransformations(CMemoryPool *pmpLocal, CXformSet *xform_set,
							  CGroupExpression *pgexpr)
{
	// iterate over xforms
	CXformSetIter xsi(*xform_set);
	while (xsi.Advance())
	{
		GPOS_CHECK_ABORT;
		CXform *pxform = CXformFactory::Pxff()->Pxf(xsi.TBit());

		// transform group expression, and insert results to memo
		CXformResult *pxfres = GPOS_NEW(m_mp) CXformResult(m_mp);
		ULONG ulElapsedTime = 0;
		ULONG ulNumberOfBindings = 0;
		pgexpr->Transform(m_mp, pmpLocal, pxform, pxfres, &ulElapsedTime,
						  &ulNumberOfBindings);
		InsertXformResult(pgexpr->Pgroup(), pxfres, pxform->Exfid(), pgexpr,
						  ulElapsedTime, ulNumberOfBindings);
		pxfres->Release();

		if (PssCurrent()->FTimedOut())
		{
			break;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::TransitionGroupExpression
//
//	@doc:
//		Transition group expression to a given target state
//
//---------------------------------------------------------------------------
void
CEngine::TransitionGroupExpression(CMemoryPool *pmpLocal,
								   CGroupExpression *pgexpr,
								   CGroupExpression::EState estTarget)
{
	GPOS_ASSERT(CGroupExpression::estExplored == estTarget ||
				CGroupExpression::estImplemented == estTarget);

	if (PssCurrent()->FTimedOut())
	{
		return;
	}

	CGroupExpression::EState estInitial = CGroupExpression::estExploring;
	CGroup::EState estGroupTargetState = CGroup::estExplored;
	if (CGroupExpression::estImplemented == estTarget)
	{
		estInitial = CGroupExpression::estImplementing;
		estGroupTargetState = CGroup::estImplemented;
	}

	pgexpr->SetState(estInitial);

	// transition all child groups
	ULONG arity = pgexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		TransitionGroup(pmpLocal, (*pgexpr)[i], estGroupTargetState);

		GPOS_CHECK_ABORT;
	}

	// find which set of xforms should be used
	CXformSet *xform_set = CXformFactory::Pxff()->PxfsExploration();
	if (CGroupExpression::estImplemented == estTarget)
	{
		xform_set = CXformFactory::Pxff()->PxfsImplementation();
	}

	// get all applicable xforms
	COperator *pop = pgexpr->Pop();
	CXformSet *pxfsCandidates = CLogical::PopConvert(pop)->PxfsCandidates(m_mp);

	// intersect them with the required set of xforms, then apply transformations
	pxfsCandidates->Intersection(xform_set);
	pxfsCandidates->Intersection(PxfsCurrentStage());
	ApplyTransformations(pmpLocal, pxfsCandidates, pgexpr);
	pxfsCandidates->Release();

	pgexpr->SetState(estTarget);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::TransitionGroup
//
//	@doc:
//		Transition group to a target state
//
//---------------------------------------------------------------------------
void
CEngine::TransitionGroup(CMemoryPool *pmpLocal, CGroup *pgroup,
						 CGroup::EState estTarget)
{
	// check stack size
	GPOS_CHECK_STACK_SIZE;

	if (PssCurrent()->FTimedOut())
	{
		return;
	}

	GPOS_ASSERT(CGroup::estExplored == estTarget ||
				CGroup::estImplemented == estTarget);

	BOOL fTransitioned = false;
	{
		CGroupProxy gp(pgroup);
		fTransitioned = gp.FTransitioned(estTarget);
	}

	// check if we can end recursion early
	if (!fTransitioned)
	{
		CGroup::EState estInitial = CGroup::estExploring;
		CGroupExpression::EState estGExprTargetState =
			CGroupExpression::estExplored;
		if (CGroup::estImplemented == estTarget)
		{
			estInitial = CGroup::estImplementing;
			estGExprTargetState = CGroupExpression::estImplemented;
		}

		CGroupExpression *pgexprCurrent = NULL;

		// transition group's state to initial state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(estInitial);
		}

		// get first group expression
		{
			CGroupProxy gp(pgroup);
			pgexprCurrent = gp.PgexprFirst();
		}

		while (NULL != pgexprCurrent)
		{
			if (!pgexprCurrent->FTransitioned(estGExprTargetState))
			{
				TransitionGroupExpression(pmpLocal, pgexprCurrent,
										  estGExprTargetState);
			}

			if (PssCurrent()->FTimedOut())
			{
				break;
			}

			// get next group expression
			{
				CGroupProxy gp(pgroup);
				pgexprCurrent = gp.PgexprNext(pgexprCurrent);
			}

			GPOS_CHECK_ABORT;
		}

		// transition group to target state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(estTarget);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PocChild
//
//	@doc:
//		Create optimization context for child group
//
//---------------------------------------------------------------------------
COptimizationContext *
CEngine::PocChild(
	CGroupExpression *pgexpr,		  // parent expression
	COptimizationContext *pocOrigin,  // optimization context of parent operator
	CExpressionHandle
		&exprhdlPlan,  // handle to compute required plan properties
	CExpressionHandle
		&exprhdlRel,  // handle to compute required relational properties
	CDrvdPropArray
		*pdrgpdpChildren,  // derived plan properties of optimized children
	IStatisticsArray *pdrgpstatCurrentCtxt, ULONG child_index, ULONG ulOptReq)
{
	GPOS_ASSERT(exprhdlPlan.Pgexpr() == pgexpr);
	GPOS_ASSERT(NULL != pocOrigin);
	GPOS_ASSERT(NULL != pdrgpdpChildren);
	GPOS_ASSERT(NULL != pdrgpstatCurrentCtxt);

	CGroup *pgroupChild = (*pgexpr)[child_index];

	// compute required properties of the n-th child
	exprhdlPlan.ComputeChildReqdProps(child_index, pdrgpdpChildren, ulOptReq);
	exprhdlPlan.Prpp(child_index)->AddRef();

	// use current stats for optimizing current child
	IStatisticsArray *stats_ctxt = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	CUtils::AddRefAppend<IStatistics, CleanupStats>(stats_ctxt,
													pdrgpstatCurrentCtxt);

	// compute required relational properties
	CReqdPropRelational *prprel = NULL;
	if (CPhysical::PopConvert(pgexpr->Pop())->FPassThruStats())
	{
		// copy requirements from origin context
		prprel = pocOrigin->GetReqdRelationalProps();
	}
	else
	{
		// retrieve requirements from handle
		prprel = exprhdlRel.GetReqdRelationalProps(child_index);
	}
	GPOS_ASSERT(NULL != prprel);
	prprel->AddRef();

	COptimizationContext *pocChild = GPOS_NEW(m_mp)
		COptimizationContext(m_mp, pgroupChild, exprhdlPlan.Prpp(child_index),
							 prprel, stats_ctxt, m_ulCurrSearchStage);

	return pocChild;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PccOptimizeChild
//
//	@doc:
//		Optimize child group and return best cost context satisfying
//		required properties
//
//---------------------------------------------------------------------------
CCostContext *
CEngine::PccOptimizeChild(
	CExpressionHandle &exprhdl,	 // initialized with required properties
	CExpressionHandle &exprhdlRel,
	COptimizationContext *pocOrigin,  // optimization context of parent operator
	CDrvdPropArray *pdrgpdp, IStatisticsArray *pdrgpstatCurrentCtxt,
	ULONG child_index, ULONG ulOptReq)
{
	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	CGroup *pgroupChild = (*exprhdl.Pgexpr())[child_index];

	// create optimization context for child group
	COptimizationContext *pocChild =
		PocChild(pgexpr, pocOrigin, exprhdl, exprhdlRel, pdrgpdp,
				 pdrgpstatCurrentCtxt, child_index, ulOptReq);

	if (pgroupChild == pgexpr->Pgroup() && pocChild->Matches(pocOrigin))
	{
		// child context is the same as origin context, this is a deadlock
		pocChild->Release();
		return NULL;
	}

	// optimize child group
	CGroupExpression *pgexprChildBest =
		PgexprOptimize(pgroupChild, pocChild, pgexpr);
	pocChild->Release();
	if (NULL == pgexprChildBest || PssCurrent()->FTimedOut())
	{
		// failed to generate a plan for the child, or search stage is timed-out
		return NULL;
	}

	// derive plan properties of child group optimal implementation
	COptimizationContext *pocFound = pgroupChild->PocLookupBest(
		m_mp, m_search_stage_array->Size(), exprhdl.Prpp(child_index));
	GPOS_ASSERT(NULL != pocFound);

	CCostContext *pccChildBest = pocFound->PccBest();
	GPOS_ASSERT(NULL != pccChildBest);

	// check if optimization can be early terminated after first child has been optimized
	CCost costLowerBound(GPOPT_INVALID_COST);
	if (exprhdl.UlFirstOptimizedChildIndex() == child_index &&
		FSafeToPrune(pgexpr, pocOrigin->Prpp(), pccChildBest, child_index,
					 &costLowerBound))
	{
		// failed to optimize child due to cost bounding
		(void) pgexpr->PccComputeCost(m_mp, pocOrigin, ulOptReq,
									  NULL /*pdrgpoc*/, true /*fPruned*/,
									  costLowerBound);
		return NULL;
	}

	return pccChildBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PdrgpocOptimizeChildren
//
//	@doc:
//		Optimize child groups of a given group expression;
//
//---------------------------------------------------------------------------
COptimizationContextArray *
CEngine::PdrgpocOptimizeChildren(
	CExpressionHandle &exprhdl,		  // initialized with required properties
	COptimizationContext *pocOrigin,  // optimization context of parent operator
	ULONG ulOptReq)
{
	GPOS_ASSERT(NULL != exprhdl.Pgexpr());

	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	const ULONG arity = exprhdl.Arity();
	if (0 == arity)
	{
		// return empty array if no children
		return GPOS_NEW(m_mp) COptimizationContextArray(m_mp);
	}

	// create array of child derived properties
	CDrvdPropArray *pdrgpdp = GPOS_NEW(m_mp) CDrvdPropArray(m_mp);

	// initialize current stats context with input stats context
	IStatisticsArray *pdrgpstatCurrentCtxt =
		GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	CUtils::AddRefAppend<IStatistics, CleanupStats>(pdrgpstatCurrentCtxt,
													pocOrigin->Pdrgpstat());

	// initialize required relational properties computation
	CExpressionHandle exprhdlRel(m_mp);
	CGroupExpression *pgexprForStats =
		pgexpr->Pgroup()->PgexprBestPromise(m_mp, pgexpr);
	if (NULL != pgexprForStats)
	{
		exprhdlRel.Attach(pgexprForStats);
		exprhdlRel.DeriveProps(NULL /*pdpctxt*/);
		exprhdlRel.ComputeReqdProps(pocOrigin->GetReqdRelationalProps(),
									0 /*ulOptReq*/);
	}

	// iterate over child groups and optimize them
	BOOL fSuccess = true;
	ULONG child_index = exprhdl.UlFirstOptimizedChildIndex();
	do
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[child_index];
		if (pgroupChild->FScalar())
		{
			// skip scalar groups from optimization
			continue;
		}

		CCostContext *pccChildBest =
			PccOptimizeChild(exprhdl, exprhdlRel, pocOrigin, pdrgpdp,
							 pdrgpstatCurrentCtxt, child_index, ulOptReq);
		if (NULL == pccChildBest)
		{
			fSuccess = false;
			break;
		}

		CExpressionHandle exprhdlChild(m_mp);
		exprhdlChild.Attach(pccChildBest);
		exprhdlChild.DerivePlanPropsForCostContext();
		exprhdlChild.Pdp()->AddRef();
		pdrgpdp->Append(exprhdlChild.Pdp());

		// copy stats of child's best cost context to current stats context
		IStatistics *pstat = pccChildBest->Pstats();
		pstat->AddRef();
		pdrgpstatCurrentCtxt->Append(pstat);

		GPOS_CHECK_ABORT;
	} while (exprhdl.FNextChildIndex(&child_index));
	pdrgpdp->Release();
	pdrgpstatCurrentCtxt->Release();

	if (!fSuccess)
	{
		return NULL;
	}

	// return child optimization contexts array
	return PdrgpocChildren(m_mp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::OptimizeGroupExpression
//
//	@doc:
//		Optimize group expression under a given context
//
//---------------------------------------------------------------------------
void
CEngine::OptimizeGroupExpression(CGroupExpression *pgexpr,
								 COptimizationContext *poc)
{
	CGroup *pgroup = pgexpr->Pgroup();
	const ULONG ulOptRequests =
		CPhysical::PopConvert(pgexpr->Pop())->UlOptRequests();
	for (ULONG ul = 0; ul < ulOptRequests; ul++)
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pgexpr);
		exprhdl.DeriveProps(NULL /*pdpctxt*/);

		// check if group expression optimization can be early terminated without optimizing any child
		CCost costLowerBound(GPOPT_INVALID_COST);
		if (FSafeToPrune(pgexpr, poc->Prpp(), NULL /*pccChild*/,
						 gpos::ulong_max /*child_index*/, &costLowerBound))
		{
			(void) pgexpr->PccComputeCost(m_mp, poc, ul, NULL /*pdrgpoc*/,
										  true /*fPruned*/, costLowerBound);
			continue;
		}

		if (FCheckReqdProps(exprhdl, poc->Prpp(), ul))
		{
			// load required properties on the handle
			exprhdl.InitReqdProps(poc->Prpp());

			// optimize child groups
			COptimizationContextArray *pdrgpoc =
				PdrgpocOptimizeChildren(exprhdl, poc, ul);

			if (NULL != pdrgpoc &&
				FCheckEnfdProps(m_mp, pgexpr, poc, ul, pdrgpoc))
			{
				// compute group expression cost under the current optimization context
				CCostContext *pccComputed = pgexpr->PccComputeCost(
					m_mp, poc, ul, pdrgpoc, false /*fPruned*/, CCost(0.0));

				if (NULL != pccComputed)
				{
					// update best group expression under the current optimization context
					pgroup->UpdateBestCost(poc, pccComputed);
				}
			}

			CRefCount::SafeRelease(pdrgpoc);
		}

		GPOS_CHECK_ABORT;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprOptimize
//
//	@doc:
//		Optimize group under a given context;
//
//---------------------------------------------------------------------------
CGroupExpression *
CEngine::PgexprOptimize(CGroup *pgroup, COptimizationContext *poc,
						CGroupExpression *pgexprOrigin)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;

	COptimizationContext *pocFound = pgroup->PocInsert(poc);
	if (poc != pocFound)
	{
		GPOS_ASSERT(COptimizationContext::estOptimized == pocFound->Est());
		return pocFound->PgexprBest();
	}

	// add-ref context to pin it in hash table
	poc->AddRef();
	poc->SetState(COptimizationContext::estOptimizing);

	EOptimizationLevel eolCurrent = pgroup->EolMax();
	while (EolSentinel != eolCurrent)
	{
		CGroupExpression *pgexprCurrent = NULL;
		{
			CGroupProxy gp(pgroup);
			pgexprCurrent = gp.PgexprSkipLogical(NULL /*pgexpr*/);
		}

		while (NULL != pgexprCurrent)
		{
			if (FOptimizeChild(pgexprOrigin, pgexprCurrent, poc, eolCurrent))
			{
				OptimizeGroupExpression(pgexprCurrent, poc);
			}

			if (PssCurrent()->FTimedOut())
			{
				break;
			}

			// move to next group expression
			{
				CGroupProxy gp(pgroup);
				pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
			}
		}

		// damp optimization level
		eolCurrent = EolDamp(eolCurrent);

		GPOS_CHECK_ABORT;
	}
	poc->SetState(COptimizationContext::estOptimized);

	return poc->PgexprBest();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Explore
//
//	@doc:
//		Apply all exploration xforms
//
//---------------------------------------------------------------------------
void
CEngine::Explore()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != PgroupRoot());

	// explore root group
	GPOS_ASSERT(!PgroupRoot()->FExplored());

	TransitionGroup(m_mp, PgroupRoot(), CGroup::estExplored /*estTarget*/);
	GPOS_ASSERT_IMP(!PssCurrent()->FTimedOut(), PgroupRoot()->FExplored());
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Implement
//
//	@doc:
//		Apply all implementation xforms
//
//---------------------------------------------------------------------------
void
CEngine::Implement()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != PgroupRoot());

	// implement root group
	GPOS_ASSERT(!PgroupRoot()->FImplemented());

	TransitionGroup(m_mp, PgroupRoot(), CGroup::estImplemented /*estTarget*/);
	GPOS_ASSERT_IMP(!PssCurrent()->FTimedOut(), PgroupRoot()->FImplemented());
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::RecursiveOptimize
//
//	@doc:
//		Recursive optimization
//
//---------------------------------------------------------------------------
void
CEngine::RecursiveOptimize()
{
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();

	CAutoTimer at("\n[OPT]: Total Optimization Time",
				  GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	const ULONG ulSearchStages = m_search_stage_array->Size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// apply exploration xforms
		Explore();

		// run exploration completion operations
		FinalizeExploration();

		// apply implementation xforms
		Implement();

		// run implementation completion operations
		FinalizeImplementation();

		// optimize root group
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_mp) COptimizationContext(
			m_mp, PgroupRoot(), m_pqc->Prpp(),
			GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(
				m_mp)),	 // pass empty required relational properties initially
			GPOS_NEW(m_mp) IStatisticsArray(
				m_mp),	// pass an empty stats context initially
			m_ulCurrSearchStage);
		(void) PgexprOptimize(PgroupRoot(), poc, NULL /*pgexprOrigin*/);
		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan = m_pmemo->PexprExtractPlan(
			m_mp, m_pmemo->PgroupRoot(), m_pqc->Prpp(),
			m_search_stage_array->Size());
		PssCurrent()->SetBestExpr(pexprPlan);

		FinalizeSearchStage();
	}

	{
		CAutoTrace atSearch(m_mp);
		atSearch.Os() << "[OPT]: Search terminated at stage "
					  << m_ulCurrSearchStage << "/"
					  << m_search_stage_array->Size();
	}

	if (optimizer_config->GetEnumeratorCfg()->FSample())
	{
		SamplePlans();
	}
}

void
CEngine::DbgPrintExpr(int group_no, int context_no)
{
	CAutoTrace at(m_mp);

	GPOS_TRY
	{
		CGroup *top_group = m_pmemo->Pgroup(group_no);
		if (NULL != top_group)
		{
			COptimizationContext *poc = top_group->Ppoc(context_no);

			if (NULL != poc)
			{
				CExpression *extracted_expr = m_pmemo->PexprExtractPlan(
					m_mp, top_group, poc->Prpp(), m_search_stage_array->Size());
				extracted_expr->OsPrint(at.Os());
				extracted_expr->Release();
			}
			else
			{
				at.Os() << "error: invalid context number";
			}
		}
		else
		{
			at.Os() << "error: invalid group number";
		}
	}
	GPOS_CATCH_EX(ex)
	{
		at.Os() << "error, couldn't complete the request";
	}
	GPOS_CATCH_END;
}

#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PdrgpocChildren
//
//	@doc:
//		Return array of child optimization contexts corresponding
//		to handle requirements
//
//---------------------------------------------------------------------------
COptimizationContextArray *
CEngine::PdrgpocChildren(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(NULL != exprhdl.Pgexpr());

	COptimizationContextArray *pdrgpoc =
		GPOS_NEW(mp) COptimizationContextArray(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->FScalar())
		{
			COptimizationContext *poc = pgroupChild->PocLookupBest(
				mp, m_search_stage_array->Size(), exprhdl.Prpp(ul));
			GPOS_ASSERT(NULL != poc);

			poc->AddRef();
			pdrgpoc->Append(poc);
		}
	}

	return pdrgpoc;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::ScheduleMainJob
//
//	@doc:
//		Create and schedule the main optimization job
//
//---------------------------------------------------------------------------
void
CEngine::ScheduleMainJob(CSchedulerContext *psc, COptimizationContext *poc)
{
	GPOS_ASSERT(NULL != PgroupRoot());

	CJobGroupOptimization::ScheduleJob(psc, PgroupRoot(), NULL /*pgexprOrigin*/,
									   poc, NULL /*pjParent*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeExploration
//
//	@doc:
//		Execute operations after exploration completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeExploration()
{
	GroupMerge();

	if (m_pqc->FDeriveStats())
	{
		// derive statistics
		m_pmemo->ResetStats();
		DeriveStats(m_mp);
	}

	if (!GPOS_FTRACE(EopttraceDonotDeriveStatsForAllGroups))
	{
		// derive stats for every group without stats
		m_pmemo->DeriveStatsIfAbsent(m_mp);
	}

	if (GPOS_FTRACE(EopttracePrintMemoAfterExploration))
	{
		{
			CAutoTrace at(m_mp);
			at.Os() << "MEMO after exploration (stage " << m_ulCurrSearchStage
					<< ")" << std::endl;
		}

		{
			CAutoTrace at(m_mp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_mp);
		(void) OsPrintMemoryConsumption(
			at.Os(), "Memory consumption after exploration ");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeImplementation
//
//	@doc:
//		Execute operations after implementation completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeImplementation()
{
	if (GPOS_FTRACE(EopttracePrintMemoAfterImplementation))
	{
		{
			CAutoTrace at(m_mp);
			at.Os() << "MEMO after implementation (stage "
					<< m_ulCurrSearchStage << ")" << std::endl;
		}

		{
			CAutoTrace at(m_mp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_mp);
		(void) OsPrintMemoryConsumption(
			at.Os(), "Memory consumption after implementation ");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeSearchStage
//
//	@doc:
//		Execute operations after search stage completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeSearchStage()
{
	ProcessTraceFlags();

	m_xforms->Release();
	m_xforms = NULL;
	m_xforms = GPOS_NEW(m_mp) CXformSet(m_mp);

	m_ulCurrSearchStage++;
	m_pmemo->ResetGroupStates();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintActivatedXforms
//
//	@doc:
//		Print activated xform
//
//---------------------------------------------------------------------------
void
CEngine::PrintActivatedXforms(IOstream &os) const
{
	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		os << std::endl
		   << "[OPT]: <Begin Xforms - stage " << m_ulCurrSearchStage << ">"
		   << std::endl;
		CXformSetIter xsi(*m_xforms);
		while (xsi.Advance())
		{
			CXform *pxform = CXformFactory::Pxff()->Pxf(xsi.TBit());
			ULONG ulCalls = (ULONG)(
				*m_pdrgpulpXformCalls)[m_ulCurrSearchStage][pxform->Exfid()];
			ULONG ulTime = (ULONG)(
				*m_pdrgpulpXformTimes)[m_ulCurrSearchStage][pxform->Exfid()];
			ULONG ulBindings = (ULONG)(
				*m_pdrgpulpXformBindings)[m_ulCurrSearchStage][pxform->Exfid()];
			ULONG ulResults = (ULONG)(
				*m_pdrgpulpXformResults)[m_ulCurrSearchStage][pxform->Exfid()];
			os << pxform->SzId() << ": " << ulCalls << " calls, " << ulBindings
			   << " total bindings, " << ulResults
			   << " alternatives generated, " << ulTime << "ms" << std::endl;
		}
		os << "[OPT]: <End Xforms - stage " << m_ulCurrSearchStage << ">"
		   << std::endl;
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintMemoryConsumption
//
//	@doc:
//		Print current memory consumption
//
//---------------------------------------------------------------------------
IOstream &
CEngine::OsPrintMemoryConsumption(IOstream &os, const CHAR *szHeader) const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CMDAccessor::MDCache *pcache = md_accessor->Pcache();

	os << std::endl
	   << szHeader << "Engine: ["
	   << (DOUBLE) m_mp->TotalAllocatedSize() / GPOPT_MEM_UNIT << "] "
	   << GPOPT_MEM_UNIT_NAME << ", MD Cache: ["
	   << (DOUBLE)(pcache->TotalAllocatedSize()) / GPOPT_MEM_UNIT << "] "
	   << GPOPT_MEM_UNIT_NAME << ", Total: ["
	   << (DOUBLE)(
			  CMemoryPoolManager::GetMemoryPoolMgr()->TotalAllocatedSize()) /
			  GPOPT_MEM_UNIT
	   << "] " << GPOPT_MEM_UNIT_NAME;

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::ProcessTraceFlags
//
//	@doc:
//		Process trace flags after optimization is complete
//
//---------------------------------------------------------------------------
void
CEngine::ProcessTraceFlags()
{
	if (GPOS_FTRACE(EopttracePrintMemoAfterOptimization))
	{
		{
			CAutoTrace at(m_mp);
			at.Os() << "MEMO after optimization (stage " << m_ulCurrSearchStage
					<< "):" << std::endl;
		}

		{
			CAutoTrace at(m_mp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_mp);

		// print optimization stats
		at.Os() << std::endl
				<< "[OPT]: Memo (stage " << m_ulCurrSearchStage << "): ["
				<< (ULONG)(m_pmemo->UlpGroups()) << " groups"
				<< ", " << m_pmemo->UlDuplicateGroups() << " duplicate groups"
				<< ", " << m_pmemo->UlGrpExprs() << " group expressions"
				<< ", " << m_xforms->Size() << " activated xforms]";

		at.Os() << std::endl
				<< "[OPT]: stage " << m_ulCurrSearchStage << " completed in "
				<< PssCurrent()->UlElapsedTime() << " msec, ";
		if (NULL == PssCurrent()->PexprBest())
		{
			at.Os() << " no plan was found";
		}
		else
		{
			at.Os() << " plan with cost " << PssCurrent()->CostBest()
					<< " was found";
		}

		PrintActivatedXforms(at.Os());

		(void) OsPrintMemoryConsumption(
			at.Os(), "Memory consumption after optimization ");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Optimize
//
//	@doc:
//		Main driver of optimization engine
//
//---------------------------------------------------------------------------
void
CEngine::Optimize()
{
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();

	CAutoTimer at("\n[OPT]: Total Optimization Time",
				  GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	GPOS_ASSERT(NULL != PgroupRoot());
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS());

	const ULONG ulJobs =
		std::min((ULONG) GPOPT_JOBS_CAP,
				 (ULONG)(m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));  // min(最大的 optimization job 数量, 每个 group 中的job数量×group数量)
	CJobFactory jf(m_mp, ulJobs);  // 构造 jobfactory(内存池，job的最大数量)
	CScheduler sched(m_mp, ulJobs);  // 构造调度器

	CSchedulerContext sc;
	sc.Init(m_mp, &jf, &sched, this);  // 初始化调度器的上下文

	const ULONG ulSearchStages = m_search_stage_array->Size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// optimize root group
		m_pqc->Prpp()->AddRef();  // 查询上下文(Engine中的成员变量)-> 计划性质访问器(plan properties )->
		COptimizationContext *poc = GPOS_NEW(m_mp) COptimizationContext(  // 构造优化器上下文。优化器上下文存储了优化器构造的 plan 需要满足的性质
			m_mp, PgroupRoot(), m_pqc->Prpp(),
			GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(
				m_mp)),	 // pass empty required relational properties initially
			GPOS_NEW(m_mp)
				IStatisticsArray(m_mp),	 // pass empty stats context initially
			m_ulCurrSearchStage);

		// schedule main optimization job
		ScheduleMainJob(&sc, poc);  // poc 是优化器上下文，将根group的job加入到等待队列

		// run optimization job
		CScheduler::Run(&sc);

		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan = m_pmemo->PexprExtractPlan(
			m_mp, m_pmemo->PgroupRoot(), m_pqc->Prpp(),
			m_search_stage_array->Size());
		//************************ARENA**************************/
		// 用于调试
		// reset_plan();
		// output_plan(pexprPlan);  // output the best plan
		//************************ARENA**************************/
		PssCurrent()->SetBestExpr(pexprPlan);

		FinalizeSearchStage();
	}


	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace atSearch(m_mp);
		atSearch.Os() << "[OPT]: Search terminated at stage "
					  << m_ulCurrSearchStage << "/"
					  << m_search_stage_array->Size();
	}


	if (optimizer_config->GetEnumeratorCfg()->FSample())
	{
		SamplePlans();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CExpression *
CEngine::PexprUnrank(ULLONG plan_id)
{
	// The CTE map will be updated by the Producer instead of the Sequence operator
	// because we are doing a DFS traversal of the TreeMap.
	CDrvdPropCtxtPlan *pdpctxtplan =
		GPOS_NEW(m_mp) CDrvdPropCtxtPlan(m_mp, false /*fUpdateCTEMap*/);
	CExpression *pexpr = Pmemotmap()->PrUnrank(m_mp, pdpctxtplan, plan_id);
	pdpctxtplan->Release();

#ifdef GPOS_DEBUG
	// check plan using configured plan checker, if any
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	CEnumeratorConfig *pec = optimizer_config->GetEnumeratorCfg();
	BOOL fCheck = pec->FCheckPlan(pexpr);
	if (!fCheck)
	{
		CAutoTrace at(m_mp);
		at.Os() << "\nextracted plan failed PlanChecker function: " << std::endl
				<< *pexpr;
	}
	GPOS_ASSERT(fCheck);
#endif	// GPOS_DEBUG

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PexprExtractPlan
//
//	@doc:
//		Extract a physical plan from the memo
//
//---------------------------------------------------------------------------
CExpression *
CEngine::PexprExtractPlan()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != m_pmemo);
	GPOS_ASSERT(NULL != m_pmemo->PgroupRoot());

	BOOL fGenerateAlt = false;
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	CEnumeratorConfig *pec = optimizer_config->GetEnumeratorCfg();
	if (pec->FEnumerate())
	{
		CAutoTrace at(m_mp);
		ULLONG ullCount = Pmemotmap()->UllCount();
		at.Os() << "[OPT]: Number of plan alternatives: " << ullCount
				<< std::endl;

		if (0 < pec->GetPlanId())
		{
			if (pec->GetPlanId() > ullCount)
			{
				// an invalid plan number is chosen
				GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiInvalidPlanAlternative,
						   pec->GetPlanId(), ullCount);
			}

			// a valid plan number was chosen
			fGenerateAlt = true;
		}
	}

	CExpression *pexpr = NULL;
	if (fGenerateAlt)
	{
		pexpr = PexprUnrank(pec->GetPlanId() -
							1 /*rank of plan alternative is zero-based*/);
		CAutoTrace at(m_mp);
		at.Os() << "[OPT]: Successfully generated plan: " << pec->GetPlanId()
				<< std::endl;
	}
	else
	{
		pexpr = m_pmemo->PexprExtractPlan(m_mp, m_pmemo->PgroupRoot(),
										  m_pqc->Prpp(),
										  m_search_stage_array->Size());
	}

	if (NULL == pexpr)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound);
	}

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::UllRandomPlanId
//
//	@doc:
//		Generate random plan id
//
//---------------------------------------------------------------------------
ULLONG
CEngine::UllRandomPlanId(ULONG *seed)
{
	ULLONG ullCount = Pmemotmap()->UllCount();
	ULLONG plan_id = 0;
	++seed;
	// do
	// {
	// 	plan_id = clib::Rand(seed);
	// } while (plan_id >= ullCount);
	std::default_random_engine e(time(NULL));
	std::uniform_int_distribution<ULLONG> u(0, ullCount);
	do
	{
		plan_id = u(e);
	} while( find_index.find(plan_id) != find_index.end());  // 当这个 id 已经被记录了，重复找
	find_index.insert(plan_id);

	return plan_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidPlanSample
//
//	@doc:
//		Extract a plan sample and handle exceptions according to enumerator
//		configurations
//
//---------------------------------------------------------------------------
BOOL
CEngine::FValidPlanSample(CEnumeratorConfig *pec, ULLONG plan_id,
						  CExpression **ppexpr	// output: extracted plan
)
{
	GPOS_ASSERT(NULL != pec);
	GPOS_ASSERT(NULL != ppexpr);

	BOOL fValidPlan = true;
	if (pec->FSampleValidPlans())
	{
		// if enumerator is configured to extract valid plans only,
		// we extract plan and catch invalid plan exception here
		GPOS_TRY
		{
			*ppexpr = PexprUnrank(plan_id);
		}
		GPOS_CATCH_EX(ex)
		{
			if (GPOS_MATCH_EX(ex, gpopt::ExmaGPOPT,
							  gpopt::ExmiUnsatisfiedRequiredProperties))
			{
				GPOS_RESET_EX;
				fValidPlan = false;
			}
			else
			{
				// for all other exceptions, we bail out
				GPOS_RETHROW(ex);
			}
		}
		GPOS_CATCH_END;
	}
	else
	{
		// otherwise, we extract plan and leave exception handling to the caller
		*ppexpr = PexprUnrank(plan_id);
	}

	return fValidPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::SamplePlans
//
//	@doc:
//		Sample distribution of possible plans uniformly;
//
//---------------------------------------------------------------------------
void
CEngine::SamplePlans()
{
	//************************ARENA**************************/
	// 测试采样时间
	auto time_start = std::chrono::steady_clock::now();
	std::ofstream fout_time("/tmp/time_record");
	std::ofstream fout_plan("/tmp/Plans");

	// 读取配置信息
	readConfig();
	//************************ARENA**************************/
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	GPOS_ASSERT(NULL != optimizer_config);

	CEnumeratorConfig *pec = optimizer_config->GetEnumeratorCfg();

	ULLONG ullSamples = pec->UllInputSamples();
	GPOS_ASSERT(0 < ullSamples);

	pec->ClearSamples();

	ULLONG ullCount = Pmemotmap()->UllCount();
	if (0 == ullCount)
	{
		// optimizer generated no plans
		return;
	}


	// generate full plan space when space size is less than or equal to
	// the required number of samples
	BOOL fGenerateAll = (ullSamples >= ullCount);

	ULLONG ullTargetSamples = ullSamples;
	if (fGenerateAll)
	{
		ullTargetSamples = ullCount;
	}

	// find cost of best plan
	CExpression *pexpr =
		m_pmemo->PexprExtractPlan(m_mp, m_pmemo->PgroupRoot(), m_pqc->Prpp(),
								  m_search_stage_array->Size());
	CCost costBest = pexpr->Cost();
	pec->SetBestCost(costBest);
	//************************ARENA**************************/
	// best plan 放在 plan_trees 中的第一个元素
	plan_buffer.push(pexpr);
	{												/////////////////////////
		PlanTree<CExpression> tempTree;							/////////////////////////
		tempTree.init(pexpr);						/////////////////////////
		fout_plan << tempTree.root->generate_json(); /////////////////////////
		fout_plan << "\n";								/////////////////////////
	}
	// plan_trees.push_back(PlanTree<CExpression>());
	// plan_trees.back().init(pexpr);
	// pexpr->Release();
	//************************ARENA**************************/

	// generate randomized seed using local time
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL /*timezone*/);
	ULONG seed = CombineHashes((ULONG) tv.tv_sec, (ULONG) tv.tv_usec);

	// set maximum number of iterations based number of samples
	// we use maximum iteration to prevent infinite looping below
	const ULLONG ullMaxIters = ullTargetSamples * GPOPT_SAMPLING_MAX_ITERS;
	ULLONG ullIters = 0;
	ULLONG ull = 0;

	//************************ARENA**************************/
	// 声明将 CExpression 转换 plantree 的线程
	// std::thread threadCE2Plan(CExpression2PlanTree);
	//************************ARENA**************************/

	while (ullIters < ullMaxIters && ull < ullTargetSamples)
	{
		// generate id of plan to be extracted
		ULLONG plan_id = ull;
		if (!fGenerateAll)
		{
			plan_id = UllRandomPlanId(&seed);
		}

		pexpr = NULL;
		BOOL fAccept = false;
		if (FValidPlanSample(pec, plan_id, &pexpr))
		{
			// add plan to the sample if it is below cost threshold
			CCost cost = pexpr->Cost();
			//************************ARENA**************************/
			// 保留该 plan，将其加入都队列中
			fAccept = pec->FAddSample(plan_id, cost);
			if (fAccept)
			{
				// m.lock();
				plan_buffer.push(pexpr);
				// m.unlock();
				// 输出到文件中用于测试
				{												/////////////////////////
					PlanTree<CExpression> tempTree;							/////////////////////////
					tempTree.init(pexpr);						/////////////////////////
					fout_plan << tempTree.root->generate_json(); /////////////////////////
					fout_plan << "\n";								/////////////////////////
				}
			}
			//************************ARENA**************************/
			pexpr = NULL;
		}

		if (fGenerateAll || fAccept)
		{
			ull++;
		}

		ullIters++;
	}
	fout_plan.close();

	//************************ARENA**************************/
	// m.lock();
	// plan_buffer.push(NULL);
	// m.unlock();

	// threadCE2Plan.join();
	auto time_end = std::chrono::steady_clock::now();  // 采样结束
	if (fout_time.is_open())  // 输出时间
	{
		fout_time << "采样花费的时间为：" << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count()
				  << "ms\n";
	}

	time_start = std::chrono::steady_clock::now();
	DealWithPlan();
	time_end = std::chrono::steady_clock::now();  // 计算结束
	if (fout_time.is_open())  // 输出时间
	{
		fout_time << "计算 k alternative plans 花费的时间为：" << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count()
				  << "ms\n";
	}
	fout_time.close();
	//************************ARENA**************************/

	pec->PrintPlanSample();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckEnfdProps
//
//	@doc:
//		Check enforceable properties and append enforcers to the current group if
//		required.
//
//		This check is done in two steps:
//
//		First, it determines if any particular property needs to be enforced at
//		all. For example, the EopttraceDisableSort traceflag can disable order
//		enforcement. Also, if there are no partitioned tables referenced in the
//		subtree, partition propagation enforcement can be skipped.
//
//		Second, EPET methods are called for each property to determine if an
//		enforcer needs to be added. These methods in turn call into virtual
//		methods in the different operators. For example, CPhysical::EpetOrder()
//		is used to determine a Sort node needs to be added to the group. These
//		methods are passed an expression handle (to access derived properties of
//		the subtree) and the required properties as a object of a subclass of
//		CEnfdProp.
//
//		Finally, based on return values of the EPET methods,
//		CEnfdProp::AppendEnforcers() is called for each of the enforced
//		properties.
//
//		Returns true if no enforcers were created because they were deemed
//		unnecessary or optional i.e all enforced properties were satisfied for
//		the group expression under the current optimization context.  Returns
//		false otherwise.
//
//		NB: This method is only concerned with a certain enforcer needs to be
//		added into the group. Once added, there is no connection between the
//		enforcer and the operator that created it. That is although some group
//		expression X created the enforcer E, later, during costing, E can still
//		decide to pick some other group expression Y for its child, since
//		theoretically, all group expressions in a group are equivalent.
//
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckEnfdProps(CMemoryPool *mp, CGroupExpression *pgexpr,
						 COptimizationContext *poc, ULONG ulOptReq,
						 COptimizationContextArray *pdrgpoc)
{
	GPOS_CHECK_ABORT;

	if (GPOS_FTRACE(EopttracePrintMemoEnforcement))
	{
		CAutoTrace at(m_mp);
		at.Os() << "CEngine::FCheckEnfdProps (Group ID: "
				<< pgexpr->Pgroup()->Id() << " Expression ID: " << pgexpr->Id()
				<< ")" << std::endl;
		m_pmemo->OsPrint(at.Os());
	}

	// check if all children could be successfully optimized
	if (!FChildrenOptimized(pdrgpoc))
	{
		return false;
	}

	// load a handle with derived plan properties
	poc->AddRef();
	pgexpr->AddRef();
	pdrgpoc->AddRef();
	CCostContext *pcc = GPOS_NEW(mp) CCostContext(mp, poc, ulOptReq, pgexpr);
	pcc->SetChildContexts(pdrgpoc);
	CExpressionHandle exprhdl(mp);
	exprhdl.Attach(pcc);
	exprhdl.DerivePlanPropsForCostContext();


	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	CReqdPropPlan *prpp = poc->Prpp();

	// check whether the current physical operator satisfies the CTE requirements
	// and whether it is a motion over unresolved part consumers
	if (!FValidCTEAndPartitionProperties(mp, exprhdl, prpp))
	{
		pcc->Release();
		return false;
	}

	// Determine if any property enforcement is disable or unnecessary
	BOOL fOrderReqd = !GPOS_FTRACE(EopttraceDisableSort) &&
					  !prpp->Peo()->PosRequired()->IsEmpty();

	// CPhysicalLeftOuterIndexNLJoin requires the inner child to be any
	// distribution but random. The OR makes an exception in this case.
	// This should be generalized when more physical operators require
	// this pattern. We need an explicit check for CPhysicalLeftOuterIndexNLJoin
	// when there are no motions, therefore we need to handle this exceptional
	// case here.
	//
	// Similar exceptions should be OR'd into fDistributionReqdException to
	// force checking EpetDistribution on the physical operation
	BOOL fDistributionReqdException =
		popPhysical->Eopid() == COperator::EopPhysicalLeftOuterIndexNLJoin;
	BOOL fDistributionReqd =
		!GPOS_FTRACE(EopttraceDisableMotions) &&
		((CDistributionSpec::EdtAny != prpp->Ped()->PdsRequired()->Edt()) ||
		 fDistributionReqdException);

	BOOL fRewindabilityReqd = !GPOS_FTRACE(EopttraceDisableSpool) &&
							  (prpp->Per()->PrsRequired()->IsCheckRequired());

	BOOL fPartPropagationReqd =
		!GPOS_FTRACE(EopttraceDisablePartPropagation) &&
		prpp->Pepp()->PppsRequired()->FPartPropagationReqd();

	// Determine if adding an enforcer to the group is required, optional,
	// unnecessary or prohibited over the group expression and given the current
	// optimization context (required properties)

	// get order enforcing type
	CEnfdProp::EPropEnforcingType epetOrder =
		prpp->Peo()->Epet(exprhdl, popPhysical, fOrderReqd);

	// get distribution enforcing type
	CEnfdProp::EPropEnforcingType epetDistribution = prpp->Ped()->Epet(
		exprhdl, popPhysical, prpp->Pepp()->PppsRequired(), fDistributionReqd);

	// get rewindability enforcing type
	CEnfdProp::EPropEnforcingType epetRewindability =
		prpp->Per()->Epet(exprhdl, popPhysical, fRewindabilityReqd);

	// get partition propagation enforcing type
	CEnfdProp::EPropEnforcingType epetPartitionPropagation =
		prpp->Pepp()->Epet(exprhdl, popPhysical, fPartPropagationReqd);

	// Skip adding enforcers entirely if any property determines it to be
	// 'prohibited'. In this way, a property may veto out the creation of an
	// enforcer for the current group expression and optimization context.
	//
	// NB: Even though an enforcer E is not added because of some group
	// expression G because it was prohibited, some other group expression H may
	// decide to add it. And if E is added, it is possible for E to consider both
	// G and H as its child.
	if (FProhibited(epetOrder, epetDistribution, epetRewindability,
					epetPartitionPropagation))
	{
		pcc->Release();
		return false;
	}

	CExpressionArray *pdrgpexprEnforcers = GPOS_NEW(mp) CExpressionArray(mp);

	// extract a leaf pattern from target group
	CBinding binding;
	CExpression *pexpr = binding.PexprExtract(
		m_mp, exprhdl.Pgexpr(), m_pexprEnforcerPattern, NULL /* pexprLast */);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pgexpr()->Pgroup() == pgexpr->Pgroup());

	prpp->Peo()->AppendEnforcers(mp, prpp, pdrgpexprEnforcers, pexpr, epetOrder,
								 exprhdl);
	prpp->Ped()->AppendEnforcers(mp, prpp, pdrgpexprEnforcers, pexpr,
								 epetDistribution, exprhdl);
	prpp->Per()->AppendEnforcers(mp, prpp, pdrgpexprEnforcers, pexpr,
								 epetRewindability, exprhdl);
	prpp->Pepp()->AppendEnforcers(mp, prpp, pdrgpexprEnforcers, pexpr,
								  epetPartitionPropagation, exprhdl);

	if (0 < pdrgpexprEnforcers->Size())
	{
		AddEnforcers(exprhdl.Pgexpr(), pdrgpexprEnforcers);
	}
	pdrgpexprEnforcers->Release();
	pexpr->Release();
	pcc->Release();

	return FOptimize(epetOrder, epetDistribution, epetRewindability,
					 epetPartitionPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidCTEAndPartitionProperties
//
//	@doc:
//		Check if the given expression has valid cte and partition properties
//		with respect to the given requirements. This function returns true iff
//		ALL the following conditions are met:
//		1. The expression satisfies the CTE requirements
//		2. The root of the expression is not a motion over an unresolved part consumer
//		3. The expression does not have an unneeded part propagator
//
//---------------------------------------------------------------------------
BOOL
CEngine::FValidCTEAndPartitionProperties(CMemoryPool *mp,
										 CExpressionHandle &exprhdl,
										 CReqdPropPlan *prpp)
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	CPartIndexMap *ppimDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppim();

	return popPhysical->FProvidesReqdCTEs(exprhdl, prpp->Pcter()) &&
		   !CUtils::FMotionOverUnresolvedPartConsumers(
			   mp, exprhdl, prpp->Pepp()->PppsRequired()->Ppim()) &&
		   !ppimDrvd->FContainsRedundantPartitionSelectors(
			   prpp->Pepp()->PppsRequired()->Ppim());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FChildrenOptimized
//
//	@doc:
//		Check if all children were successfully optimized
//
//---------------------------------------------------------------------------
BOOL
CEngine::FChildrenOptimized(COptimizationContextArray *pdrgpoc)
{
	GPOS_ASSERT(NULL != pdrgpoc);

	const ULONG length = pdrgpoc->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		if (NULL == (*pdrgpoc)[ul]->PgexprBest())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimize
//
//	@doc:
//		Check if optimization is possible under the given property enforcing
//		types
//
//---------------------------------------------------------------------------
BOOL
CEngine::FOptimize(CEnfdProp::EPropEnforcingType epetOrder,
				   CEnfdProp::EPropEnforcingType epetDistribution,
				   CEnfdProp::EPropEnforcingType epetRewindability,
				   CEnfdProp::EPropEnforcingType epetPropagation)
{
	return CEnfdProp::FOptimize(epetOrder) &&
		   CEnfdProp::FOptimize(epetDistribution) &&
		   CEnfdProp::FOptimize(epetRewindability) &&
		   CEnfdProp::FOptimize(epetPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FProhibited
//
//	@doc:
//		Check if any of the given property enforcing types prohibits enforcement
//
//---------------------------------------------------------------------------
BOOL
CEngine::FProhibited(CEnfdProp::EPropEnforcingType epetOrder,
					 CEnfdProp::EPropEnforcingType epetDistribution,
					 CEnfdProp::EPropEnforcingType epetRewindability,
					 CEnfdProp::EPropEnforcingType epetPropagation)
{
	return (CEnfdProp::EpetProhibited == epetOrder ||
			CEnfdProp::EpetProhibited == epetDistribution ||
			CEnfdProp::EpetProhibited == epetRewindability ||
			CEnfdProp::EpetProhibited == epetPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdPartPropagation
//
//	@doc:
//		Check if partition propagation resolver is passed an empty part propagation
// 		spec
//
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckReqdPartPropagation(CPhysical *pop,
								   CEnfdPartitionPropagation *pepp)
{
	BOOL fPartPropagationReqd =
		(NULL != pepp &&
		 pepp->PppsRequired()->Ppim()->FContainsUnresolvedZeroPropagators());

	return fPartPropagationReqd ||
		   COperator::EopPhysicalPartitionSelector != pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdProps
//
//	@doc:
//		Determine if checking required properties is needed.
//		This method is called after a group expression optimization job has
//		started executing and can be used to cancel the job early.
//
//		This is useful to prevent deadlocks when an enforcer optimizes same
//		group with the same optimization context. Also, in case the subtree
//		doesn't provide the required columns we can save optimization time by
//		skipping this optimization request.
//
//		NB: Only relational properties are available at this stage to make this
//		decision.
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckReqdProps(CExpressionHandle &exprhdl, CReqdPropPlan *prpp,
						 ULONG ulOptReq)
{
	GPOS_CHECK_ABORT;

	if (GPOS_FTRACE(EopttracePrintMemoEnforcement))
	{
		CAutoTrace at(m_mp);
		at.Os() << "CEngine::FCheckReqdProps (Group ID: "
				<< exprhdl.Pgexpr()->Pgroup()->Id()
				<< " Expression ID: " << exprhdl.Pgexpr()->Id() << ")"
				<< std::endl;
		m_pmemo->OsPrint(at.Os());
	}

	// check if operator provides required columns
	if (!prpp->FProvidesReqdCols(m_mp, exprhdl, ulOptReq))
	{
		return false;
	}

	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	COperator::EOperatorId op_id = popPhysical->Eopid();

	// check if sort operator is passed an empty order spec;
	// this check is required to avoid self-deadlocks, i.e.
	// sort optimizing same group with the same optimization context;
	BOOL fOrderReqd = !prpp->Peo()->PosRequired()->IsEmpty();
	if (!fOrderReqd && COperator::EopPhysicalSort == op_id)
	{
		return false;
	}

	// check if motion operator is passed an ANY distribution spec;
	// this check is required to avoid self-deadlocks, i.e.
	// motion optimizing same group with the same optimization context;
	BOOL fDistributionReqd =
		(CDistributionSpec::EdtAny != prpp->Ped()->PdsRequired()->Edt());
	if (!fDistributionReqd && CUtils::FPhysicalMotion(popPhysical))
	{
		return false;
	}

	// check if spool operator is passed a non-rewindable spec;
	// this check is required to avoid self-deadlocks, i.e.
	// spool optimizing same group with the same optimization context;
	if (!prpp->Per()->PrsRequired()->IsCheckRequired() &&
		COperator::EopPhysicalSpool == op_id)
	{
		return false;
	}

	return FCheckReqdPartPropagation(popPhysical, prpp->Pepp());
}

UlongPtrArray *
CEngine::GetNumberOfBindings()
{
	return m_pdrgpulpXformBindings;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintRoot
//
//	@doc:
//		Print root group
//
//---------------------------------------------------------------------------
void
CEngine::PrintRoot()
{
	CAutoTrace at(m_mp);
	at.Os() << "Root Group:" << std::endl;
	m_pmemo->Pgroup(m_pmemo->PgroupRoot()->Id())->OsPrint(at.Os());
	at.Os() << std::endl;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintOptCtxts
//
//	@doc:
//		Print main optimization context and optimal cost context
//
//---------------------------------------------------------------------------
void
CEngine::PrintOptCtxts()
{
	CAutoTrace at(m_mp);
	COptimizationContext *poc = m_pmemo->PgroupRoot()->PocLookupBest(
		m_mp, m_search_stage_array->Size(), m_pqc->Prpp());
	GPOS_ASSERT(NULL != poc);

	at.Os() << std::endl << "Main Opt Ctxt:" << std::endl;
	(void) poc->OsPrintWithPrefix(at.Os(), " ");
	at.Os() << std::endl;
	at.Os() << "Optimal Cost Ctxt:" << std::endl;
	(void) poc->PccBest()->OsPrint(at.Os());
	at.Os() << std::endl;
}
#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CEngine::OsPrint
//
//	@doc:
//		print function
//
//---------------------------------------------------------------------------
IOstream &
CEngine::OsPrint(IOstream &os) const
{
	return m_pmemo->OsPrint(os);
}

//************************ARENA**************************/
void readConfig()
{
	// 注意文件应以 variable value 的形式，每行一个变量
	std::string config_file_name = "/tmp/";
	config_file_name += ARENA_file_name;
	config_file_name += "_config";
	std::ifstream fin(config_file_name);
	std::string buf;
	std::stringstream ss;
	std::string name;
	if (fin.is_open())
	{
		while(std::getline(fin, buf))
		{
			ss.clear();
			ss << buf;
			ss >> name;
			if (name == "s_weight")
				ss >> s_weight;
			else if (name == "c_weight")
				ss >> c_weight;
			else if (name == "cost_weight")
				ss >> cost_weight;
			else if (name == "lambda")
				ss >> lambda;
			else if (name == "ARENA_k")
				ss >> ARENA_k;
			else if (name == "treeEdit_flag")
			{
				int a;  // true 或 false 通过数字表示，0 代表 false
				ss >> a;
				treeEdit_flag = a;
			}
			else if (name == "bow_flag")
			{
				int a;  // true 或 false 通过数字表示，0 代表 false
				ss >> a;
				bow_flag = a;
			}
			ss.str("");
		}
		fin.close();
	}
	// remove(config_file_name.c_str());
}


// 计算最终结果的MaxMin值，用于 debug
double calculateMaxMin(std::vector<int> & res)
{
	double dist = DBL_MAX;
	double temp = 0.0;
	for(std::size_t i=0;i<res.size();++i)
	{
		for(std::size_t j=i+1;j<res.size();++j)
		{
			temp = plan_trees[res[i]].distance(plan_trees[res[j]]);
			if (temp < dist)
				dist = temp;
		}
	}
	return dist;
}


void ARENA_result(std::vector<int> & res)
{
	std::string filename = "/tmp/";
	std::ofstream fout(filename+ARENA_file_name);
	if(fout.is_open())
	{
		fout << calculateMaxMin(res) << std::endl;
		fout << "[";
		nlohmann::json j;
		for (std::size_t i=0;i<res.size();++i)
		{
			j["id"] = i+1;
			j["cost"] = plan_trees[res[i]].root->data.cost;

			if (i == 0)  // best plan
			{
				j["s_dist"] = 0;
				j["c_dist"] = 0;
				j["cost_dist"] = 0;
				j["relevance"] = 0;
			}
			else
			{
				j["s_dist"] = plan_trees[res[i]].structure_dist(plan_trees[0]);
				j["c_dist"] = plan_trees[res[i]].content_dist(plan_trees[0]);
				j["cost_dist"] = plan_trees[res[i]].cost_dist(plan_trees[0]);
				j["relevance"] = plan_trees[res[i]].offset;
			}

			j["content"] = plan_trees[res[i]].root->generate_json();
			fout << j;
			fout << ",";
		}
		fout.seekp(-1, std::ios::cur);
		fout << "]";
		fout.close();
	}
	// 重置全局变量
	std::vector<PlanTree<CExpression>> ().swap(plan_trees);
	find_index.clear();
	// plan_trees.clear();
	new_add = 0;
	max_cost = 0.0;
	treeEdit_flag = false;
	bow_flag = false;
	ARENA_k = 10;
}


void DealWithPlan()
{
	if (ARENA_k == 0)  // 只选择 QEP
	{
		// std::vector<int> res;
		// res.push_back(0);
		plan_trees.push_back(PlanTree<CExpression>());
		plan_trees[0].init(plan_buffer.front());
		sendResult(0);  // 发送这个数据
		// ARENA_result(res);
		std::queue<gpopt::CExpression*> ().swap(plan_buffer);
	}
	else
	{
		plan_trees.reserve(plan_buffer.size());
		std::size_t i=0;
        std::ofstream fout_time("/home/wang/timeRecord.txt");
        if (fout_time.is_open())
        {
			fout_time << "计划数量为: " << plan_buffer.size() << std::endl;
            auto start = std::chrono::steady_clock::now();
			auto startAll = start;
            while(!plan_buffer.empty())
            {
                plan_trees.push_back(PlanTree<CExpression>());
                plan_trees[i].init(plan_buffer.front(), 0);
                plan_buffer.front()->Release();
                plan_buffer.pop();

                // 设置 cost 的最大值
                if (plan_trees[i].root->data.cost > max_cost)
                {
                    max_cost = plan_trees[i].root->data.cost;
                }

                ++i;
            }
            fout_time << "第一阶段初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			///// 测试不同初始化的时间
			for(std::size_t i=0;i<plan_trees.size();i++)  // init1
			{
				plan_trees[i].init(NULL, 1);
			}
            fout_time << "    生成字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			for(std::size_t i=0;i<plan_trees.size();i++)  // init3
			{
				plan_trees[i].init(NULL, 3);
			}
            fout_time << "    初始化上下文的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();


			for(std::size_t i=0;i<plan_trees.size();i++)  // init4
			{
				plan_trees[i].init(NULL, 4);
			}
            fout_time << "    构造后缀树的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			for(std::size_t i=0;i<plan_trees.size();i++)  // init5
			{
				plan_trees[i].init(NULL, 5);
			}
            fout_time << "    收尾工作1(计算节点数量)的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			for(std::size_t i=0;i<plan_trees.size();i++)  // init5
			{
				plan_trees[i].init(NULL, 6);
			}
			fout_time << "    收尾工作2(计算与自己的距离)的时间为(ms): "
					  << (std::chrono::duration_cast<std::chrono::milliseconds>(
							  std::chrono::steady_clock::now() - start))
							 .count()
					  << std::endl;
			start = std::chrono::steady_clock::now();

			for(std::size_t i=0;i<plan_trees.size();i++)  // init5
			{
				plan_trees[i].init(NULL, 7);
			}
			fout_time << "    剪枝所需的时间为(ms): "
					  << (std::chrono::duration_cast<std::chrono::milliseconds>(
							  std::chrono::steady_clock::now() - start))
							 .count()
					  << std::endl;
			start = std::chrono::steady_clock::now();

			for (std::size_t i = 0; i < plan_trees.size(); i++)
			{
				plan_trees[i].init(NULL, 2);
			}
            fout_time << "第三阶段初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

			///// 测试不同初始化的时间

            fout_time << "初始每个计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

            max_cost -= plan_trees[0].root->data.cost;
            // max_cost = log(max_cost) - log(plan_trees[0].root->data.cost);

            // 计算每个 plan 与 best plan 之间的偏差，并且设置 dist_record
            // 只有当要选出的数量比较少时才进行计算，否则全部选择
            std::priority_queue<MinDist> dist_record;
            // if (ARENA_k < plan_trees.size())
            if (true)  // 如果不执行这个代码块，就不会计算 relevance
            {
				double temp_dist;
				suffixLinkFlag = true;
				fout_dist.open("/home/wang/distRecord.txt");
				if (fout_dist.is_open()){   // 统计距离的相关信息
					fout_dist << "id\ts_dist\tc_dist\tcost_dist\treference\n";
					foutDistFlag = true;
				} else {
					fout_time << "fout_dist 打开失败\n";
				}

				for (std::size_t i = 1; i < plan_trees.size(); ++i)
				{
					if (foutDistFlag)
						fout_dist << i << "\t";
					temp_dist = plan_trees[i].distance_with_best(plan_trees[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}

				if (foutDistFlag){
					fout_dist.close();
					foutDistFlag = false;
				}

				fout_time << "后缀链的平均使用次数为: " << suffixLinkUsage / plan_trees.size() << std::endl;
				suffixLinkUsage = 0;
				suffixLinkFlag = false;
			}
            fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

            std::vector<int> res;
            FindK(plan_trees, res, ARENA_k, dist_record);
            fout_time << "寻找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            ARENA_result(res);
            fout_time << "程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;
            fout_time.close();
        }
	}

	// 重置全局变量
	std::vector<PlanTree<CExpression>> ().swap(plan_trees);
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
	treeEdit_flag = false;
	bow_flag = false;
	ARENA_k = 10;


}
//************************ARENA**************************/

// EOF
