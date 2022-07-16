// ---------------------------------------------------------------------------
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
//	3、当前 CEngine.cpp 用于进行时间的实验
//************************ARENA**************************/
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/Edge.h"
#include "gpopt/engine/Sender.h"
#include "gpopt/engine/json.hpp"
#include "gpopt/engine/JSON.h"
#include "gpos/io/ARENAstream.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalIndexOnlyScan.h"
#include "ARENA_global.h"
#include <curl/curl.h>
#include <thread>
#include <mutex>
#include <queue>
#include <utility>
#include <chrono>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

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
#include <iomanip>


//************************ARENA**************************/
#define ARENA_DEBUG
#define ARENA_PHYSICAL

// 对序列化后的树进行排序，并返回排序后的新树
std::string ARENASortSTree(const std::string & s)
{
	std::string res;
	if(s.size() < 2)
	{
		res = s;
		return res;
	}

	res += '[';
	std::vector<std::string> sub_tree;
	int left = 0, right = 0;
	std::size_t pre = 1;
	for(std::size_t now=1;now < s.size() -1 ;now++)
	{
		switch(s[now])
		{
			case '[':
				left++;
				break;
			case ']':
				right++;
				break;
		}
		if (left == right && left != 0)
		{
			std::string tempStr = s.substr(pre, now - pre + 1);
			sub_tree.push_back(ARENASortSTree(tempStr));
			left = 0;
			right = 0;
			pre = now + 1;
		}
	}
	std::sort(sub_tree.begin(), sub_tree.end());
	for(auto subS: sub_tree)
	{
		res += subS;
	}
	res += ']';
	return res;
}


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


// 否则将 CTreeNode 中统计的 ARENA_groupTreePlus 的信息转变为
// 记录所有子树的 hash table，用于之后进一步计算两棵树之间的结构距离
struct gtTree
{
	struct gtSingleChar
	{
		char inC;
		std::string inChild;

		gtSingleChar(char c): inC(c) {};
	};

    typedef std::unordered_map<std::string, std::vector<int>>::iterator pairIter;

    pairIter inIter;  // 指向 CTreeNode 中某个特定元素的指针
    std::unordered_map<std::string, int> inSubtreeInfo;
	double inSelfKernel;

    gtTree(pairIter iter)
    {
        inIter = iter;
		inSelfKernel = 0.0;
    }

    // 初始化，生成某棵 GroupTree 的 hash 表
    void init()
    {
        std::vector<gtSingleChar> stack;
		std::string sortedStr = ARENASortSTree(inIter->first);
        for(char c: sortedStr)
        {
            switch(c)
            {
                case '[':  // 直接加入栈中
                    stack.emplace_back(gtSingleChar(c));
                    break;
                case ']':
                    std::string tempStr{'['};
                    tempStr += stack.back().inChild;
                    tempStr += ']';
                    stack.pop_back();

                    // 记录该子串
                    inSubtreeInfo[tempStr]++;
                    // 将该子串添加到前一个字符的孩子上
                    if(!stack.empty())
                    {
                        stack.back().inChild += tempStr;
                    }
                    break;
            }
        }

		// 计算与自己的树核
		std::unordered_map<std::string, int> * small;
		small = &inSubtreeInfo;

		// 找到两棵树的公共子树，并计算相应的乘积
		for (auto iter=small->begin(); iter != small->end() ; iter++){
			inSelfKernel += iter->second * iter->second;
		}

    }

    friend std::ostream& operator<<(std::ostream& fout, gtTree& tree)
    {
        for(auto & pair: tree.inSubtreeInfo)
        {
            fout << pair.first << " : " << pair.second << '\n';
        }
        return fout;
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
std::ofstream fout_dist;  // 用于统计每个计划与最有计划之间的各种距离

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
void FindKRandom();
void FindKCost();
void ARENATimeExp1();  // 测试不同结构计算方法对性能的影响 （这个实验暂时没用）
void ARENATimeExp2();  // 测试添加剪枝条件时对性能的影响 （这个实验暂时也没用）
void ARENATimeExp3();  // 测试不同选择方法的效果和效率对比
void ARENATimeExp3Random();  // 测试不同选择方法的效果和效率对比
void ARENATimeExp3Cost();  // 测试不同选择方法的效果和效率对比
void ARENATimeExp4();  // 测试使用优化过的后缀树构造方法所需的时间
void ARENATimeExp4Hash();  // 测试使用 Hash Table 计算树核时花费的时间
void ARENATimeExp4Old();  // 测试使用原始的后缀树构造方法所需的时间
void ARENAGTExp();  // 测试使用 Group Tree 进行过滤的效果
void ARENAOutputExp(std::size_t , std::ofstream &);  // 用于将结果输出到文件中
void ARENAAosExp();  // 测试 AOS 策略的实验
void ARENAFindTargetType();  // 用于寻找特定类型的 plan
#define ARENA_GTEXP
void readConfig(char mode='B');
void ARENAGetIndexInfo(CExpression * expression, IOstream &os);


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
std::string gConf;  // 暂存 config 信息的字符串
std::string gResFile;  // 最终结果的文件名
double gSWeight = 0.33;  // 结构的差距所占的比重
double gCWeight = 0.33;  // 内容的差异所占的比重
double gCostWeight = 0.33;  // cost 的差异所占的比重
double gLambda = 0.5;  // diff 所占的比重
char gMode = 'B';  // ARENA 以哪种模式运行
std::size_t gARENAK = 5;  // 默认选出 5 个 alternative plans
Sender* web_client = nullptr;  // 与 web 服务器进行通信的客户端
bool gIsGTFilter = false;  // 是否使用 Group Tree 对 plan 进行过滤，默认如果 plan 的数量大于 10 万就进行过滤，不能有用户来决定，因为用户并不知道共有多少个 plan
int gGTNumThreshold = 50000;
double gGTFilterThreshold = 0.5;  // 与 QEP 的结构差异大于该阈值将会被过滤掉
ULLONG gSampleThreshold = 10;
bool gisSample = false;
std::size_t gAosStart = 0;  // 记录从哪个索引开始就是 Aos 策略添加的 plan ，只在实验中有用
int gMethod = 0;  // 采用哪种方法选择 plan  0->正常的方法 1->cost_based 2->random
ULLONG gJoin = 0; // the number of join
bool gTEDFlag = false;  // 指示是否使用 tree edit distance

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


// 普通版本的编辑距离，不进行归一化
int editDistG(std::vector<std::string*> word1, std::vector<std::string*> word2)
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
	return D[n][m];
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

	// 在剪枝的过程中，更新某条边的终点位置
	void updateEdge(int startNode, char c, int endIndex) {
		long key = returnHashKey(startNode, c);
		std::unordered_map<long, Edge>::iterator search = edgeHash.find(key);
		search->second.endLabelIndex = endIndex;
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
			// 遍历其中的每个字符，直到符合树结构（每一条边上可能有多个字符）
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
					context.updateEdge(e.startNode, context.s[e.startLabelIndex], i);
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
				context.updateEdge(e.startNode, context.s[e.startLabelIndex], i);
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
	std::string tag;  // 该节点的类型是什么，当前记录的类型包括（TABLE 表, SCAN 扫描操作符, JOIN 连接操作符）
	std::string info;  // 该节点的额外信息，当前只有 index 类型才会展示额外信息，包括（index name，过滤条件）
	double cost;
	NodeData(double cost_in = 0.0, std::string name_in = "", std::string tag_in = "", std::string info_in = "")
	{
		name = name_in;
		cost = cost_in;
		tag = tag_in;
		info = info_in;
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

	// 去除最后一个孩子节点
	void pop_back()
	{
		if (child.size() > 0)
		{
			child.pop_back();
		}
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
		res["tag"] = data.tag;
		res["info"] = data.info;
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


//--------------------------------------------------
// @function:
//		serialize
//
// @doc:
// 		对树进行序列化，并返回序列化之后的字符串。当前采用的方法是先序遍历序列化
//--------------------------------------------------
std::string serialize(PlanTreeNode * root){
	if(root == nullptr){
		return "[]";
	}

	std::string res = '[' + root->data.name;
	for(size_t i=0;i<root->size();i++){
		res += ',' + serialize(root->child[i]);
	}
	res += ']';
	return res;
}


template <class T>
struct PlanTree
{
	using json = nlohmann::json;

	// 与 best_plan 的结构和cost的偏离程度。也可以理解为某个 plan 的价值。当前我们认为 cost 差异越大，结构和内容差异越小，价值越大
	double offset;

	PlanTreeNode *root;
	std::string str_expression;  // 用于计算后缀树
	bool init_flag;
	suffixTreeContext context;
	std::vector<std::string*> str_of_node;  // 节点内容构成的字符串，用于度量内容之间的差异
	// 这两个属性均与 treeEditDist 有关，使用其它方法时不需考虑
	std::string json_str;
	int node_num;
	double cost_log;
	double s_dist_best, c_dist_best, cost_dist_best;  // 与 best_plan 之间的各种距离

	suffixTree testTree;
	std::unordered_map<std::string, int> subTree;  // 记录所有的子树以及各个子树出现的个数，用于计算结构距离


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

	// 返回当前树的总 cost ，即根节点的 cost
	double get_cost(){
		return root->data.cost;
	}

	void insert_child(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

			// 检查这个操作是否为 CPhysical 操作，因为这个操作，所以 PlanTree 不再适用于其它类型
#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				// 检查是否是 Index 操作，如果是，输出详细信息
				if (name_len > 5 && !strncmp(expression_name, "Index", 5)){  // IndexScan
#ifdef ARENA_DEBUG
					// std::ofstream fout_index("/tmp/Index");
					// fout_index << "存在 index 节点\n";
#endif
					gpos::ARENAIOstream stream;
					// 这一步用于输出索引名
					if (strncmp(expression_name, "IndexS", 6) == 0){
						dynamic_cast<CPhysicalIndexScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}else if (strncmp(expression_name, "IndexO", 6) == 0){
						dynamic_cast<CPhysicalIndexOnlyScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}
					stream << '\n';

					// 这一步用于取得索引的过滤条件
					if (child_expression->Arity() == 1){  // 恰好只有一个孩子
						ARENAGetIndexInfo((*child_expression)[0], stream);
					} else {  // 有多个孩子或者没有孩子
						stream << "子表达式的个数与预期不符，子表达式的个数为：" << child_expression->Arity();
					}
					cur->data.info = stream.ss.str();

#ifdef ARENA_DEBUG
					// if (fout_index.is_open()){
					// 	fout_index << expression_name << '\n';
					// 	fout_index << stream.ss.str();
					// 	fout_index.close();
					// }
#endif
				}

				// 需要检查名字是否是 Scan 类型，如果是，需要获得 relation 的名字

				// 字符串长度大于4，并且最后四个字母是 Scan，需要取得表名
				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";  // 节点的标签为: 扫描操作符
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
						PlanTreeNode * table = new PlanTreeNode(NodeData(0.0, temp.substr(start, end-start), "TABLE"));
						cur->push_back(table);
					}
				} else if(name_len > 4 && !strcmp(expression_name + name_len -4, "Join"))  // Join 操作符，设置 JOIN 标签
				{
					cur->data.tag = "JOIN";
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child((*child_expression)[i], *cur);
				}

				// 检查 如果是 Motion 操作节点，并且节点下只有一个子节点，并且该节点的 cost 和 子节点的 cost 相同，则去除该节点
				const char * motionOp = "Motion";
				if (cur->data.name.size() > 6 && cur->data.name.compare(0, 6, motionOp) == 0 && cur->size() == 1 && 
					cur->data.cost - (*cur)[0]->data.cost < 0.1 * cur->data.cost )  // 该节点与子节点的 cost 差异很小
				{
					// 将当前节点从父节点中去除并将子节点移入到父节点的孩子中
					parent.child[parent.size()-1] = (*cur)[0];
					// 释放当前节点的内存
					cur->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
					delete cur;
				}
#ifdef ARENA_PHYSICAL
			}
#endif
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
		str_expression = root->get_string();
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
		std::queue<PlanTreeNode*> node_queue;  // 采用层序遍历的方式来生成节点的内容
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
			// 如果节点是 表扫描操作 或者是 连接操作，设置 node 的 tag
			if (root->data.name.size() > 4)
			{
				std::size_t compareStart = root->data.name.size() - 4;
				if(root->data.name.compare(compareStart, 4, "Scan") == 0)
				{
					root->data.tag = "SCAN";
				} else if (root->data.name.compare(compareStart, 4, "Join") == 0)
				{
					root->data.tag = "JOIN";
				}
			}

			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}

			// 检测节点是否是 Motion 节点，如果是，删除
			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )  // 该节点与子节点的 cost 差异很小
			{
				// 临时存储子节点的地址
				auto temp = (*root)[0];
				// 释放当前节点的内存
				root->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
				delete root;
				root = temp;
			}
		}
		// cost_log = log(root->data.cost);  // 用于计算归一化的 cost 距离
		init_flag = true;
		generateSuffixTree();
		getNodeStr();
	}

	void init(T* plan, int flag)  // 与上述 init 函数的功能相同，但是该函数中将各个部分进行了拆分，用于测试不同初始化的时间
	{
		if (flag == 0 && NULL != plan)
		{
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			// 如果节点是 表扫描操作 或者是 连接操作，设置 node 的 tag
			if (root->data.name.size() > 4)
			{
				std::size_t compareStart = root->data.name.size() - 4;
				if(root->data.name.compare(compareStart, 4, "Scan") == 0)
				{
					root->data.tag = "SCAN";
				} else if (root->data.name.compare(compareStart, 4, "Join") == 0)
				{
					root->data.tag = "JOIN";
				}
			}

			// 添加子节点
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}

			// 检测节点是否是 Motion 节点，如果是，删除
			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )  // 该节点与子节点的 cost 差异很小
			{
				// 临时存储子节点的地址
				auto temp = (*root)[0];
				// 释放当前节点的内存
				root->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
				delete root;
				root = temp;
			}
		}

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
		else if (flag == 7) { // 对后缀树进行剪枝，原方法不需要剪枝
			purnTree(testTree);
			getSubTree(context, 0, std::string{});
		}
	}

private:
	// 用于利用剪枝后的后缀树生成子树的统计信息
	void getSubTree(suffixTreeContext& context_var, int node, std::string s){
		Edge lchild = context_var.findEdge(node, '[');
		Edge rchild = context_var.findEdge(node, ']');

		// 叶子节点，记录并返回
		if (!lchild.valid() && !rchild.valid()) {
			if (s.back() == '$') {  // 在剪枝的过程中，可能有些节点保留了最后的哨兵字符 '$', 而另外一些没有保留，这里统一删去该字符
				s.pop_back();
			}

			subTree[s] = context_var.nodeArray[node].lvs;  // 每个节点叶子节点的数量，代表该节点出现的数量
			return;
		}

		// 不是叶子节点，需要对两个子节点分别进行处理
		if (lchild.valid()) {
			std::string temp = s;  // 生成临时字符串，方式对原字符串的修改
			for (auto i=lchild.startLabelIndex; i<=lchild.endLabelIndex; i++) {  // 将该边上的子串加到遍历过的字符串中
				temp += context_var.s[i];
			}
			getSubTree(context_var, lchild.endNode, temp);
		}

		if (rchild.valid()) {
			std::string temp = s;
			for (auto i=rchild.startLabelIndex; i<=rchild.endLabelIndex; i++) {
				temp += context_var.s[i];
			}
			getSubTree(context_var, rchild.endNode, temp);
		}

		return;
	}

public:
	//****************** 距离相关的一些函数 ********************************/
	// 计算两棵树之间的距离
	double distance(PlanTree& other)
	{
		// double s_dist = context.distance(other.context);  // 结构的距离
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
		double cost = cost_dist(other);
		return (1-gLambda)*(offset + other.offset) / 2 +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	double distance_with_best(PlanTree &other)
	{
		// double s_dist = context.distance(other.context);  // 结构的距离
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
		double cost = cost_dist(other);

		s_dist_best = s_dist;
		c_dist_best = c_dist;
		cost_dist_best = cost;
		// offset = abs(0.5*(s_dist+c_dist) - cost);  // 原本的价值评定方式：cost 和 结构以及内容的偏离程度
		offset = cost - (s_dist+c_dist);  // 新的价值评定：cost 与 (s_dist+c_dist) 的差值，值越大，价值越高

		return (1-gLambda)*offset / 2 +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	// 返回两者之间结构的差距
	// 当前使用的方法是将子树转换为字符串，寻找相同字符串的个数
	double structure_dist(PlanTree& other)
	{
		int res = 0;
		
		// 找到比较小的记录
		std::unordered_map<std::string, int> * small, *large;
		if (subTree.size() < other.subTree.size()){
			small = &subTree;
			large = &(other.subTree);
		} else {
			small = &(other.subTree);
			large = &subTree;
		}

		// 找到两棵树的公共子树，并计算相应的乘积
		for (auto iter=small->begin(); iter != small->end() ; iter++){
			const std::string & key = iter->first;
			auto iter2 = large->find(key);
			if (iter2 != large->end()) {  // 存在公共子串
				res += iter->second * iter2->second;
			}
		}

		// 计算结构距离 : 
		double temp  = sqrt(context.self_kernel * other.context.self_kernel);
		temp = (double)(res) / temp;
		return sqrt(1.0 - temp);
	}

	// 返回两者之间内容的差距
	double content_dist(PlanTree& other)
	{
		return editDist(str_of_node, other.str_of_node);
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


template <class T>
struct PlanTreeHash  // 利用 hash 表计算结构距离的 PlanTree
{
	using json = nlohmann::json;

	// 与 best_plan 的结构和cost的偏离程度。也可以理解为某个 plan 的价值。当前我们认为 cost 差异越大，结构和内容差异越小，价值越大
	double offset;

	PlanTreeNode *root;
	bool init_flag;
	std::vector<std::string*> str_of_node;  // 节点内容构成的字符串，用于度量内容之间的差异
	// 这两个属性均与 treeEditDist 有关，使用其它方法时不需考虑
	std::string json_str;
	std::string str_serialize;
	int node_num;
	double cost_log;
	double s_dist_best, c_dist_best, cost_dist_best;  // 与 best_plan 之间的各种距离
	double self_kernel;  // 用于对结构距离进行归一化
	T * inOriginal;  // 指向原对象的指针

	std::unordered_map<std::string, int> subTree;  // 记录所有的子树以及各个子树出现的个数，用于计算结构距离


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

	// 返回当前树的总 cost ，即根节点的 cost
	double get_cost(){
		return root->data.cost;
	}

	// 插入子节点，该函数相比于下面的 insert_child_detaild 函数，对每个节点增加了更多的判断
	// 同时也向每个节点中插入了更多的信息，便于前端进行展示
	void insert_child_detailed(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

			// 检查这个操作是否为 CPhysical 操作，因为这个操作，所以 PlanTree 不再适用于其它类型
#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				// 检查是否是 Index 操作，如果是，输出详细信息
				if (name_len > 5 && !strncmp(expression_name, "Index", 5)){  // IndexScan
#ifdef ARENA_DEBUG
					// std::ofstream fout_index("/tmp/Index");
					// fout_index << "存在 index 节点\n";
#endif
					gpos::ARENAIOstream stream;
					// 这一步用于输出索引名
					if (strncmp(expression_name, "IndexS", 6) == 0){
						dynamic_cast<CPhysicalIndexScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}else if (strncmp(expression_name, "IndexO", 6) == 0){
						dynamic_cast<CPhysicalIndexOnlyScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}
					stream << '\n';

					// 这一步用于取得索引的过滤条件
					if (child_expression->Arity() == 1){  // 恰好只有一个孩子
						ARENAGetIndexInfo((*child_expression)[0], stream);
					} else {  // 有多个孩子或者没有孩子
						stream << "子表达式的个数与预期不符，子表达式的个数为：" << child_expression->Arity();
					}
					cur->data.info = stream.ss.str();

#ifdef ARENA_DEBUG
					// if (fout_index.is_open()){
					// 	fout_index << expression_name << '\n';
					// 	fout_index << stream.ss.str();
					// 	fout_index.close();
					// }
#endif
				}

				// 需要检查名字是否是 Scan 类型，如果是，需要获得 relation 的名字

				// 字符串长度大于4，并且最后四个字母是 Scan，需要取得表名
				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";  // 节点的标签为: 扫描操作符
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
						PlanTreeNode * table = new PlanTreeNode(NodeData(0.0, temp.substr(start, end-start), "TABLE"));
						cur->push_back(table);
					}
				} else if(name_len > 4 && !strcmp(expression_name + name_len -4, "Join"))  // Join 操作符，设置 JOIN 标签
				{
					cur->data.tag = "JOIN";
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child_detailed((*child_expression)[i], *cur);
				}

				// 检查 如果是 Motion 操作节点，并且节点下只有一个子节点，并且该节点的 cost 和 子节点的 cost 相同，则去除该节点
				const char * motionOp = "Motion";
				if (cur->data.name.size() > 6 && cur->data.name.compare(0, 6, motionOp) == 0 && cur->size() == 1 && 
					cur->data.cost - (*cur)[0]->data.cost < 0.1 * cur->data.cost )  // 该节点与子节点的 cost 差异很小
				{
					// 将当前节点从父节点中去除并将子节点移入到父节点的孩子中
					parent.child[parent.size()-1] = (*cur)[0];
					// 释放当前节点的内存
					cur->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
					delete cur;
				}
#ifdef ARENA_PHYSICAL
			}
#endif
		}
	}

	void insert_child(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

			// 检查这个操作是否为 CPhysical 操作，因为这个操作，所以 PlanTree 不再适用于其它类型
#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				// 需要检查名字是否是 Scan 类型，如果是，需要获得 relation 的名字
				// 字符串长度大于4，并且最后四个字母是 Scan，需要取得表名
				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";  // 节点的标签为: 扫描操作符
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
						PlanTreeNode * table = new PlanTreeNode(NodeData(0.0, temp.substr(start, end-start), "TABLE"));
						cur->push_back(table);
					}
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child((*child_expression)[i], *cur);
				}
#ifdef ARENA_PHYSICAL
			}
#endif
		}
	}

	PlanTreeHash()
	{
		offset = 0.0;
		root = NULL;
		init_flag = false;
	}

	~PlanTreeHash()
	{
		if (NULL != root)
			delete root;
	}

	// 用于生成由节点内容组成的字符串
	// 用于计算编辑距离
	void getNodeStr()
	{
		std::queue<PlanTreeNode*> node_queue;  // 采用层序遍历的方式来生成节点的内容
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

	// 该函数相比于下面两个函数，它会在根节点进行更多的判断，用于最终的结果展示
	void init_detailed(T* plan)
	{
		if (NULL != plan)
		{
			inOriginal = plan;  // 记录该指针，便于后面调用 init_detailed
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			// 如果节点是 表扫描操作 或者是 连接操作，设置 node 的 tag
			if (root->data.name.size() > 4)
			{
				std::size_t compareStart = root->data.name.size() - 4;
				if(root->data.name.compare(compareStart, 4, "Scan") == 0)
				{
					root->data.tag = "SCAN";
				} else if (root->data.name.compare(compareStart, 4, "Join") == 0)
				{
					root->data.tag = "JOIN";
				}
			}

			// 添加子节点
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child_detailed((*plan)[i], *root);
			}

			// 检测节点是否是 Motion 节点，如果是，删除
			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )  // 该节点与子节点的 cost 差异很小
			{
				// 临时存储子节点的地址
				auto temp = (*root)[0];
				// 释放当前节点的内存
				root->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
				delete root;
				root = temp;
			}
		}
	}

	void init(T * plan)
	{
		init(plan, 0);
		init(nullptr, 1);
		init(nullptr, 2);
		init(nullptr, 3);
	}

	void init(T* plan, int flag)  // 与上述 init 函数的功能相同，但是该函数中将各个部分进行了拆分，用于测试不同初始化的时间
	{
		if (flag == 0 && NULL != plan)
		{
			inOriginal = plan;  // 记录该指针，便于后面调用 init_detailed
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			// 添加子节点
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}
		}

		init_flag = true;
		if (flag == 1)
		{
			// 生成子树统计信息
			str_serialize = postOrder(root);
		}
		else if (flag == 2)
		{
			// 将树转变为字符串，用于计算内容差异
			getNodeStr();
		}
		else if (flag == 3)
		{
			// 计算自己与自己的树核信息
			structure_dist_self();

			if(gTEDFlag)  // 如果需要使用 TreeEdit 距离
			{
				TED_json();
			}
		}
		else if (flag == 4)
		{
			// 计算在利用后缀树时，单单生成序列化的树所需要的时间
			str_serialize = postOrderNone(root);
		}
		else if (flag == 5)
		{
			// 计算遍历所有字符所需要的时间
			std::unordered_map<int, int> temp;
			for(std::size_t i=0;i<str_serialize.size();i++)
			{
				temp[i]++;
			}
		}
	}

	std::string postOrder(PlanTreeNode* root)
	{
		if (root == nullptr) {
			return "";
		}

		std::string treeStr;
		if (root->size() == 0) {  // 叶子节点
			treeStr = "[]";
		}
		else
		{
			treeStr = '[';
			std::vector<std::string> temp;
			for (size_t i = 0; i < root->size(); i++)
			{
				temp.push_back(postOrder((*root)[i]));
			}
			sort(temp.begin(), temp.end());

			for(auto& s: temp)
			{
				treeStr += s;
			}

			treeStr += ']';
		}

		subTree[treeStr]++;
		return treeStr;
	}

	std::string postOrderNone(PlanTreeNode* root)
	{
		if (root == nullptr) {
			return "";
		}

		std::string treeStr;
		if (root->size() == 0) {  // 叶子节点
			treeStr = "[]";
		}
		else
		{
			treeStr = '[';
			for (size_t i = 0; i < root->size(); i++)
			{
				treeStr += postOrderNone((*root)[i]);
			}
			treeStr += ']';
		}

		return treeStr;
	}


public:
	//****************** 距离相关的一些函数 ********************************/
	// 计算两棵树之间的距离
	double distance(PlanTreeHash& other)
	{
		// double s_dist = context.distance(other.context);  // 结构的距离
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
		double cost = cost_dist(other);
		return (1-gLambda)*(offset + other.offset) / 2 +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	double distance_with_best(PlanTreeHash &other)
	{
		// double s_dist = context.distance(other.context);  // 结构的距离
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);  // 内容的距离
		double cost = cost_dist(other);

		s_dist_best = s_dist;
		c_dist_best = c_dist;
		cost_dist_best = cost;
		// offset = abs(0.5*(s_dist+c_dist) - cost);  // 原本的价值评定方式：cost 和 结构以及内容的偏离程度
		// offset = (cost - (s_dist+c_dist) + 1) / 2.0;  // 新的价值评定：cost 与 (s_dist+c_dist) 的差值，值越大，价值越高，+1 与 /2 是为了保证其范围是 [0, 1]
		// 利用拟合后的函数作为价值函数，如果值小于0，置为0
		// double a[7]{0.4974,   0.4232,   1.4671,   0.2306,  -1.7610,  -1.3291,   0.7043};
		double a[7]{1.0000,    1.0000,    1.0000,   -1.0000,   -2.0000,   -2.0000,    2.0000};
		// offset = a[0]*std::pow(s_dist, 3)+ a[1]*std::pow(c_dist, 3)+a[2]*std::pow(cost, 3)+a[3]*s_dist*c_dist+a[4]*s_dist*cost+a[5]*c_dist*cost+a[6]*s_dist*c_dist*cost;
		offset = a[0]*s_dist+ a[1]*c_dist+a[2]*cost+a[3]*s_dist*c_dist+a[4]*s_dist*cost+a[5]*c_dist*cost+a[6]*s_dist*c_dist*cost;
		if(offset < 0.0)
		{
			offset = 0.0;
		}

		return (1-gLambda)*offset +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	// 返回两者之间结构的差距
	// 当前使用的方法是将子树转换为字符串，寻找相同字符串的个数
	double structure_dist(PlanTreeHash& other)
	{
		int res = 0;
		
		// 找到比较小的记录
		std::unordered_map<std::string, int> * small, *large;
		if (subTree.size() < other.subTree.size()){
			small = &subTree;
			large = &(other.subTree);
		} else {
			small = &(other.subTree);
			large = &subTree;
		}

		// 找到两棵树的公共子树，并计算相应的乘积
		for (auto iter=small->begin(); iter != small->end() ; iter++){
			const std::string & key = iter->first;
			auto iter2 = large->find(key);
			if (iter2 != large->end()) {  // 存在公共子串
				res += iter->second * iter2->second;
			}
		}

		// 计算结构距离 : 
		double temp  = sqrt(self_kernel * other.self_kernel);
		temp = (double)(res) / temp;

		if(!gTEDFlag)
		{
			return sqrt(1.0 - temp);
		}
		else  // 使用 tree edit 距离
		{
			TreeEdit te;
			return te(json_str, other.json_str, node_num, other.node_num);  // s_dist 同时包含结构的差异与内容的差异
		}
	}

	// 该函数可以计算 PlanTreeHash 类型的对象与 gtTree 类型的对象
	// 之间的结构距离，主要用于根据 Group Tree 对 plan 进行剪枝
	double structure_dist(gtTree& other)
	{
		int res = 0;
		
		// 找到比较小的记录
		std::unordered_map<std::string, int> * small, *large;
		if (subTree.size() < other.inSubtreeInfo.size()){
			small = &subTree;
			large = &(other.inSubtreeInfo);
		} else {
			small = &(other.inSubtreeInfo);
			large = &subTree;
		}

		// 找到两棵树的公共子树，并计算相应的乘积
		for (auto iter=small->begin(); iter != small->end() ; iter++){
			const std::string & key = iter->first;
			auto iter2 = large->find(key);
			if (iter2 != large->end()) {  // 存在公共子串
				res += iter->second * iter2->second;
			}
		}

		// 计算结构距离 : 
		double temp  = sqrt(self_kernel * other.inSelfKernel);
		temp = (double)(res) / temp;
		return sqrt(1.0 - temp);
	}

	void structure_dist_self()
	{
		int res = 0;
		
		// // 找到比较小的记录
		std::unordered_map<std::string, int> * small;
		small = &subTree;

		// 找到两棵树的公共子树，并计算相应的乘积
		for (auto iter=small->begin(); iter != small->end() ; iter++){
			res += iter->second * iter->second;
		}

		// 比较慢的方法
		// for(auto iter=subTree.begin(); iter!= subTree.end();iter++)
		// {
		// 	const std::string & key = iter->first;
		// 	auto iter2 = subTree.find(key);
		// 	if (iter2 != subTree.end()) {  // 存在公共子串
		// 		res += iter->second * iter2->second;
		// 	}
		// }
		self_kernel = res;
	}

	// 返回两者之间内容的差距
	double content_dist(PlanTreeHash& other)
	{
		return editDist(str_of_node, other.str_of_node);
	}

	// 普通版本的 content distance，即不进行归一化
	int content_distG(PlanTreeHash& other)
	{
		return editDistG(str_of_node, other.str_of_node);
	}

	// 返回两者之间 cost 的差异
	double cost_dist(PlanTreeHash& other)
	{
		double res = abs(root->data.cost - other.root->data.cost) / max_cost;  // 没有取 log 的 cost 差异
		// double res = abs(cost_log - other.cost_log) / max_cost;  // 对 cost 取log，再计算差异
		return res;
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
// 利用 Group Tree 进行过滤的算法
void ARENAGTFilter(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep, std::unordered_map<int, CCost>* id2Cost);
// 利用 AOS 策略将结构相同的 plan 加入到采样之中
void ARENAAos(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep);

/******************** ARENA EXP ********************/
#ifdef ARENA_EXP
int getIndex(char c)
{
	switch (c)
	{
	case '[':
		return 0;
	case ']':
		return 1;
	case '$':
		return 2;
	default:
		std::cerr << "出现了特殊字符 " << c << '\n';
		return 2;
	}
}

edge* findEdge(edge* e, char c)
{
	if (e == nullptr)
	{
		return nullptr;
	}
	return e->m_pEndNode->m_pEdgeVector[getIndex(c)];
}

void node::preOrder(int prefix, std::string& s, std::ostream& fout)
{
	for (int i = 0; i < m_length; i++)
	{
		if (m_pEdgeVector[i] == nullptr)
		{
			continue;
		}

		// 打印前缀
		for (int j = 0; j < prefix; j++)
		{
			fout << ' ';
		}
		// 打印边上的信息
		edge* e = m_pEdgeVector[i];
		for (int j = e->m_startIndex; j < e->m_endIndex; j++)
		{
			fout << s[j];
		}
		fout << '\n';
		// 打印子节点
		e->m_pEndNode->preOrder(prefix + 4, s, fout);
	}
}

node::~node()
{
	for (int i = 0; i < m_length; i++)
	{
		delete m_pEdgeVector[i];
	}
	delete m_pSuffixLink;
}

int node::calculateNum()
{
	if (m_num == 0)
	{
		m_num = 1;
		for (int i = 0; i < m_length; i++)
		{
			if (m_pEdgeVector[i] != nullptr)
			{
				m_num += m_pEdgeVector[i]->m_pEndNode->calculateNum();
			}
		}
	}
	return m_num;
}

void suffixTreeNew::construct()
{
	m_originalStr += '$';
	for (std::size_t i = 0; i < m_originalStr.size(); i++)
	{
		m_remainder++;
		char c = m_originalStr[i];
		int index = getIndex(c);
		// 根据 m_activate 的情况进行相应的处理
		// 最简单的情况，当前在根节点并且原来没有相应的字符
		if (m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix == '\0')
		{
			// 原来没有相应的字符
			if (m_activate.m_pNode->m_pEdgeVector[index] == nullptr)
			{
				// 直接加入新的边和节点
				edge* e = new edge();
				node* tempNode = new node();
				e->m_pStartNode = m_activate.m_pNode;
				e->m_pEndNode = tempNode;
				e->m_startIndex = (int)i;
				e->m_endIndex = (int)(m_originalStr.size());
				m_activate.m_pNode->m_pEdgeVector[index] = e;
				m_remainder--;
			}
			else // 原来有相应的字符
			{
				m_activate.m_edgePrefix = c;
				m_activate.m_len = 1;
			}
		} 
		// 当前是根节点，但是活动边不是空字符 (m_activate.m_edgePrefix != 0)
		else if(m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix != '\0')
		{
			// 检查是新字符还是已有字符
			int subIndex = getIndex(m_activate.m_edgePrefix);
			edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
			// 检查这条边上的下一个字符（！！！！！！！！可能存在的结果是下面已经进行了分裂，当前没有考虑）
			// 当长度大于这条边上的字符个数时
			if (m_activate.m_len >= e->size())
			{
				// 它一定是一个一个增加的，极限情况就是 == 的情况，判断是否存在该字符引出的边
				node* tempNode = e->m_pEndNode;
				if (tempNode->m_pEdgeVector[index] != nullptr)
				{
					// 更新活动节点
					m_activate.m_pNode = tempNode;
					m_activate.m_edgePrefix = c;
					m_activate.m_len = 1;
				}
				else
				{
					// 分裂边
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
			else
				// 还能在原来边上继续增加
			{
				char subC = m_originalStr[e->m_startIndex + m_activate.m_len];  // 取得这条边上的下一个字符，并于接下来的字符比较
				if (c == subC)
				{
					// 继续增加 activate_length 和 remainder
					m_activate.m_len++;
				}
				else  // 不相等，递归地处理各个activate point直到为 remainder 为0，或者有相同的后缀
				{
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
		}
		// 活动节点不是根节点
		else if (!m_activate.m_pNode->m_isRoot)
		{
			int subIndex = getIndex(m_activate.m_edgePrefix);
			edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
			if (m_activate.m_len >= e->size())
			{
				// 它一定是一个一个增加的，极限情况就是 == 的情况，判断是否存在该字符引出的边
				node* tempNode = e->m_pEndNode;
				if (tempNode->m_pEdgeVector[index] != nullptr)
				{
					// 更新活动节点
					m_activate.m_pNode = tempNode;
					m_activate.m_edgePrefix = c;
					m_activate.m_len = 1;
				}
				else
				{
					// 分裂边
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
			else
				// 还能在原来边上继续增加
			{
				char subC = m_originalStr[e->m_startIndex + m_activate.m_len];  // 取得这条边上的下一个字符，并于接下来的字符比较
				if (c == subC)
				{
					// 继续增加 activate_length 和 remainder
					m_activate.m_len++;
				}
				else  // 不相等，递归地处理各个activate point直到为 remainder 为0，或者有相同的后缀
				{
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
		}
	}
}

bool suffixTreeNew::onePoint(std::size_t currIndex, node* preNode)
{
	char c = m_originalStr[currIndex];
	int index = getIndex(c);
	if (m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix == '\0')
	{
		// 原来没有相应的字符
		if (m_activate.m_pNode->m_pEdgeVector[index] == nullptr)
		{
			// 直接加入新的边和节点
			edge* e = new edge();
			node* tempNode = new node();
			e->m_pStartNode = m_activate.m_pNode;
			e->m_pEndNode = tempNode;
			e->m_startIndex = (int)currIndex;
			e->m_endIndex = (int)(m_originalStr.size());
			m_activate.m_pNode->m_pEdgeVector[index] = e;
			m_remainder--;
		}
		else // 原来有相应的字符
		{
			m_activate.m_edgePrefix = c;
			m_activate.m_len = 1;
		}
		return false;
	} 

	// 当前是根节点，但是活动边不是空字符 (m_activate.m_edgePrefix != 0)
	if(m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix != '\0')
	{
		// 检查是新字符还是已有字符
		int subIndex = getIndex(m_activate.m_edgePrefix);
		edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
		// 检查这条边上的下一个字符（！！！！！！！！可能存在的结果是下面已经进行了分裂，当前没有考虑）
		if (m_activate.m_len >= e->size())
		{
			// 它一定是一个一个增加的，极限情况就是 == 的情况，判断是否存在该字符引出的边
			node* tempNode = e->m_pEndNode;
			if (tempNode->m_pEdgeVector[index] != nullptr)
			{
				// 更新活动节点
				m_activate.m_pNode = tempNode;
				m_activate.m_edgePrefix = c;
				m_activate.m_len = 1;
				return false;
			}
			else
			{
				// 分裂边
				// 将原来的边分为两部分
				node* subNode = e->m_pEndNode;
				e->m_pEndNode = new node();
				e->m_endIndex = e->m_startIndex + m_activate.m_len;

				edge* newEdge = new edge();
				newEdge->m_startIndex = e->m_endIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = subNode;
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[getIndex(m_originalStr[newEdge->m_startIndex])] = newEdge;
				// 创建一条新边
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
				// 检查是否需要创建后缀连接
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				// 之后更新 m_activate
				m_remainder--;
				if (m_remainder == 1)
				{
					m_activate.m_edgePrefix = '\0';
				}
				else
				{
					m_activate.m_edgePrefix = m_originalStr[currIndex + 1 - m_remainder];
				}
				m_activate.m_len--;
				return true;
			}
		}
		else
		{
			char subC = m_originalStr[e->m_startIndex + m_activate.m_len];  // 取得这条边上的下一个字符，并于接下来的字符比较
			if (c == subC)
			{
				// 继续增加 activate_length 和 remainder
				m_activate.m_len++;
				return false;  // 不需要继续处理
			}
			else  // 不相等，递归地处理各个activate point直到为 remainder 为0，或者有相同的后缀
			{
				// 首先进行边的分裂
				// 将原来的边分为两部分
				node* subNode = e->m_pEndNode;
				e->m_pEndNode = new node();
				e->m_endIndex = e->m_startIndex + m_activate.m_len;

				edge* newEdge = new edge();
				newEdge->m_startIndex = e->m_endIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = subNode;
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[getIndex(m_originalStr[newEdge->m_startIndex])] = newEdge;
				// 创建一条新边
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
				// 检查是否需要创建后缀连接
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				// 之后更新 m_activate
				m_remainder--;
				if (m_remainder == 1)
				{
					m_activate.m_edgePrefix = '\0';
				}
				else
				{
					m_activate.m_edgePrefix = m_originalStr[currIndex + 1 - m_remainder];
				}
				m_activate.m_len--;
				return true;
			}
		}
	}
	// 活动节点不是根节点
	if (!m_activate.m_pNode->m_isRoot)
	{
		int subIndex = getIndex(m_activate.m_edgePrefix);
		edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
		if (m_activate.m_len >= e->size())
		{
			// 它一定是一个一个增加的，极限情况就是 == 的情况，判断是否存在该字符引出的边
			node* tempNode = e->m_pEndNode;
			if (tempNode->m_pEdgeVector[index] != nullptr)
			{
				// 更新活动节点
				m_activate.m_pNode = tempNode;
				m_activate.m_edgePrefix = c;
				m_activate.m_len = 1;
				return false;
			}
			else
			{
				node* subNode = e->m_pEndNode;
				e->m_pEndNode = new node();
				e->m_endIndex = e->m_startIndex + m_activate.m_len;

				edge* newEdge = new edge();
				newEdge->m_startIndex = e->m_endIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = subNode;
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[getIndex(m_originalStr[newEdge->m_startIndex])] = newEdge;
				// 创建一条新边
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
				// 检查是否需要创建后缀连接
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				// 之后更新 m_activate
				m_remainder--;
				if (m_activate.m_pNode->m_pSuffixLink != nullptr)
				{
					// 存在后缀连接
					m_activate.m_pNode = m_activate.m_pNode->m_pSuffixLink;
				}
				else
				{
					m_activate.m_pNode = &m_root;
				}
				return true;
			}
		}
		else
			// 还能在原来边上继续增加
		{
			char subC = m_originalStr[e->m_startIndex + m_activate.m_len];  // 取得这条边上的下一个字符，并于接下来的字符比较
			if (c == subC)
			{
				// 继续增加 activate_length 和 remainder
				m_activate.m_len++;
			}
			else  // 不相等，递归地处理各个activate point直到为 remainder 为0，或者有相同的后缀
			{
				node* subNode = e->m_pEndNode;
				e->m_pEndNode = new node();
				e->m_endIndex = e->m_startIndex + m_activate.m_len;

				edge* newEdge = new edge();
				newEdge->m_startIndex = e->m_endIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = subNode;
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[getIndex(m_originalStr[newEdge->m_startIndex])] = newEdge;
				// 创建一条新边
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
				// 检查是否需要创建后缀连接
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				// 之后更新 m_activate
				m_remainder--;
				if (m_activate.m_pNode->m_pSuffixLink != nullptr)
				{
					// 存在后缀连接
					m_activate.m_pNode = m_activate.m_pNode->m_pSuffixLink;
				}
				else
				{
					m_activate.m_pNode = &m_root;
				}
				return true;
			}
		}
	}
	return false;
}

void suffixTreeNew::generateMS(std::string& s)
{
	matchingStatistic.clear();
	node* start = &m_root;  // 从哪个节点进行匹配
	std::size_t alreadyMatch = 0;  // 已经匹配的字符长度
	std::size_t maxMatch = 0;  // 匹配的最大长度
	// 每一次需要进行以下过程：1、需要最大匹配 2、更新上述变量，为下次寻找做准备
	for (std::size_t i = 0; i < s.size(); i++)
	{
		node* pre = start;
		std::size_t j = alreadyMatch;
		edge* e = start->m_pEdgeVector[getIndex(s[i+j])];
		while (j < maxMatch && j+e->size() < maxMatch)  // 快速去除已经实现的匹配
		{
			node* temp = e->m_pEndNode;
			pre = temp;
			j += e->size();
			e = temp->m_pEdgeVector[getIndex(s[i + j])];
		}

		int k = 0;
		if (j == maxMatch)  // 前一轮重复的匹配已经跳过，开始本轮的匹配
		{
			while (e != nullptr && i + j < s.size() && s[i + j] == getChar(e, k))
			{
				j++;
				k++;
				if (k == e->size())
				{
					if (i + j == s.size())  // 已经匹配完了
					{
						break;
					}
					pre = e->m_pEndNode;
					e = findEdge(e, s[i + j]);
					k = 0;
				}
			}

		}
		// 设置本轮匹配信息
		maxMatch = j;
		if (k == 0)  // 恰好把某条边匹配完
		{
			matchingStatistic.push_back(std::make_pair(maxMatch, pre));
		}
		else
		{
			matchingStatistic.push_back(std::make_pair(maxMatch, e->m_pEndNode));
		}
		// 设置下轮匹配信息
		maxMatch--;
		node* tempNode;
		if (k == 0)
		{
			tempNode = pre;
		}
		else
		{
			tempNode = e->m_pStartNode;
		}
		alreadyMatch = maxMatch - k;
		start = tempNode->m_pSuffixLink == nullptr ? &m_root : tempNode->m_pSuffixLink;
	}
}

void suffixTreeNew::generateSS()
{
	m_treeRecord.resize(m_originalStr.size());
	int left = 0, right = 0;
	int i = (int)m_originalStr.size()-1;
	for(; i >= 0; i--)
	{
		switch (m_originalStr[i])
		{
		case '[':
			left++;
			break;
		case ']':
			right++;
			break;
		default:
			break;
		}
		m_treeRecord[i] = std::make_pair(left, right);
	}
}

double suffixTreeNew::distance(suffixTreeNew& other)
{
	if (other.m_treeRecord.size() == 0)
	{
		other.generateSS();
	}

	generateMS(other.m_originalStr);
	double res = 0.0;

	int i = 0;
	for (auto & p: matchingStatistic)
	{
		int length = p.first;
		int left, right;
		if ((std::size_t)(i + length) >= other.m_originalStr.size())
		{
			left = other.m_treeRecord[i].first;
			right = other.m_treeRecord[i].second;
		}
		else
		{
			left = other.m_treeRecord[i].first - other.m_treeRecord[i + length].first;
			right = other.m_treeRecord[i].second - other.m_treeRecord[i + length].second;
		}
		res += left > right ? 0 : p.second->m_num;
		i++;
	}
	return res;
}

template <class T>
struct PlanTreeExp  // 利用后缀树计算树核的实现
{
	// 与 best_plan 的结构和cost的偏离程度。也可以理解为某个 plan 的价值。当前我们认为 cost 差异越大，结构和内容差异越小，价值越大
	double offset;

	PlanTreeNode *root;
	suffixTreeNew *m_stn;
	bool init_flag;
	std::string str_serialize;
	int node_num;
	double s_dist_best, c_dist_best, cost_dist_best;  // 与 best_plan 之间的各种距离
	double self_kernel;  // 用于对结构距离进行归一化

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

	// 返回当前树的总 cost ，即根节点的 cost
	double get_cost(){
		return root->data.cost;
	}

	void insert_child(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

			// 检查这个操作是否为 CPhysical 操作，因为这个操作，所以 PlanTree 不再适用于其它类型
#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				// 检查是否是 Index 操作，如果是，输出详细信息
				if (name_len > 5 && !strncmp(expression_name, "Index", 5)){  // IndexScan
#ifdef ARENA_DEBUG
					// std::ofstream fout_index("/tmp/Index");
					// fout_index << "存在 index 节点\n";
#endif
					gpos::ARENAIOstream stream;
					// 这一步用于输出索引名
					if (strncmp(expression_name, "IndexS", 6) == 0){
						dynamic_cast<CPhysicalIndexScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}else if (strncmp(expression_name, "IndexO", 6) == 0){
						dynamic_cast<CPhysicalIndexOnlyScan*>(child_expression->Pop())->ARENAGetInfo(stream);
					}
					stream << '\n';

					// 这一步用于取得索引的过滤条件
					if (child_expression->Arity() == 1){  // 恰好只有一个孩子
						ARENAGetIndexInfo((*child_expression)[0], stream);
					} else {  // 有多个孩子或者没有孩子
						stream << "子表达式的个数与预期不符，子表达式的个数为：" << child_expression->Arity();
					}
					cur->data.info = stream.ss.str();

#ifdef ARENA_DEBUG
					// if (fout_index.is_open()){
					// 	fout_index << expression_name << '\n';
					// 	fout_index << stream.ss.str();
					// 	fout_index.close();
					// }
#endif
				}

				// 需要检查名字是否是 Scan 类型，如果是，需要获得 relation 的名字

				// 字符串长度大于4，并且最后四个字母是 Scan，需要取得表名
				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";  // 节点的标签为: 扫描操作符
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
						PlanTreeNode * table = new PlanTreeNode(NodeData(0.0, temp.substr(start, end-start), "TABLE"));
						cur->push_back(table);
					}
				} else if(name_len > 4 && !strcmp(expression_name + name_len -4, "Join"))  // Join 操作符，设置 JOIN 标签
				{
					cur->data.tag = "JOIN";
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child((*child_expression)[i], *cur);
				}

				// 检查 如果是 Motion 操作节点，并且节点下只有一个子节点，并且该节点的 cost 和 子节点的 cost 相同，则去除该节点
				const char * motionOp = "Motion";
				if (cur->data.name.size() > 6 && cur->data.name.compare(0, 6, motionOp) == 0 && cur->size() == 1 && 
					cur->data.cost - (*cur)[0]->data.cost < 0.1 * cur->data.cost )  // 该节点与子节点的 cost 差异很小
				{
					// 将当前节点从父节点中去除并将子节点移入到父节点的孩子中
					parent.child[parent.size()-1] = (*cur)[0];
					// 释放当前节点的内存
					cur->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
					delete cur;
				}
#ifdef ARENA_PHYSICAL
			}
#endif
		}
	}

	PlanTreeExp()
	{
		offset = 0.0;
		root = NULL;
		init_flag = false;
	}

	~PlanTreeExp()
	{
		if (NULL != root)
			delete root;
	}


	void init(T* plan, int flag)  // 与上述 init 函数的功能相同，但是该函数中将各个部分进行了拆分，用于测试不同初始化的时间
	{
		if (flag == 0 && NULL != plan)
		{
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			// 如果节点是 表扫描操作 或者是 连接操作，设置 node 的 tag
			if (root->data.name.size() > 4)
			{
				std::size_t compareStart = root->data.name.size() - 4;
				if(root->data.name.compare(compareStart, 4, "Scan") == 0)
				{
					root->data.tag = "SCAN";
				} else if (root->data.name.compare(compareStart, 4, "Join") == 0)
				{
					root->data.tag = "JOIN";
				}
			}

			// 添加子节点
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}

			// 检测节点是否是 Motion 节点，如果是，删除
			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )  // 该节点与子节点的 cost 差异很小
			{
				// 临时存储子节点的地址
				auto temp = (*root)[0];
				// 释放当前节点的内存
				root->child[0] = nullptr;  // 析构函数会递归地释放孩子的内存，所以用空指针代替
				delete root;
				root = temp;
			}
		}

		init_flag = true;
		if (flag == 1)
		{
			// 将树转变为字符串，之后再构建后缀树
			str_serialize = postOrder(root);
			m_stn = new suffixTreeNew();
			m_stn->init(str_serialize);
			// stn.init(str_serialize);
		}
	}

	std::string postOrder(PlanTreeNode* root)
	{
		if (root == nullptr) {
			return "";
		}

		std::string treeStr;
		if (root->size() == 0) {  // 叶子节点
			treeStr = "[]";
		}
		else
		{
			treeStr = '[';
			for (size_t i = 0; i < root->size(); i++)
			{
				treeStr += postOrder((*root)[i]);
			}
			treeStr += ']';
		}

		return treeStr;
	}

public:
	//****************** 距离相关的一些函数 ********************************/
};
#endif
/************************************************************/


///**************************************************/
/// 一些会用到的全局变量
///**************************************************/
std::queue<CExpression *> plan_buffer;  // 用于存储所有 CExpressioin 的缓存，主线程用于向该变量中不断地
std::vector<CExpression *> plan_buffer_for_exp;  // 用于存储所有 CExpressioin 的缓存，主线程用于向该变量中不断地
std::vector<PlanTree<CExpression>> plan_trees;  // 添加元素，辅助线程将其转换为 PlanTree
std::vector<PlanTreeHash<CExpression>> plan_trees_hash;
std::vector<PlanTreeHash<CExpression>> plan_trees_send;  // 用于最后发送相关 plan 时，生成 plan 的详细信息

// #define ARENA_EXP


///**************************************************/
/// 负责将所有的结果都存储到 HTTP 客户端中，最后统一发送
/// 参数：
///		id 代表这个 plan tree 在 plan_trees 中的索引
///**************************************************/
void addResult(int id)
{
	plan_trees_send.emplace_back(PlanTreeHash<CExpression>());
	plan_trees_send.back().init_detailed(plan_trees_hash[id].inOriginal);

	nlohmann::json j;
	j["id"] = counter+1;
	j["cost"] = plan_trees_hash[id].root->data.cost; 

	if (counter == 0)  // best plan
	{
		j["s_dist"] = 0;
		j["c_dist"] = 0;
		j["cost_dist"] = 0; 
		j["relevance"] = 0;
	}
	else
	{
		j["s_dist"] = plan_trees_hash[id].structure_dist(plan_trees_hash[0]);
		j["c_dist"] = plan_trees_hash[id].content_dist(plan_trees_hash[0]);
		j["cost_dist"] = plan_trees_hash[id].cost_dist(plan_trees_hash[0]);
		j["relevance"] = plan_trees_hash[id].offset; 
	}

	j["content"] = plan_trees_send.back().root->generate_json();  // 只有 content 内容需要使用详细信息，其它部分可以用原来 Plan 的信息
	std::string result= j.dump();
    if(web_client->isStart())
    {
        web_client->send_result(result);
    }

	// 负责将 counter + 1
	++counter;
}


template<class T>
void FindK(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record)
{
	res.push_back(0);  // res 的第一个元素是 QEP
	addResult(0);  // 将数据发送

    // 总数比需要找到的数量少
    if (plans.size() <= gARENAK)
    {
		for (std::size_t i = 1; i < plans.size(); ++i)
        {
			if (plans[i].root->data.cost != plans[0].root->data.cost)
			{
				res.push_back(i);
				addResult(i);  // 将数据发送
			}
        }
    }
    // 寻找 k 个 plan
    else {
		std::vector<MinDist> tempRemoved;  // 记录需要暂时移除的计划
		tempRemoved.reserve(plans.size() / 3);  // 默认先申请 1/3 的内存

        for (std::size_t i = 0; i < gARENAK; ++i)
		// 迭代 k 次
		// 每次都从 dist_record 的最大值处进行遍历，直到可以进行剪枝为止
        {
			MinDist maxValue;
			maxValue.dist = -100;
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
			addResult(maxValue.index);
			for(auto & md : tempRemoved)
				dist_record.push(md);
			tempRemoved.clear();
        }
    }
}

// 处理 I-AQPS 模式的相关算法算法
void handler(int a) {
    a++;
}

// 更改信号的控制函数，更改的信号为 SIGUSR1
void changeHandler(){
    struct sigaction newAct;
    newAct.sa_handler = handler;
    sigemptyset(&newAct.sa_mask);
    newAct.sa_flags = 0;

    sigaction(SIGUSR1, &newAct, nullptr);
}

// 用于对文件进行加锁和释放锁的操作
// Args:
//		fd 文件描述符
//		type 1->加锁  0->释放锁
// Return:
//		0 成功
// 		other 失败
int lockFile(int fd, int type)
{
	struct flock lock;
	lock.l_whence = SEEK_SET;
	lock.l_start = 0;
	lock.l_len = 0;
	if(type == 1)
	{
		lock.l_type = F_WRLCK;
	}
	else if (type == 0)
	{
		lock.l_type = F_UNLCK;
	}

	return fcntl(fd, F_SETLKW, &lock);
}

// 向文件描述符中输出字符串 s
void ARENAWrite(int fd, std::string & s)
{
	const char * res = s.c_str();
	write(fd, res, strlen(res));
}

// 告诉前端当前进程停止运行，删除数据库中该 pid 的记录
void ARENATellFrontStop(pid_t pid) {
	curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL * handle = curl_easy_init();

    if (handle == nullptr){
        std::exit(1);
    } else {
		std::string url = "http://127.0.0.1:5000/arena_inner/i_stop/";
		url += std::to_string(pid);
        curl_easy_setopt(handle, CURLOPT_URL, url.c_str());
        curl_easy_perform(handle);

    }
    curl_easy_cleanup(handle);
}

// 用于 I-AQPS 模式将结果输出到文件中
// 它会申请文件写锁，然后写入，之后关闭文件，释放锁
void IAQPOutputRes(int id)
{
	// 打开文件，并对其加锁
	std::string fileName = "/tmp/" + gResFile;
	int fd = open(fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU|S_IRWXG);
	if(fd == -1)
	{
		return;
	}
	if(lockFile(fd, 1) != 0)  // 加锁
	{
		return;
	}

	// 生成需要输出的内容
	std::string result;
	if(id == -1)  // 已经没有新的 plan 可以返回了
	{
		result = "no more";
	}
	else
	{
		plan_trees_send.emplace_back(PlanTreeHash<CExpression>());
		plan_trees_send.back().init_detailed(plan_trees_hash[id].inOriginal);

		nlohmann::json j;
		j["id"] = counter+1;
		j["cost"] = plan_trees_hash[id].root->data.cost; 

		if (counter == 0)  // best plan
		{
			j["s_dist"] = 0;
			j["c_dist"] = 0;
			j["cost_dist"] = 0; 
			j["relevance"] = 0;
		}
		else
		{
			j["s_dist"] = plan_trees_hash[id].structure_dist(plan_trees_hash[0]);
			j["c_dist"] = plan_trees_hash[id].content_dist(plan_trees_hash[0]);
			j["cost_dist"] = plan_trees_hash[id].cost_dist(plan_trees_hash[0]);
			j["relevance"] = plan_trees_hash[id].offset; 
		}

		j["content"] = plan_trees_send.back().root->generate_json();  // 只有 content 内容需要使用详细信息，其它部分可以用原来 Plan 的信息
		result= j.dump();
		counter++;
	}
	ARENAWrite(fd, result);
	// 输出换行
	write(fd, "\n", 1);
	// 输出 Pid
	result = std::to_string(getpid());
	ARENAWrite(fd, result);

	// 释放锁
	lockFile(fd, 0);
	close(fd);
}


template<class T>
void FindK_I(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record)
{
	changeHandler();

	res.push_back(0);  // res 的第一个元素是 QEP
	IAQPOutputRes(0);  // 将数据发送
	unsigned int t;
	t = sleep(300);  // 等待5分钟
	if (t == 0)  // 被信号唤醒
	{
		ARENATellFrontStop(getpid());
		return;
	}

	std::vector<MinDist> tempRemoved;  // 记录需要暂时移除的计划
	tempRemoved.reserve(plans.size() / 3);  // 默认先申请 1/3 的内存

	for (std::size_t i = 0; i < plans.size(); ++i)
	// 迭代 k 次
	// 每次都从 dist_record 的最大值处进行遍历，直到可以进行剪枝为止
	{
		MinDist maxValue;
		maxValue.dist = -100;
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
		for(auto & md : tempRemoved)
			dist_record.push(md);
		tempRemoved.clear();

		IAQPOutputRes(maxValue.index);
		t = sleep(300);  // 等待5分钟
		if (t != 0)  // 被信号唤醒
		{
			continue;
		}
		else
		{
			ARENATellFrontStop(getpid());
			return;
		}
	}
	while(true)
	{
		IAQPOutputRes(-1);
		t = sleep(60);  // 等待1分钟
		if (t != 0)  // 被信号唤醒
		{
			continue;
		}
		else
		{
			ARENATellFrontStop(getpid());
			return;
		}
	}
}

double ARENACalculateDist(std::vector<int> & res)
{
	plan_trees_hash.reserve(res.size());
	// std::ofstream fout_test("/tmp/test", std::ios_base::app);

	// fout_test << "\n尝试获得 cost 的最大值\n";
	// 设置 cost 的最大值
	for(std::size_t i=0;i<plan_buffer_for_exp.size();i++)
	{
		double temp_cost = plan_buffer_for_exp[i]->Cost().Get();
		if (temp_cost> max_cost)
		{
			max_cost = temp_cost;
		}
	}
	// fout_test << "获得 cost 的最大值成功\n";

	// 初始化的第零阶段，生成 plan_tree 结构
	for (std::size_t i = 0; i < res.size(); i++)
	{
		plan_trees_hash.push_back(PlanTreeHash<CExpression>());
		plan_trees_hash[i].init(plan_buffer_for_exp[res[i]], 0);
	}
	// fout_test << "第一阶段初始成功\n";

	// 初始化第一阶段，生成一棵树的所有统计信息
	for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
	{
		plan_trees_hash[i].init(NULL, 1);
	}
	// fout_test << "第二阶段初始成功\n";

	// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
	for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
	{
		plan_trees_hash[i].init(NULL, 2);
	}
	// fout_test << "第三阶段初始成功\n";

	// 初始化第三阶段，计算自己与自己的树核
	for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
	{
		plan_trees_hash[i].init(NULL, 3);
	}
	// fout_test << "第四阶段初始成功\n";
	max_cost -= plan_trees_hash[0].root->data.cost;

	// 计算每个计划于 QEP 的距离
	double min_dist;
	{
		double temp_dist;
		for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
		{
			temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
			if (temp_dist < min_dist)
			{
				min_dist = temp_dist;
			}
		}
	}
	// fout_test << "计算每个计划于 QEP 的距离成功\n";

	{
		double temp_dist;
		for(std::size_t i=1; i < plan_trees_hash.size(); ++i)
		{
			for(std::size_t j=i+1;j < plan_trees_hash.size(); ++j)
			{
				temp_dist = plan_trees_hash[i].distance(plan_trees_hash[j]);
				if (temp_dist < min_dist)
				{
					min_dist = temp_dist;
				}
			}
		}
	}
	// fout_test << "计算最小距离成功\n";
	plan_trees_hash.clear();
	return min_dist;
}

double ARENACalculateDist(std::unordered_set<int> & res)
{
	std::vector<int> new_res;
	for(auto n: res)
	{
		new_res.push_back(n);
	}
	sort(new_res.begin(), new_res.end());
	return ARENACalculateDist(new_res);
}


// 用于实验的随机选择算法
template<class T>
double FindKRandomExp(std::vector<T>& plans, std::vector<int>& res, std::size_t k)
{
	std::default_random_engine rand(time(NULL));
    std::uniform_int_distribution<int> dis(1, plans.size()-1);
	double distance = -100;
	for(int iter=0;iter<30;iter++)  // 迭代 30 次
	{
		std::unordered_set<int> res_set;  // 记录最终结果的编号
		res_set.insert(0);
		std::size_t num = 0;
		while(num < k)
		{
			int id = dis(rand) % plans.size();
			if(res_set.find(id) == res_set.end())  // 原来没有记录
			{
				res_set.insert(id);
				num++;
			}
		}

		double temp_dist = ARENACalculateDist(res_set);
		if(temp_dist > distance)
		{
			res.clear();
			for(auto &n: res_set)
			{
				res.push_back(n);
			}
			distance = temp_dist;
		}
	}

	return distance;
}

// 用于实验的基于 cost 的选择算法
template<class T>
void FindKCostExp(std::vector<T>& plans, std::vector<int>& res, std::size_t k)
{
	std::vector<std::pair<int, double>> idCost;
	idCost.reserve(plans.size());
	for(std::size_t i=0;i<plans.size();i++)
	{
		idCost.push_back(std::make_pair(i, plans[i]->Cost().Get()));
	}

	sort(idCost.begin(), idCost.end(), [](std::pair<int, double> x, std::pair<int, double> y){ return x.second < y.second;});
	for(std::size_t i=0;i<k+1;i++)
	{
		res.push_back(idCost[i].first);
	}
}

template<class T>
void FindKTimeExpNew(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record)
{
	res.push_back(0);  // res 的第一个元素是 QEP

	std::vector<MinDist> tempRemoved;  // 记录需要暂时移除的计划
	tempRemoved.reserve(plans.size() / 3);  // 默认先申请 1/3 的内存

	for (std::size_t i = 0; i < 100; ++i)  // 寻找10个结果
	// 迭代 k 次
	// 每次都从 dist_record 的最大值处进行遍历，直到可以进行剪枝为止
	{
		MinDist maxValue;
		maxValue.dist = -100;
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
		for(auto & md : tempRemoved)
			dist_record.push(md);
		tempRemoved.clear();
	}
}

template<class T>
void FindKTimeExpOld(std::vector<T>& plans, std::vector<int>& res, std::unordered_map<int, double> & dist_record, std::size_t k=0)
{
    std::vector<char> falive(plans.size(), 'a');
    res.push_back(0);
	if(k == 0)
	{
		k = 10;
	}

	for (std::size_t i = 0; i < k; ++i)  // 迭代 k 次
	{
		// 本轮迭代需要加入的索引
		int index = -1;
		double max_min = -DBL_MIN;
		double dist;

		// 第一次迭代，需要初始化 dist_record
		if (i == 0)
		{
			for (std::size_t j = 1; j < plans.size(); ++j)
			{
				dist = dist_record[j];
				if (dist > max_min)
				{
					index = j;
					max_min = dist;
				}
			}
		}
		else
		{
			for (std::size_t j = 1; j < plans.size(); ++j)
			{
				if (falive[j])
				{
					// 找到最小距离
					dist = plans[new_add].distance(plans[j]);
					if (dist < dist_record[j])
					{
						dist_record[j] = dist;
					}
					else
					{
						dist = dist_record[j];
					}

					// 大于当前已经找到的最小距离的最大值
					if (dist > max_min)
					{
						index = j;
						max_min = dist;
					}
				}
			}
		}

		res.push_back(index);
		falive[index] = 0;  // 标注这个 index 已经被选出
		new_add = index;
	}
}

template<class T>
double FindKDiffMethodExp(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record, std::size_t k, std::vector<double> * dist_list = nullptr)
{
	res.push_back(0);  // res 的第一个元素是 QEP

	std::vector<MinDist> tempRemoved;  // 记录需要暂时移除的计划
	tempRemoved.reserve(plans.size() / 3);  // 默认先申请 1/3 的内存
	double res_dist = 0.0;

	// 迭代 k 次
	// 每次都从 dist_record 的最大值处进行遍历，直到可以进行剪枝为止
	for (std::size_t i = 0; i < k; ++i)  // 寻找10个结果
	{
		MinDist maxValue;
		maxValue.dist = -100;
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
		res_dist = maxValue.dist;
		if(dist_list != nullptr)
		{
			dist_list->push_back(maxValue.dist);
		}

		for(auto & md : tempRemoved)
			dist_record.push(md);
		tempRemoved.clear();
	}
	return res_dist;
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

	// 第一次执行 memotmap 函数
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

		/************************* ARENA *************************/
		auto start = std::chrono::steady_clock::now();
		std::ofstream fout_time("/tmp/treeNode");
		m_pmemo->BuildTreeMap(poc);
		optimizer_config->GetEnumeratorCfg()->SetPlanSpaceSize(
			m_pmemo->Pmemotmap()->UllCount());
		fout_time << "统计 Plan 的数量(构造 GT) 花费的时间为: " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

		/**************************************************/

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
	// GPOS_SET_TRACE(EopttraceDisableMotions);
	// GPOS_SET_TRACE(EopttraceDisableMotionHashDistribute);
	// GPOS_SET_TRACE(EopttraceDisableMotionGather);
	// GPOS_SET_TRACE(EopttraceDisableMotionBroadcast);
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


	// GPOS_UNSET_TRACE(EopttraceDisableMotions);
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
	CExpression *pexpr = Pmemotmap()->PrUnrank(m_mp, pdpctxtplan, plan_id);  // 获取 plan 的关键部分
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
	// ULLONG ullCount = Pmemotmap()->UllCount();  // 暂时修改
	ULLONG ullCount = 39999;
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
//		提取一个 plan sample，并且根据枚举器的配置来处理异常
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
#ifdef ARENA_DEBUG
	auto time_start = std::chrono::steady_clock::now();
	auto all_start = time_start;
	std::ofstream fout_time("/tmp/time_record");
	std::ofstream fout_plan("/tmp/Plans");
	fout_plan << "# 执行了采样\n";
#endif

	// 读取配置信息
	readConfig();
#ifdef ARENA_GTEXP
	// 当进行 Group Tree 的过滤实验时，执行的代码
	std::ifstream fin_gt("/tmp/gtArg");
	if(fin_gt.is_open())
	{
		// 读取第一行，其格式为: 阈值数量  距离阈值
		fin_gt >> gGTNumThreshold;
		fin_gt >> gGTFilterThreshold;
		fin_gt >> gSampleThreshold;
		fin_gt.close();
	}
#endif
	#ifdef ARENA_DEBUG
	fout_plan << "# 配置信息读取完成\n";
	fout_plan << "# 全局配置参数如下：\n";
	fout_plan << "plan 数量: " << gARENAK << '\n';
	fout_plan << "s_weight: " << gSWeight << '\n';
	fout_plan << "c_weight: " << gCWeight << '\n';
	fout_plan << "cost_weight: " << gCostWeight << '\n';
	fout_plan << "lambda: " << gLambda << '\n';
	fout_plan << "resultFileName: " << gResFile << '\n';
	fout_plan << "进行过滤所需要的 plan 数量阈值为: " << gGTNumThreshold << '\n';
	fout_plan << "过滤 plan 的距离阈值: " << gGTFilterThreshold << '\n';
	fout_plan << "采样阈值为: " << gSampleThreshold << '\n';
	fout_plan << "用户选择的方法: " << gMethod << '\n';
	if(gTEDFlag)
		fout_plan << "使用 Tree Edit Flag 计算距离\n";
	else
		fout_plan << "使用 subtree kernel 计算距离\n";

	// 获取 root Group 的 expressioin 的数量
	// fout_plan << "root Group 中 expression 的总数量；" << m_pmemo->PgroupRoot()->UlGExprs() << '\n';
	#endif
	//************************ARENA**************************/
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	GPOS_ASSERT(NULL != optimizer_config);

	CEnumeratorConfig *pec = optimizer_config->GetEnumeratorCfg();

	pec->ClearSamples();

	ULLONG ullCount = Pmemotmap()->UllCount();
	//************************ARENA**************************/
	#ifdef ARENA_DEBUG
	fout_plan << "# 所有可能的组合数为：" << ullCount << std::endl;
	#endif
	gIsGTFilter = ullCount >= (ULLONG)gGTNumThreshold;  // 如果 plan 的数量大于50000，自动进行过滤
	gisSample = gJoin >= gSampleThreshold;
	if(gisSample)  // 如果执行采样，那么就没有必要进行过滤了
	{
		gIsGTFilter = false;
	}
	std::unordered_set<int> filteredId;  // 将要被过滤掉的 id
	//**************************************************/
	if (0 == ullCount)
	{
		// optimizer generated no plans
		return;
	}


	ULLONG ullTargetSamples = ullCount;

	// find cost of best plan
	CExpression *pexpr =
		m_pmemo->PexprExtractPlan(m_mp, m_pmemo->PgroupRoot(), m_pqc->Prpp(),
								  m_search_stage_array->Size());
	CCost costBest = pexpr->Cost();
	pec->SetBestCost(costBest);
	//************************ARENA**************************/
	fout_plan << "TreeNode 的数量为：" << m_pmemo->Pmemotmap()->UlNodes() << std::endl;
	std::vector<std::string>& groupTree = m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTree;
	fout_plan << "GroupTree 的数量为：" << groupTree.size() << std::endl;
	// 打印 ARENA_groupTreePlus 的信息
	std::unordered_map<std::string, std::vector<int>>& groupTreePlus = m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus;
#ifdef ARENA_COSTFT
	std::unordered_map<int, CCost> & id2Cost = m_pmemo->Pmemotmap()->ProotNode()->ARENA_id2Cost;
	fout_plan << std::showpoint;
#endif
	fout_plan << "ARENA_groupTreePlus 中包含的 GroupTree 的数量为：" << groupTreePlus.size() << '\n';
	// for(auto iter_temp = id2Cost.begin();iter_temp != id2Cost.end();iter_temp++)
	// {
	// 	double temp_cost = iter_temp->second.Get();
	// 	if(temp_cost > max_cost)
	// 		max_cost = temp_cost;
	// }
	// fout_plan << "最大的 cost 为: " << max_cost << '\n';

// 	for(auto & p: groupTreePlus)  // 打印每个 GroupTree 对应的编号
// 	{
// 		fout_plan << "*" <<  p.first << " :  ";
// 		for(auto &id: p.second)
// 		{
// #ifdef ARENA_COSTFT
// 			fout_plan << id << '(' << id2Cost.at(id).Get() << ") ";
// #else
// 			fout_plan << id << ' ' ;
// #endif
// 		}
// 		fout_plan << '\n';
// 	}

	// best plan 放在 plan_trees 中的第一个元素
	plan_buffer.push(pexpr);
#ifdef ARENA_EXP
	plan_buffer_for_exp.push_back(pexpr);
#endif
	{
		PlanTreeHash<CExpression> tempTree;  // 这个是 best plan
		tempTree.init(pexpr);

		// 如果需要利用 Group Tree 进行过滤，并且没有达到采样的阈值
		if(gIsGTFilter)
		{
			fout_plan << "需要进行过滤\n";
			// 寻找所有需要被过滤的 GroupTree 以及其对应的 plan 的 id，将其记录到 filteredId 中
#ifdef ARENA_COSTFT
			ARENAGTFilter(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree, &id2Cost);
#else
			ARENAGTFilter(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree, nullptr);
#endif
			fout_plan << "要过滤的 plan_id 为: ";
			for(auto tempIter=filteredId.begin(); tempIter != filteredId.end(); tempIter++)
			{
				fout_plan << *tempIter << ' ';
			}
			fout_plan << '\n';
		}
		// 如果需要采样
		if(gisSample)
		{
			fout_plan << "需要进行采样\n";
			ullTargetSamples = 10000;
			ARENAAos(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree);

			fout_plan << "要采样的数量为: " << filteredId.size() << "其 id 为: ";
			for(auto tempIter=filteredId.begin(); tempIter != filteredId.end(); tempIter++)
			{
				fout_plan << *tempIter << ' ';
			}
			fout_plan << '\n';
		}

	#ifdef ARENA_DEBUG
		fout_plan << tempTree.root->generate_json();
		fout_plan << "\n";
	#endif
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
	// const ULLONG ullMaxIters = ullTargetSamples * GPOPT_SAMPLING_MAX_ITERS;  // 最大的迭代次数是目标采样数量 X 30
	// ULLONG ullIters = 0;  // 记录当前的迭代次数
	ULLONG ull = 0;  // 记录当前的采样数量

	std::vector<ULLONG> plan_id_list;
	{
		plan_id_list.reserve(40000);
		std::ifstream fin_temp("/tmp/plan_id.txt");
		if(fin_temp.is_open())
		{
			fout_plan << "成功打开 id 文件\n";
			ULLONG temp_id = 0;
			for(int i=0;i<40000;i++)
			{
				fin_temp >> temp_id;
				plan_id_list.push_back(temp_id);
			}
			fin_temp.close();
		}
	}

	while (ull < ullTargetSamples)
	{
		// generate id of plan to be extracted
		ULLONG plan_id = ull;  // 如果进行全部采样，当前的 plan_id 就是当前的采样数量，默认情况下 plan id = ull，除采样情况例外
		
		if(gisSample)  // 如果需要进行采样，随机生成 plan id
		{
			plan_id = UllRandomPlanId(&seed);
			plan_id = plan_id_list[(int)plan_id];
		}
		fout_plan << "当前的 plan id 为: " << plan_id << '\n';

		if(gIsGTFilter && filteredId.find(plan_id) != filteredId.end())  // 需要用过滤算法，并且这个 id 的 plan 与 qep 的结构差异很大
		{
			fout_plan << "过滤掉 plan " << plan_id << '\n';
			ull++;
			continue;
		}

		pexpr = NULL;
		// BOOL fAccept = false;
		
		// 下面的整体流程为：1、是否能够找到有效的 plan (FValidPlanSample) 2、是否可以将该 plan 加入到采样中
		if (FValidPlanSample(pec, plan_id, &pexpr))
		{
			// add plan to the sample if it is below cost threshold
			// 如果当前的 cost 低于 cost 阈值，将当前的 plan 加入到采样结果中
			// CCost cost = pexpr->Cost();
			//************************ARENA**************************/
			// 保留该 plan，将其加入都队列中
			// fAccept = pec->FAddSample(plan_id, cost);  // 这个可能没有用!!!!!!!!
			// fAccept = true;
			plan_buffer.push(pexpr);
#ifdef ARENA_EXP
			plan_buffer_for_exp.push_back(pexpr);
#endif
			// 输出到文件中用于测试
#ifdef ARENA_DEBUG
			{
				PlanTreeHash<CExpression> tempTree;
				tempTree.init(pexpr);
				fout_plan << '$' << tempTree.root->generate_json();
				fout_plan << "\n";
			}
#endif
			//************************ARENA**************************/
			pexpr = NULL;
		}
		else
		{
#ifdef ARENA_DEBUG
			fout_plan << "# FValidPlanSample 失败\n";
#endif
		}

		ull++;
	}
	if(gisSample)  // 用来将 Aos 策略选出的 plan 添加到采样 plan 中
	{
		gAosStart = plan_buffer_for_exp.size();
		for(auto id: filteredId)
		{
			ULLONG plan_id = (ULLONG)id;
			fout_plan << "当前的 plan id 为: " << plan_id << '\n';

			pexpr = NULL;
			
			// 下面的整体流程为：1、是否能够找到有效的 plan (FValidPlanSample) 2、是否可以将该 plan 加入到采样中
			if (FValidPlanSample(pec, plan_id, &pexpr))
			{
				plan_buffer.push(pexpr);
	#ifdef ARENA_EXP
				plan_buffer_for_exp.push_back(pexpr);
	#endif
				// 输出到文件中用于测试
	#ifdef ARENA_DEBUG
				{
					PlanTreeHash<CExpression> tempTree;
					tempTree.init(pexpr);
					fout_plan << '$' << tempTree.root->generate_json();
					fout_plan << "\n";
				}
	#endif
				pexpr = NULL;
			}
			else
			{
	#ifdef ARENA_DEBUG
				fout_plan << "# FValidPlanSample 失败\n";
	#endif
			}
		}
	}



#ifdef ARENA_DEBUG
	fout_plan << "ull: " << ull << "\tullTargetSamples: " << ullTargetSamples
			  << std::endl;

	fout_plan << "找到的有效 plan 个数为：" << plan_buffer.size() << '\n';

	if (gResFile.size() == 0 || gResFile.compare("###") == 0){
		fout_plan << "# 查询请求的结果文件名不存在，不会进行 ARENA 查询\n";
	}
	fout_plan.close();
#endif

	//************************ARENA**************************/
#ifdef ARENA_DEBUG
	auto time_end = std::chrono::steady_clock::now();  // 采样结束
	if (fout_time.is_open())						   // 输出时间
	{
		fout_time << "采样花费的时间为："
				  << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count()
				  << "ms\n";
	}

	time_start = std::chrono::steady_clock::now();
#endif
	if (gResFile.size() > 0 && gResFile.compare("###") != 0)
	{
		fout_time << "execute TIPS\n";
		DealWithPlan();
	}
#ifdef ARENA_EXP
	else
	{
		// DealWithPlan();

		// 在这里可以进行任何不需要前端和 web 的实验
		/* 不同算法效率的实验 */
		// ARENATimeExp3();
		// ARENATimeExp3Random();
		// ARENATimeExp3Cost();

		/* Group Tree 的过滤性实验 */
		// ARENAGTExp();

		ARENAAosExp();
		// ARENAFindTargetType();  // 用于寻找特定类型的 plan
	}
#endif

#ifdef ARENA_DEBUG
	time_end = std::chrono::steady_clock::now();  // 计算结束
	if (fout_time.is_open())					  // 输出时间
	{
		fout_time << "计算 k alternative plans 花费的时间为："
				  << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count()
				  << "ms\n";
		fout_time << "*采样与AQPS的工程总共花费的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - all_start)).count() << '\n';
	}
	fout_time.close();
#endif
	//************************ARENA**************************/

	// pec->PrintPlanSample();  // ORCA 系统自带的采样结果输出函数，暂时用不到，所以将其关闭以减少 I/O
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
// 利用 libcurl 库时，需要利用该函数将 http 请求得到的结果写入全局变量
size_t writeToConf(char *ptr, size_t size, size_t nmemb, void *userdata){
    char buffer[4096];
    for(size_t i=0;i<nmemb;i++) {
        buffer[i] = ptr[i];
    }
    buffer[nmemb] = '\0';
    gConf += buffer;

	// 没用的代码，只是未来通过编译器的检查
	if(userdata != nullptr){
		size++;
		size--;
	}
    return nmemb;
}

// 从 WEB 端取得相应的配置信息
bool arenaGetConfig(char mode) {
	gConf = "";  // 在请求配置信息之前，将配置信息清空
	curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL * handle = curl_easy_init();
	CURLcode res;

    if (handle == nullptr){
        std::exit(1);
    } else {
        std::cout << "CURL 初始化成功\n";
        curl_mimepart * part;
        curl_mime * mime;
        mime = curl_mime_init(handle);
        part = curl_mime_addpart(mime);

		if (mode == 'B'){
			// curl_mime_data(part, ARENA_file_name, CURL_ZERO_TERMINATED);
			curl_mime_data(part, ARENA_file_name, CURL_ZERO_TERMINATED);
			curl_mime_name(part, "hash");
		} else if (mode == 'I'){
			curl_mime_data(part, std::to_string(getpid()).c_str(), CURL_ZERO_TERMINATED);
			curl_mime_name(part, "hash");
		}
        curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToConf);
        curl_easy_setopt(handle, CURLOPT_MIMEPOST, mime);
        curl_easy_setopt(handle, CURLOPT_URL, (void *)"http://127.0.0.1:5000/arena_inner/conf");
        res = curl_easy_perform(handle);

        curl_mime_free(mime);
    }
    curl_easy_cleanup(handle);
	return res == 0;
}


// 读取 ARENA 系统的全局配置信息
// Args:
//		mode：是 B-APQ 还是 I-APQ，如果是 I-APQ 模式，相关配置信息通过本进程的pid进行标识
void readConfig(char mode)
{
	std::ofstream fout_temp("/tmp/debug");
	fout_temp << "读取配置信息\n";
	// 先从 web 端取得相应的信息
	if(!arenaGetConfig(mode)){  // 不能顺利获取到信息，退出该函数，使用默认参数（可能的原因是没有开启服务器）
		return;
	}
	fout_temp << "请求配置信息完成\n";
	fout_temp << gConf << std::endl;
	gConf = gConf.substr(1);  // 由于第一个字符是 '"' ，所以需要去除
	// 配置信息的详细格式为 k;s;c;cost;lambda;mode;gtFilterThreshold;targetFileName，并且确保最后一个参数一定是目标文件名。下面进行解析
	size_t  index = 0;
	size_t  pre = 0;
	int i= 1;
	for(index = gConf.find(';', pre); index != std::string::npos; i++)
	{
		std::string s = gConf.substr(pre, index-pre);
		pre = index+1;
		index = gConf.find(';', pre);

		fout_temp << s << '\n';
		switch(i){
			case 1:
				gARENAK = std::stoi(s);
				break;
			case 2:
				gSWeight = std::stod(s);
				break;
			case 3:
				gCWeight = std::stod(s);
				break;
			case 4:
				gCostWeight = std::stod(s);
				break;
			case 5:
				gLambda = std::stod(s);
				break;
			case 6:
				gMode = s[0];
				break;
			case 7:
				gGTFilterThreshold = std::stod(s);
				break;
			case 8:
				gJoin = std::stoi(s);
				break;
			case 9:
				gTEDFlag = std::stoi(s);
				break;
		}
	}
	// 最后一定是最终结果的文件名
	index = gConf.find('"', pre);
	gResFile = gConf.substr(pre, index-pre);
	fout_temp << gResFile << std::endl;
	fout_temp.close();
}


// 利用中序遍历来获得相应的信息，其节点类型有以下几种可能，对不同的操作符采用不同的处理方式：
//		CScalarBoolOp: 标量 bool 运算符 ->  ( , 左孩子，根节点，有孩子，)
//		CScalarCmp: 标量比较运算符 -> 左孩子，根节点，右孩子
//		CScalarCast: 不太清楚其具体用法，当前理解为重命名运算符  -> 寻找子节点
//		CScalarIdent: 标量列标识符 -> 当前节点的名称
//		CScalarConst: 标量常量运算符 -> 当前节点的名称
//		other: 其它未知的情况 -> 操作符的名称
void ARENAGetIndexInfo(CExpression * exp, IOstream &os){
	const char *name_start = exp->Pop()->SzId();
	if (strcmp(name_start, "CScalarBoolOp") == 0){ 
		switch(exp->Arity()){
			case 2:
				os << "( ";
				ARENAGetIndexInfo((*exp)[0], os);
				os << "  ";
				exp->Pop()->OsPrint(os);
				os << "  ";
				ARENAGetIndexInfo((*exp)[1], os);
				os << ") ";
				return;
			case 1:
				os << "( ";
				exp->Pop()->OsPrint(os);
				os << "  ";
				ARENAGetIndexInfo((*exp)[0], os);
				os << ") ";
				return;
			default:
				os << "CScalarBoolOp 具有" << exp->Arity() << "个孩子节点，返回";
				return;
		}
	} else if (strcmp(name_start, "CScalarCmp") == 0) {
		switch(exp->Arity()){
			case 2:
				ARENAGetIndexInfo((*exp)[0], os);
				os << " ";
				exp->Pop()->OsPrint(os);
				os << " ";
				ARENAGetIndexInfo((*exp)[1], os);
				return;
			default:
				os << "CScalarCmp 具有" << exp->Arity() << "个孩子节点，返回";
				return;
		}
	} else if (strcmp(name_start, "CScalarCast") == 0) {
		switch(exp->Arity()){
			case 1:
				ARENAGetIndexInfo((*exp)[0], os);
				return;
			default:
				os << "CScalarCast 具有" << exp->Arity() << "个孩子节点，返回";
				return;
		}
	} else if (strcmp(name_start, "CScalarIdent") == 0) {
		exp->Pop()->OsPrint(os);
		return;
	} else if (strcmp(name_start, "CScalarConst") == 0) {
		exp->Pop()->OsPrint(os);
		return;
	} else {
		os << "未识别的操作符：" << name_start;
		return;
	}
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
	std::ofstream fout(filename+"ARENA_result");
	if(fout.is_open())
	{
		// fout << calculateMaxMin(res) << std::endl;
		fout << "[";
		nlohmann::json j;
		for (std::size_t i=0;i<res.size();++i)
		{
			j["id"] = i+1;
			j["cost"] = plan_trees_hash[res[i]].root->data.cost;

			if (i == 0)  // best plan
			{
				j["s_dist"] = 0;
				j["c_dist"] = 0;
				j["cost_dist"] = 0;
				j["relevance"] = 0;
				j["distance"] = 0;
			}
			else
			{
				double s, c, cost;
				s = plan_trees_hash[res[i]].structure_dist(plan_trees_hash[0]);
				c = plan_trees_hash[res[i]].content_dist(plan_trees_hash[0]);
				cost = plan_trees_hash[res[i]].cost_dist(plan_trees_hash[0]);
				j["s_dist"] = s;
				j["c_dist"] = c;
				j["cost_dist"] = cost;
				j["relevance"] = plan_trees_hash[res[i]].offset;
				j["distance"] = s * gSWeight + c * gCWeight + cost * gCostWeight;
			}

			// j["content"] = plan_trees_hash[res[i]].root->generate_json();
			fout << j;
			fout << ",\n";
		}
		fout.seekp(-1, std::ios::cur);
		fout << "]";
		fout.close();
	}
	// 重置全局变量
	std::vector<PlanTreeHash<CExpression>> ().swap(plan_trees_hash);
	find_index.clear();
	// plan_trees.clear();
	new_add = 0;
	max_cost = 0.0;
	gARENAK = 5;
}


// 在采样时，将结构相同的 plan 重新加入到采样中
// 
// Args:
//		groupTreePlus: CTreeNode 中的 ARENA_groupTreePlus 成员
//		record: 用于记录那些需要被剪枝的 plan 的 id
//		qep: best plan 的结构
void ARENAAos(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep)
{
	std::size_t target_len = qep.str_serialize.size();
	// 遍历 groupTreePlus ，如果某个 GT 的结构和 qep 相同，将其中所有的 plan id 记录
	for(auto & iter: groupTreePlus)
	{
		if(iter.first.size() != target_len)
			continue;

		std::string s = ARENASortSTree(iter.first);
		if(s == qep.str_serialize)
		{
			for(auto id: iter.second)
				record.insert(id - 1);
		}
	}
}

// 利用 Group Tree 对 plan 进行过滤和剪枝
// 
// Args:
//		groupTreePlus: CTreeNode 中的 ARENA_groupTreePlus 成员
//		record: 用于记录那些需要被剪枝的 plan 的 id
//		qep: best plan 的结构
//		id2Csot: 记录 id 到 cost 的映射，便于根据 cost 进行过滤
void ARENAGTFilter(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep, std::unordered_map<int, CCost>* id2Cost)
{
	std::vector<gtTree> gtList;
	std::ofstream fout_dist("/tmp/gtDist");
	if(nullptr == id2Cost)
	{
		id2Cost = nullptr;
	}

#ifdef ARENA_COSTFT
	double tMinCost = qep.get_cost();
	double tMaxCost = 0.0;
	for(auto iter = id2Cost->begin();iter != id2Cost->end(); iter++)
	{
		double temp = iter->second.Get();
		if(temp > tMaxCost)
			tMaxCost = temp;
	}
	tMaxCost -= tMinCost;
#endif
	
	// 初始化 Group Tree
	for(auto iter=groupTreePlus.begin(); iter!=groupTreePlus.end();iter++)
	{
		gtList.emplace_back(gtTree(iter));
		gtList.back().init();
	}

	// 过滤结构差异过大的 Group Tree
	double temp=0.0;
	for(auto & t: gtList)
	{
		temp = qep.structure_dist(t);
		fout_dist << t.inIter->first << "    " << temp << '\n';
		if(temp > gGTFilterThreshold)  // 大于结构差异的阈值
		{
			for(int tempId: t.inIter->second)  // 遍历每一个 id
			{
#ifdef ARENA_COSTFT
				temp = id2Cost->at(tempId).Get();
				if((temp-tMinCost)/tMaxCost >= gGTFilterThreshold)
				{
					record.insert(tempId - 1);
				}
#else
				record.insert(tempId - 1);
#endif
			}
		}
	}
	fout_dist.close();
}


void DealWithPlan() {
	if(gMode == 'B')
	{
		web_client = new Sender;
		web_client->sha256 = gResFile;
		plan_trees_send.resize(gARENAK+1);
	}
	else if(gMode == 'I')  // 对于 I 模式，最多返回 plan_buffer_for_exp.size 个 plan ，而每个 plan 都需要 plan_trees_send
	{
		plan_trees_send.resize(plan_buffer_for_exp.size());
	}

	if (gARENAK == 0)  // 只选择 QEP
	{
		plan_trees_hash.push_back(PlanTreeHash<CExpression>());
		plan_trees_hash[0].init(plan_buffer.front());
		addResult(0);
		std::queue<gpopt::CExpression*> ().swap(plan_buffer);  // 目的在于清空 plan_buffer
	}
	else
	{
		plan_trees_hash.reserve(plan_buffer.size());
		std::size_t i=0;
#ifdef ARENA_DEBUG
		std::ofstream fout_time("/tmp/timeRecord.txt");
        if (fout_time.is_open())
        {
			fout_time << "计划数量为: " << plan_buffer.size() << std::endl;
            auto start = std::chrono::steady_clock::now();
			auto startAll = start;
#endif
            // 初始化的第一阶段，生成 plan_tree 结构
			// 记录已经出现过的树，如果相同的树重复出现可以直接删除，既可以直接避免结果中出现相同的结果，也可以加快程序执行速度。
			// 之所以会出现相同的树，是因为 plan_tree 中只保留了 Physical 节点，使得原本不同的执行计划可能变为相同的计划。
			// 经验证发现，即使保留其它节点，仍然会有重复的 plan 出现，为什么？
			{
				std::unordered_map<std::string, std::unordered_set<int>> temp_plan_tree;
				// while(!plan_buffer.empty())
				for(auto expressionIter: plan_buffer_for_exp)
				{
					plan_trees_hash.push_back(PlanTreeHash<CExpression>());
					plan_trees_hash[i].init_detailed(expressionIter);
					plan_trees_hash[i].init(nullptr, 1);

					// 设置 cost 的最大值
					if (plan_trees_hash[i].get_cost() > max_cost)
					{
						max_cost = plan_trees_hash[i].get_cost();
					}

					// 对树进行序列化，判断是否已经存在相同的树
					std::string & tempTreeStr = plan_trees_hash[i].str_serialize;
					fout_time << "第 " << i << " 个 plan 的树结构为: "  << tempTreeStr << '\n';
					auto tempIter = temp_plan_tree.find(tempTreeStr);
					if(tempIter != temp_plan_tree.end()){  // 存在相同的字符串
						int tempCost = (int)(plan_trees_hash[i].get_cost());
						if (tempIter->second.find(tempCost) != tempIter->second.end()){  // 相同的字符串的 cost 也相同
							// 删除当前元素
							// fout_time << "第 " << i << " 个元素重复，其序列化的值为：" << tempTreeStr << '\n';
							plan_trees_hash.pop_back();
							i--;
						} else{
							tempIter->second.insert(tempCost);
						}
					} else {  // 还没有相同的字符串
						temp_plan_tree[tempTreeStr] = std::unordered_set<int>{(int)(plan_trees_hash[i].get_cost())};
					}
					
					++i;
				}
			}
            fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "此时剩余的 plan 个数为: " << plan_trees_hash.size() << '\n';
            start = std::chrono::steady_clock::now();

			// 初始化的剩余阶段
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 2);  // 转变为字符串，用于计算内容差异
				plan_trees_hash[i].init(NULL, 3);  // 计算自己与自己的树核
			}
            fout_time << "初始化剩余阶段的时间为: " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

            max_cost -= plan_trees_hash[0].root->data.cost;

			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
            std::priority_queue<MinDist> dist_record;
            // 这个代码块用来计算 relevance
            {
                double temp_dist;
				for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
				{
					temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
            }

			// 统计寻找最终结果所用时间
            fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			std::vector<int> res;
			if (gMode == 'B'){  // B-APQ 模式
				FindK(plan_trees_hash, res, dist_record);
				fout_time << "寻找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
				fout_time << "最终找到结果的编号为：" << '\n';
				for(auto i: res){
					fout_time << i << '\t';
				}
				fout_time << '\n';
			} else if (gMode == 'I'){  // I-APQ 模式
				fout_time << "调用 I-AQPS 查询\n";
				fout_time << "结果的输出文件是: " << gResFile << '\n';
				FindK_I(plan_trees_hash, res, dist_record);
			}

            ARENA_result(res);  // 将结果保留到文件中
            fout_time << "程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;
            fout_time.close();
        }
	}
	if(gMode == 'B')
	{
		web_client->Close();
	}


	// 重置全局变量
	delete web_client;
	std::vector<PlanTree<CExpression>> ().swap(plan_trees);
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
	bow_flag = false;
	gARENAK = 5;
}

void FindKRandom() {
	web_client = new Sender;
	web_client->sha256 = gResFile;
	plan_trees_send.resize(gARENAK+1);

	if (gARENAK == 0)  // 只选择 QEP
	{
		plan_trees_hash.push_back(PlanTreeHash<CExpression>());
		plan_trees_hash[0].init(plan_buffer.front());
		addResult(0);
		std::queue<gpopt::CExpression*> ().swap(plan_buffer);  // 目的在于清空 plan_buffer
	}
	else
	{
		plan_trees_hash.reserve(plan_buffer.size());
		std::size_t i=0;
#ifdef ARENA_DEBUG
		std::ofstream fout_time("/home/wang/timeRecord.txt");
        if (fout_time.is_open())
        {
			fout_time << "计划数量为: " << plan_buffer.size() << std::endl;
            auto start = std::chrono::steady_clock::now();
#endif
            // 初始化的第一阶段，生成 plan_tree 结构
			// 记录已经出现过的树，如果相同的树重复出现可以直接删除，既可以直接避免结果中出现相同的结果，也可以加快程序执行速度。
			// 之所以会出现相同的树，是因为 plan_tree 中只保留了 Physical 节点，使得原本不同的执行计划可能变为相同的计划。
			// 经验证发现，即使保留其它节点，仍然会有重复的 plan 出现，为什么？
			{
				std::unordered_map<std::string, std::unordered_set<int>> temp_plan_tree;
				// while(!plan_buffer.empty())
				for(auto expressionIter: plan_buffer_for_exp)
				{
					plan_trees_hash.push_back(PlanTreeHash<CExpression>());
					plan_trees_hash[i].init_detailed(expressionIter);
					plan_trees_hash[i].init(nullptr, 1);

					// 设置 cost 的最大值
					if (plan_trees_hash[i].get_cost() > max_cost)
					{
						max_cost = plan_trees_hash[i].get_cost();
					}

					// 对树进行序列化，判断是否已经存在相同的树
					std::string & tempTreeStr = plan_trees_hash[i].str_serialize;
					fout_time << "第 " << i << " 个 plan 的树结构为: "  << tempTreeStr << '\n';
					auto tempIter = temp_plan_tree.find(tempTreeStr);
					if(tempIter != temp_plan_tree.end()){  // 存在相同的字符串
						int tempCost = (int)(plan_trees_hash[i].get_cost());
						if (tempIter->second.find(tempCost) != tempIter->second.end()){  // 相同的字符串的 cost 也相同
							// 删除当前元素
							// fout_time << "第 " << i << " 个元素重复，其序列化的值为：" << tempTreeStr << '\n';
							plan_trees_hash.pop_back();
							i--;
						} else{
							tempIter->second.insert(tempCost);
						}
					} else {  // 还没有相同的字符串
						temp_plan_tree[tempTreeStr] = std::unordered_set<int>{(int)(plan_trees_hash[i].get_cost())};
					}
					
					++i;
				}
			}
            fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "此时剩余的 plan 个数为: " << plan_trees_hash.size() << '\n';
            start = std::chrono::steady_clock::now();

			// 初始化的剩余阶段
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 2);  // 转变为字符串，用于计算内容差异
				plan_trees_hash[i].init(NULL, 3);  // 计算自己与自己的树核
			}
            fout_time << "初始化剩余阶段的时间为: " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

            max_cost -= plan_trees_hash[0].root->data.cost;

			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
            std::priority_queue<MinDist> dist_record;
            // 这个代码块用来计算 relevance
            {
                double temp_dist;
				for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
				{
					temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
            }
            fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();


			if(plan_trees_hash.size() <= gARENAK)
			{
				gARENAK = plan_trees_hash.size() - 1;
			}

			// 进行随机的选择
			std::default_random_engine rand(time(NULL));
			std::uniform_int_distribution<int> dis(1, plan_trees_hash.size()-1);
			double distance = -100;
			std::vector<int> res;
			for(int iter=0;iter<30;iter++)  // 迭代 30 次
			{
				std::unordered_set<int> res_set;  // 记录最终结果的编号
				res_set.clear();
				res_set.insert(0);
				std::size_t num = 0;
				while(num < gARENAK)
				{
					int id = dis(rand) % plan_trees_hash.size();
					if(res_set.find(id) == res_set.end())  // 原来没有记录
					{
						res_set.insert(id);
						num++;
					}
				}

				// 计算本次选出的结果的最小距离
				double temp_dist = 100.0;
				{
					std::vector<int> temp_list{res_set.begin(), res_set.end()};
					for(std::size_t i=0;i<temp_list.size();i++)
					{
						for(std::size_t j=i+1;j<temp_list.size();j++)
						{
							double tt_dist = plan_trees_hash[i].distance(plan_trees_hash[j]);
							if(tt_dist < temp_dist)
								temp_dist = tt_dist;
						}
					}
				}
				if(temp_dist > distance)
				{
					res.clear();
					for(auto &n: res_set)
					{
						res.push_back(n);
					}
					distance = temp_dist;
				}
			}

			// 将结果进行发送
			addResult(0);
			for(auto iter: res)
			{
				if(iter != 0)
					addResult(iter);
			}
		}
	}

	web_client->Close();

	// 重置全局变量
	delete web_client;
	std::vector<PlanTree<CExpression>> ().swap(plan_trees);
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
	bow_flag = false;
	gARENAK = 5;
}

void FindKCost() {
	web_client = new Sender;
	web_client->sha256 = gResFile;
	plan_trees_send.resize(gARENAK+1);

	if (gARENAK == 0)  // 只选择 QEP
	{
		plan_trees_hash.push_back(PlanTreeHash<CExpression>());
		plan_trees_hash[0].init(plan_buffer.front());
		addResult(0);
		std::queue<gpopt::CExpression*> ().swap(plan_buffer);  // 目的在于清空 plan_buffer
	}
	else
	{

		plan_trees_hash.reserve(plan_buffer.size());
		std::size_t i=0;
#ifdef ARENA_DEBUG
		std::ofstream fout_time("/home/wang/timeRecord.txt");
        if (fout_time.is_open())
        {
			fout_time << "计划数量为: " << plan_buffer.size() << std::endl;
            auto start = std::chrono::steady_clock::now();
#endif
            // 初始化的第一阶段，生成 plan_tree 结构
			// 记录已经出现过的树，如果相同的树重复出现可以直接删除，既可以直接避免结果中出现相同的结果，也可以加快程序执行速度。
			// 之所以会出现相同的树，是因为 plan_tree 中只保留了 Physical 节点，使得原本不同的执行计划可能变为相同的计划。
			// 经验证发现，即使保留其它节点，仍然会有重复的 plan 出现，为什么？
			{
				std::unordered_map<std::string, std::unordered_set<int>> temp_plan_tree;
				// while(!plan_buffer.empty())
				for(auto expressionIter: plan_buffer_for_exp)
				{
					plan_trees_hash.push_back(PlanTreeHash<CExpression>());
					plan_trees_hash[i].init_detailed(expressionIter);
					plan_trees_hash[i].init(nullptr, 1);

					// 设置 cost 的最大值
					if (plan_trees_hash[i].get_cost() > max_cost)
					{
						max_cost = plan_trees_hash[i].get_cost();
					}

					// 对树进行序列化，判断是否已经存在相同的树
					std::string & tempTreeStr = plan_trees_hash[i].str_serialize;
					fout_time << "第 " << i << " 个 plan 的树结构为: "  << tempTreeStr << '\n';
					auto tempIter = temp_plan_tree.find(tempTreeStr);
					if(tempIter != temp_plan_tree.end()){  // 存在相同的字符串
						int tempCost = (int)(plan_trees_hash[i].get_cost());
						if (tempIter->second.find(tempCost) != tempIter->second.end()){  // 相同的字符串的 cost 也相同
							// 删除当前元素
							// fout_time << "第 " << i << " 个元素重复，其序列化的值为：" << tempTreeStr << '\n';
							plan_trees_hash.pop_back();
							i--;
						} else{
							tempIter->second.insert(tempCost);
						}
					} else {  // 还没有相同的字符串
						temp_plan_tree[tempTreeStr] = std::unordered_set<int>{(int)(plan_trees_hash[i].get_cost())};
					}
					
					++i;
				}
			}
            fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "此时剩余的 plan 个数为: " << plan_trees_hash.size() << '\n';
            start = std::chrono::steady_clock::now();

			// 初始化的剩余阶段
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 2);  // 转变为字符串，用于计算内容差异
				plan_trees_hash[i].init(NULL, 3);  // 计算自己与自己的树核
			}
            fout_time << "初始化剩余阶段的时间为: " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

            max_cost -= plan_trees_hash[0].root->data.cost;

			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
            std::priority_queue<MinDist> dist_record;
            // 这个代码块用来计算 relevance
            {
                double temp_dist;
				for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
				{
					temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
            }
            fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
            start = std::chrono::steady_clock::now();

			if(plan_trees_hash.size() < gARENAK)
			{
				gARENAK = plan_trees_hash.size() - 1;
			}

			std::vector<std::pair<int, double>> idCost;
			std::vector<int> res;
			idCost.reserve(plan_trees_hash.size());
			for(std::size_t i=0;i<plan_trees_hash.size();i++)
			{
				idCost.push_back(std::make_pair(i, plan_trees_hash[i].get_cost()));
			}

			sort(idCost.begin(), idCost.end(), [](std::pair<int, double> x, std::pair<int, double> y){ return x.second < y.second;});
			for(std::size_t i=0;i<gARENAK+1;i++)
			{
				res.push_back(idCost[i].first);
			}

			// 将结果进行发送
			for(auto iter: res)
				addResult(iter);
		}
	}

	web_client->Close();

	// 重置全局变量
	delete web_client;
	std::vector<PlanTree<CExpression>> ().swap(plan_trees);
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
	bow_flag = false;
	gARENAK = 5;
}

void ARENATimeExp1() {
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (int tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n当前测试的 plan 数量为：" << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			auto startAll = start;
			// 初始化的第零阶段，生成 plan_tree 结构
			for (int i = 0; i < tempPlanNum; i++)
			{
				plan_trees_hash.push_back(PlanTreeHash<CExpression>());
				plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);

				// 设置 cost 的最大值
				if (plan_trees_hash[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[i].root->data.cost;
				}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第一阶段，生成一棵树的所有统计信息
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 1);
			}
			fout_time << "    生成子树统计信息的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
			for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
			{
				plan_trees_hash[i].init(NULL, 2);
			}
			fout_time << "    *取得由节点内容组成的字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第三阶段，计算自己与自己的树核
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
			{
				plan_trees_hash[i].init(NULL, 3);
			}
			fout_time << "    计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			max_cost -= plan_trees_hash[0].root->data.cost;
			

			// 当使用新方法时，需要用有限队列的形式存储 distance
			std::priority_queue<MinDist> dist_record;
			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
			// 这个代码块用来计算 relevance
			{
				double temp_dist;
				for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
				{
					temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
			}
			
			// // 当使用旧方法时，则用 hash 表存储 distance
            // std::unordered_map<int, double> dist_record;
			// for(std::size_t i=1;i<plan_trees_hash.size();++i)
			// {
			// 	double temp_dist = plan_trees_hash[i].distance_with_best(plan_trees[0]);
			// 	dist_record[i] = temp_dist;
			// }

			// 统计寻找最终结果所用时间
			fout_time << "*计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			std::vector<int> res;
			FindKTimeExpNew(plan_trees_hash, res, dist_record);
			fout_time << "*查找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "最终找到结果的编号为: " << '\n';
			for(auto i: res){
				fout_time << i << '\t';
			}
			fout_time << '\n';

			// ARENA_result(res);  // 将结果保留到文件中
			fout_time << "*程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;

			// 重置全局变量
			plan_trees_hash.clear();
			new_add = 0;
			max_cost = 0.0;
		}

		fout_time.close();
	}
}

void ARENATimeExp2() {
	plan_trees.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (int tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n当前测试的 plan 数量为：" << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			auto startAll = start;
			// 初始化的第一阶段，生成 plan_tree 结构
			for (int i = 0; i < tempPlanNum; i++)
			{
				plan_trees.push_back(PlanTree<CExpression>());
				plan_trees[i].init(plan_buffer_for_exp[i], 0);

				// 设置 cost 的最大值
				if (plan_trees[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees[i].root->data.cost;
				}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第二阶段，对一棵树进行序列化，用于之后计算后缀树
			for(std::size_t i=0;i<plan_trees.size();i++)  // init1
			{
				plan_trees[i].init(NULL, 1);
			}
			fout_time << "    生成字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第四阶段，初始化后缀树的上下文
			for(std::size_t i=0;i<plan_trees.size();i++)  // init3
			{
				plan_trees[i].init(NULL, 3);
			}
			fout_time << "    初始化上下文的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第五阶段，构造后缀树
			for(std::size_t i=0;i<plan_trees.size();i++)  // init4
			{
				plan_trees[i].init(NULL, 4);
			}
			fout_time << "    构造后缀树的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第六阶段，收尾工作1，计算每个节点的叶子节点的数量
			for(std::size_t i=0;i<plan_trees.size();i++)  // init5
			{
				plan_trees[i].init(NULL, 5);
			}
			fout_time << "    收尾工作1(计算节点数量)的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第七阶段，收尾工作2，计算与自己的距离
			for(std::size_t i=0;i<plan_trees.size();i++)  // init6
			{
				plan_trees[i].init(NULL, 6);
			}
			fout_time << "    收尾工作2(计算与自己的距离)的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()- start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第八阶段，对后缀树进行剪枝
			for(std::size_t i=0;i<plan_trees.size();i++)  // init7
			{
				plan_trees[i].init(NULL, 7);
			}
			fout_time << "    剪枝所需的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第三阶段，生成节点内容组成的字符串，用于计算内容差异
			for (std::size_t i = 0; i < plan_trees.size(); i++)
			{
				plan_trees[i].init(NULL, 2);
			}
			fout_time << "*第三阶段(取得由节点内容组成的字符串)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			max_cost -= plan_trees[0].root->data.cost;
			
			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异

			// 当使用新方法时，需要用有限队列的形式存储 distance
			std::priority_queue<MinDist> dist_record;
			// 这个代码块用来计算 relevance
			{
				double temp_dist;
				for (std::size_t i = 1; i < plan_trees.size(); ++i)
				{
					temp_dist = plan_trees[i].distance_with_best(plan_trees[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
			}
			
			// // 当使用旧方法时，则用 hash 表存储 distance
            // std::unordered_map<int, double> dist_record;
			// for(std::size_t i=1;i<plan_trees.size();++i)
			// {
			// 	double temp_dist = plan_trees[i].distance_with_best(plan_trees[0]);
			// 	dist_record[i] = temp_dist;
			// }

			// 统计寻找最终结果所用时间
			fout_time << "*计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			std::vector<int> res;
			FindKTimeExpNew(plan_trees, res, dist_record);
			fout_time << "*查找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "最终找到结果的编号为：" << '\n';
			for(auto i: res){
				fout_time << i << '\t';
			}
			fout_time << '\n';

			// ARENA_result(res);  // 将结果保留到文件中
			fout_time << "*程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;

			// 重置全局变量
			plan_trees.clear();
			new_add = 0;
			max_cost = 0.0;
		}

		fout_time.close();
	}
}

// 测试不同方法（随机选择、基于 cost 选择等方法）再选择相同数量的 AP 
// 时花费的时间和最终效果的对比实验
void ARENATimeExp3() {
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*当前要选出的候选计划的数量为: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			auto startAll = start;
			// 初始化的第零阶段，生成 plan_tree 结构
			for (std::size_t i = 0; i < plan_buffer_for_exp.size(); i++)
			{
				plan_trees_hash.push_back(PlanTreeHash<CExpression>());
				plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);

				// 设置 cost 的最大值
				if (plan_trees_hash[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[i].root->data.cost;
				}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第一阶段，生成一棵树的所有统计信息
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 1);
			}
			fout_time << "    生成子树统计信息的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
			for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
			{
				plan_trees_hash[i].init(NULL, 2);
			}
			fout_time << "    取得由节点内容组成的字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第三阶段，计算自己与自己的树核
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
			{
				plan_trees_hash[i].init(NULL, 3);
			}
			fout_time << "    计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			max_cost -= plan_trees_hash[0].root->data.cost;
			

			// 当使用旧方法时，则用 hash 表存储 distance
            std::unordered_map<int, double> dist_record;
			for(std::size_t i=1;i<plan_trees.size();++i)
			{
				double temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
				dist_record[i] = temp_dist;
			}

			// // 当使用新方法时，需要用有限队列的形式存储 distance
			// std::priority_queue<MinDist> dist_record;
			// // 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
			// // 这个代码块用来计算 relevance
			// {
			// 	double temp_dist;
			// 	for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
			// 	{
			// 		temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
			// 		MinDist temp;
			// 		temp.index = i;
			// 		temp.dist = temp_dist;
			// 		dist_record.push(temp);
			// 	}
			// }
			
			// 统计寻找最终结果所用时间
			fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			std::vector<int> res;
			double min_dist = 0.0;
			// min_dist = FindKDiffMethodExp(plan_trees_hash, res, dist_record, tempPlanNum);
			FindKTimeExpOld(plan_trees_hash, res, dist_record, tempPlanNum);
			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			fout_time << "*最小距离为: " << min_dist << '\n';
			fout_time << "查找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "最终找到结果的编号为：" << '\n';
			for(auto i: res){
				fout_time << i << '\t';
			}
			fout_time << '\n';

			// ARENA_result(res);  // 将结果保留到文件中
			fout_time << "*程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;

			// 重置全局变量
			plan_trees_hash.clear();
			new_add = 0;
			max_cost = 0.0;
		}

		fout_time.close();
	}
}

void ARENATimeExp3Random()
{
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	std::vector<int> res;
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*当前要选出的候选计划的数量为: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			double min_dist = FindKRandomExp(plan_buffer_for_exp, res, tempPlanNum);
			fout_time << "*最小距离为: " << min_dist << '\n';
			fout_time << "*程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			plan_trees_hash.clear();
			max_cost = 0.0;
		}
		fout_time.close();
	}
}

void ARENATimeExp3Cost()
{
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	std::vector<int> res;
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*当前要选出的候选计划的数量为: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			FindKCostExp(plan_buffer_for_exp, res, tempPlanNum);

			// fout_time << "选出的 plan id 和 cost 为: ";
			// for(auto i: res)
			// {
			// 	fout_time << "  (" << i << " , " << plan_buffer_for_exp[i]->Cost().Get() << ')';
			// }
			// fout_time << '\n';
			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			fout_time << "*最小距离为: " << ARENACalculateDist(res) << '\n';

			fout_time << "程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			plan_trees_hash.clear();
			res.clear();
			max_cost = 0.0;
		}
		fout_time.close();
	}
}

// 测试不同方法使用优化过的后缀树花费的时间
void ARENATimeExp4()
{
	std::vector<PlanTreeExp<CExpression>> plan_trees_exp;  // 添加元素，辅助线程将其转换为 PlanTree
	plan_trees_exp.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n*当前测试的 plan 数量为: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			// 初始化的第零阶段，生成 plan_tree 结构
			for (std::size_t i = 0; i < tempPlanNum; i++)
			{
				plan_trees_exp.push_back(PlanTreeExp<CExpression>());
				plan_trees_exp[i].init(plan_buffer_for_exp[i], 0);

				// 设置 cost 的最大值
				if (plan_trees_exp[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_exp[i].root->data.cost;
				}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第一阶段，构造后缀树
			for(std::size_t i=0;i<plan_trees_exp.size();i++)  // init1
			{
				plan_trees_exp[i].init(NULL, 1);
			}

			// 下一阶段，计算叶子节点的个数
			for(std::size_t i=0;i<plan_trees_exp.size();i++)  // 计算叶子节点的个数
			{
				plan_trees_exp[i].m_stn->calculateNum();
			}
			fout_time << "*构造后缀树（并计算叶子节点的个数）的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();
			
			
			for(std::size_t i=0;i<plan_trees_exp.size();i++)  // 计算自己与自己的树核
			{
				plan_trees_exp[i].m_stn->distance(*(plan_trees_exp[i].m_stn));
			}
			fout_time << "*计算自己的树核的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			plan_trees_exp.clear();
		}
		fout_time.close();
	}
}

// 测试不同方法使用优化过的后缀树花费的时间
void ARENATimeExp4Hash()
{
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n*当前要选出的候选计划的数量为: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			// 初始化的第零阶段，生成 plan_tree 结构
			for (std::size_t i = 0; i < tempPlanNum; i++)
			{
				plan_trees_hash.push_back(PlanTreeHash<CExpression>());
				plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);

				// 设置 cost 的最大值
				if (plan_trees_hash[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[i].root->data.cost;
				}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第一阶段，生成一棵树的所有统计信息
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
			{
				plan_trees_hash[i].init(NULL, 1);
			}
			fout_time << "*生成子树统计信息的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第三阶段，计算自己与自己的树核
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
			{
				plan_trees_hash[i].init(NULL, 3);
			}
			fout_time << "*计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			max_cost -= plan_trees_hash[0].root->data.cost;

			// 重置全局变量
			plan_trees_hash.clear();
			new_add = 0;
			max_cost = 0.0;
		}

		fout_time.close();
	}
}

// 原始的后缀树
void ARENATimeExp4Old() {
	plan_trees.reserve(plan_buffer.size());
	std::size_t i=0;
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		fout_time << "计划数量为: " << plan_buffer.size() << std::endl;
		auto start = std::chrono::steady_clock::now();
		// 初始化的第一阶段，生成 plan_tree 结构
		// 记录已经出现过的树，如果相同的树重复出现可以直接删除，既可以直接避免结果中出现相同的结果，也可以加快程序执行速度。
		// 之所以会出现相同的树，是因为 plan_tree 中只保留了 Physical 节点，使得原本不同的执行计划可能变为相同的计划。
		// 经验证发现，即使保留其它节点，仍然会有重复的 plan 出现，为什么？
		{
			std::unordered_map<std::string, std::unordered_set<int>> temp_plan_tree;
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
		}
		fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		fout_time << "此时剩余的 plan 个数为: " << plan_trees.size() << '\n';
		start = std::chrono::steady_clock::now();

		// 初始化第二阶段，对一棵树进行序列化，用于之后计算后缀树
		for(std::size_t i=0;i<plan_trees.size();i++)  // init1
		{
			plan_trees[i].init(NULL, 1);
		}
		fout_time << "    生成字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第四阶段，初始化后缀树的上下文
		for(std::size_t i=0;i<plan_trees.size();i++)  // init3
		{
			plan_trees[i].init(NULL, 3);
		}
		fout_time << "    初始化上下文的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第五阶段，构造后缀树
		for(std::size_t i=0;i<plan_trees.size();i++)  // init4
		{
			plan_trees[i].init(NULL, 4);
		}
		fout_time << "    构造后缀树的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();
		fout_time.close();
	}
}


// 该函数就是正常的 B-AQPS 算法，但是不会发送数据
// 参数的修改是在 SamplePlans 函数中发生的
void ARENAGTExp()
{
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		fout_time << "\n*当前候选计划的数量为: " << plan_buffer.size() << '\n';
		auto start = std::chrono::steady_clock::now();
		auto startAll = start;
		// 初始化的第零阶段，生成 plan_tree 结构
		for (std::size_t i = 0; i < plan_buffer_for_exp.size(); i++)
		{
			plan_trees_hash.emplace_back(PlanTreeHash<CExpression>());
			plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);

			// 设置 cost 的最大值
			if (plan_trees_hash[i].root->data.cost > max_cost)
			{
				max_cost = plan_trees_hash[i].root->data.cost;
			}
		}
		fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第一阶段，生成一棵树的所有统计信息
		for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init1
		{
			plan_trees_hash[i].init(NULL, 1);
		}
		fout_time << "生成子树统计信息的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
		for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
		{
			plan_trees_hash[i].init(NULL, 2);
		}
		fout_time << "取得由节点内容组成的字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第三阶段，计算自己与自己的树核
		for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
		{
			plan_trees_hash[i].init(NULL, 3);
		}
		fout_time << "计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();
		fout_time << "最大的 cost 为: " << max_cost << '\n';

		max_cost -= plan_trees_hash[0].root->data.cost;
		

		// 当使用旧方法时，则用 hash 表存储 distance
		// std::unordered_map<int, double> dist_record;
		// for(std::size_t i=1;i<plan_trees.size();++i)
		// {
		// 	double temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
		// 	dist_record[i] = temp_dist;
		// }

		// 当使用新方法时，需要用优先队列的形式存储 distance
		// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
		// 这个代码块用来计算 relevance
		std::priority_queue<MinDist> dist_record;
		{
			double temp_dist;
			for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
			{
				temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
				MinDist temp;
				temp.index = i;
				temp.dist = temp_dist;
				dist_record.push(temp);
			}
		}
		fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		std::vector<int> res;
		double min_dist = 0.0;
		// 统计寻找最终结果所用时间
		min_dist = FindKDiffMethodExp(plan_trees_hash, res, dist_record, 10);  // 新方法
		// FindKTimeExpOld(plan_trees_hash, res, dist_record, tempPlanNum);  // 旧方法
		fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
		fout_time << "*最小距离为: " << min_dist << '\n';
		fout_time << "查找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		fout_time << "最终找到结果的编号为：" << '\n';
		for(auto i: res){
			fout_time << i << '\t';
		}
		fout_time << '\n';

		// ARENA_result(res);  // 将结果保留到文件中
		fout_time << "*程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;
		// fout_time << "max_cost = " << max_cost << '\n';

		// 重置全局变量
		new_add = 0;
		max_cost = 0.0;
		fout_time.close();
	}
}

// 用于实验中将某个 id 的 plan 输出到文件
// 暂时只用于 ARENAAosExp 实验中，并且不需要输出 Plan 的详细信息
void ARENAOutputExp(std::size_t id, std::ofstream & fout)
{
	// 打开文件，并对其加锁
	// plan_trees_send.emplace_back(PlanTreeHash<CExpression>());
	// plan_trees_send.back().init_detailed(plan_trees_hash[id].inOriginal);

	nlohmann::json j;
	j["id"] = counter+1;
	j["cost"] = plan_trees_hash[id].root->data.cost; 

	if (counter == 0)  // best plan
	{
		j["s_dist"] = 0;
		j["c_dist"] = 0;
		j["cost_dist"] = 0; 
		j["relevance"] = 0;
	}
	else
	{
		j["s_dist"] = plan_trees_hash[id].structure_dist(plan_trees_hash[0]);
		j["c_dist"] = plan_trees_hash[id].content_dist(plan_trees_hash[0]);
		j["cost_dist"] = plan_trees_hash[id].cost_dist(plan_trees_hash[0]);
		j["relevance"] = plan_trees_hash[id].offset; 
	}

	// j["content"] = plan_trees_send.back().root->generate_json();  // 只有 content 内容需要使用详细信息，其它部分可以用原来 Plan 的信息
	std::string result= j.dump();
	if(fout.is_open())
	{
		fout << result << '\n';
	}
	counter++;
}

// 该函数与上面的 ARENAGTExp 函数类似，是正常的 B-AQP 算法
// 但是该函数在每选出一个新的 plan 之后，都会记录当前的最小距离，以对比 AOS 算法的效果
void ARENAAosExp()
{
	std::unordered_set<int> plan_join_diff;  // 记录那些与 QEP 只有 content 不同的 plan
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		for (int turn = 1; turn <= 2; turn++)  // 第一轮是没有 Aos 时的结果，第二轮时有 Aos 时的结果
		{
			std::size_t plan_num = turn == 1 ? gAosStart : plan_buffer_for_exp.size();
			if(plan_num == 0)
			{
				continue;
			}
			fout_time << "\n当前候选计划的数量为: " << plan_num << '\n';
			auto start = std::chrono::steady_clock::now();
			auto startAll = start;
			// 初始化的第零阶段，生成 plan_tree 结构
			{
				int j=0;
			for (std::size_t i = 0; i < plan_num; i++)
			{
				if(turn == 3)  // 第三次，只记录那些内容差异为 2 的 plan
				{
					if(i >= gAosStart && plan_join_diff.find(i) == plan_join_diff.end())  // 没有记录
					{
						continue;
					}
				}
				plan_trees_hash.emplace_back(PlanTreeHash<CExpression>());
				plan_trees_hash[j].init(plan_buffer_for_exp[i], 0);
				plan_trees_hash[j].init(nullptr, 1);

				// 设置 cost 的最大值
				if (plan_trees_hash[j].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[j].root->data.cost;
				}
				j++;
			}
			}
			fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
			for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
			{
				plan_trees_hash[i].init(NULL, 2);
			}
			fout_time << "取得由节点内容组成的字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 初始化第三阶段，计算自己与自己的树核
			for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
			{
				plan_trees_hash[i].init(NULL, 3);
			}
			fout_time << "计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// 小样本，此时将之前的 cost 读入
			if(gSampleThreshold <= 1000)
			{
				std::ifstream fout_temp("/tmp/aosCost");
				if(fout_temp.is_open())
				{
					fout_temp >> max_cost;
					fout_temp.close();
				}
			}
			else  // 大样本，将 cost 写入
			{
				std::ofstream fout_temp("/tmp/aosCost");
				if(fout_temp.is_open())
				{
					fout_temp << max_cost << ' ';
					fout_temp.close();
				}
			}
			// max_cost = std::pow(1.0, 14);  // 由于 AOS 策略进行采样，所以max_cost会有区别，为了消除其影响，此处设定最大的 cost
			max_cost -= plan_trees_hash[0].root->data.cost;


			if(gAosStart > 0 && turn == 2)
			{
				// 选出那些与 QEP 的内容差异只有2的 AP
				fout_time << "那些与 QEP 的结构差异仅有2的 plan id 为: ";
				for(std::size_t t=gAosStart; t < plan_buffer_for_exp.size(); t++)
				{
					if(plan_trees_hash[t].content_distG(plan_trees_hash[0]) == 2)
					{
						plan_join_diff.insert(t);
						fout_time << t << "  ";
					}
				}
				fout_time << "\n";
			}

			
			// 当使用新方法时，需要用优先队列的形式存储 distance
			// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
			std::priority_queue<MinDist> dist_record;
			{
				double temp_dist;
				for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
				{
					temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
					MinDist temp;
					temp.index = i;
					temp.dist = temp_dist;
					dist_record.push(temp);
				}
			}
			fout_time << "计算每个计划与最优计划距离的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			std::vector<int> res;
			std::vector<double> dist_res;
			// 统计寻找最终结果所用时间
			FindKDiffMethodExp(plan_trees_hash, res, dist_record, 10, &dist_res);  // 新方法

			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			fout_time << "查找k个目标值的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			fout_time << "最终找到结果的编号为：" << '\n';
			for(auto i: res){
				fout_time << i << '\t';
			}
			fout_time << '\n';
			fout_time << "每次添加新的 plan 之后的最小距离为: \n";
			for(auto d: dist_res)
			{
				fout_time << '*' << d << '\n';
			}

			// ARENA_result(res);  // 将结果保留到文件中
			fout_time << "程序整体的执行时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startAll)).count() << std::endl;

			if(turn == 2)
			{
				ARENA_result(res);
			}

			if(turn < 3)
			{
				plan_trees_hash.clear();
				max_cost = 0.0;
			}
			new_add = 0;

			// if(turn > 1)
			// {
			// 	for(auto i: res)
			// 	{
			// 		ARENAOutputExp(i, fout_time);
			// 	}
			// 	fout_time << "\n\n";

			// 	for(std::size_t i=0;i<plan_num;i++)
			// 	{
			// 		ARENAOutputExp(i, fout_time);
			// 	}
			// }
		}
		fout_time.close();
	}
}

// 用于寻找特定类型的 plan
// 当前寻找的是 与 QEP 结构相同，但是 Cost 差异却非常大的 plan
void ARENAFindTargetType()
{
	plan_trees_hash.reserve(plan_buffer.size());
	std::ofstream fout_time("/home/wang/timeRecord.txt");
	if (fout_time.is_open())
	{
		auto start = std::chrono::steady_clock::now();

		for (std::size_t i = 0; i < plan_buffer_for_exp.size(); i++)
		{
			plan_trees_hash.emplace_back(PlanTreeHash<CExpression>());
			plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);
			plan_trees_hash[i].init(nullptr, 1);

			// 设置 cost 的最大值
			if (plan_trees_hash[i].root->data.cost > max_cost)
			{
				max_cost = plan_trees_hash[i].root->data.cost;
			}
		}
		fout_time << "第一阶段(初始化PlanTreeNode结构体)初始计划的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第二阶段，生成节点内容组成的字符串，用于计算内容差异
		for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
		{
			plan_trees_hash[i].init(NULL, 2);
		}
		fout_time << "取得由节点内容组成的字符串的时间(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		// 初始化第三阶段，计算自己与自己的树核
		for(std::size_t i=0;i<plan_trees_hash.size();i++)  // init3
		{
			plan_trees_hash[i].init(NULL, 3);
		}
		fout_time << "计算与自己的距离的时间为(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
		start = std::chrono::steady_clock::now();

		auto maxCostRecord = max_cost;
		max_cost -= plan_trees_hash[0].root->data.cost;
		
		// 当使用新方法时，需要用优先队列的形式存储 distance
		// 初始化的最终阶段，计算每个 plan 与 best_plan 的结构差异、内容差异和cost差异
		{
			double temp_dist;
			for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)
			{
				temp_dist = plan_trees_hash[i].structure_dist(plan_trees_hash[0]);
				if(temp_dist == 0.0)
				{
					if((maxCostRecord - plan_trees_hash[i].get_cost())/maxCostRecord < 0.1)
					{
						fout_time << "*找到目标值: " << i;
					}
				}
			}
		}
		fout_time.close();
	}
}

//************************ARENA**************************/

// EOF
