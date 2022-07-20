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

#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP 5000	 // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP \
	20	// estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

using namespace gpopt;


//************************ARENA**************************/
#define ARENA_DEBUG
#define ARENA_PHYSICAL

// Sort the serialized tree
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


// struct MinDist is used for B-TIPS-HEAP
struct MinDist {
	int index;  // plan index
	std::size_t distNum;
	double dist;  // current minimum distance

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


// struct gtTree is used for record the information of a GroupTree
struct gtTree
{
	struct gtSingleChar
	{
		char inC;
		std::string inChild;

		gtSingleChar(char c): inC(c) {};
	};

    typedef std::unordered_map<std::string, std::vector<int>>::iterator pairIter;

    pairIter inIter;  // pointer to a specific element in the CTreeNode
    std::unordered_map<std::string, int> inSubtreeInfo;
	double inSelfKernel;

    gtTree(pairIter iter)
    {
        inIter = iter;
		inSelfKernel = 0.0;
    }

    // initialize, genearte the hash table of a GroupTree, used to calculated the structure difference
    void init()
    {
        std::vector<gtSingleChar> stack;
		std::string sortedStr = ARENASortSTree(inIter->first);
        for(char c: sortedStr)
        {
            switch(c)
            {
                case '[':
                    stack.emplace_back(gtSingleChar(c));
                    break;
                case ']':
                    std::string tempStr{'['};
                    tempStr += stack.back().inChild;
                    tempStr += ']';
                    stack.pop_back();

                    inSubtreeInfo[tempStr]++;
                    if(!stack.empty())
                    {
                        stack.back().inChild += tempStr;
                    }
                    break;
            }
        }

		std::unordered_map<std::string, int> * small;
		small = &inSubtreeInfo;

		// find the common subtrees
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


// Used to calculate TreeEditDistance
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
        resultT(matchT match = MATCH, int cost = 10000) : match(match), cost(cost), map() {}

        matchT match;
        int cost;
        std::vector<mapT> map;
    };

    struct nodeT {
        int id;
        std::wstring type;
        int leaf;
        bool key;
        int size;
    };

	    typedef std::vector<resultT> VR;
    typedef std::vector<VR> VVR;
    typedef std::vector<int> VI;
    typedef std::vector<VI> VVI;
    typedef std::vector<JSONValue*> VJ;
    typedef std::vector<bool> VB;
    typedef std::vector<nodeT> VN;

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

    nodeT makeNode(JSONValue* node, int leaf, bool key, int size)
    {
        nodeT n;
        n.type = node->AsObject().at(L"type")->AsString();
        n.id = static_cast<int>(node->AsObject().at(L"id")->AsNumber());
        n.id = static_cast<int>(node->AsObject().at(L"id")->AsNumber());
        n.leaf = leaf;
        n.key = key;
        n.size = size;

        return n;
    }

    int postorder(JSONValue* node, VN& post, bool isKey)
    {
        const JSONArray& children = node->AsObject().at(L"children")->AsArray();
        int ans = -1;
        int size = 1;
        for (std::size_t i = 0; i < children.size(); i++)
        {
            int tmp = postorder(children[i], post, i > 0);
            size += post[post.size() - 1].size;
            if (ans == -1)
                ans = tmp;
        }
        if (ans == -1)
            ans = post.size();

        post.push_back(makeNode(node, ans, isKey, size));
        return ans;
    }

    inline int match(const nodeT& a, const nodeT& b) {
        if (a.type != b.type) return 1;
        return 0;
    }

	    void matchTree(const VN& A, const VN& B, int a, int b)
    {
        dp2[0][0].cost = 0;
        int leafA = A[a].leaf, leafB = B[b].leaf;
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

	 // reset dp, dp2, postA, postB
    void reset()
    {
        VN().swap(postA);
        VN().swap(postB);
        VVR().swap(dp);
        VVR().swap(dp2);
    }

    double operator()(std::string& json_a, std::string& json_b, int a_size, int b_size)
    {
        JSONValue* A = readJSON(json_a);
        JSONValue* rootA = A->AsObject().at(L"root");
        JSONValue* B = readJSON(json_b);
        JSONValue* rootB = B->AsObject().at(L"root");

        int na = getSize(rootA), nb = getSize(rootB);

        dp = VVR(na, VR(nb));
        dp2 = VVR(na + 1, VR(nb + 1));

        postorder(rootA, postA, true);
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

		double res = (2.0 * ans) / (a_size + b_size + ans);
        return res;
    }
};


// These three functions are used in the ARENA system
void DealWithPlan();  // TIPS algorithm
void FindKRandom();  // Random
void FindKCost();  // Cost

// The following functions are used for the experiments in Section 7.2
void ARENATimeExp3();  // Exp1 in Section 7.2, TIPS algorithm
void ARENATimeExp3Random();  // Exp1 in Section 7.2, Random
void ARENATimeExp3Cost();  // Exp1 in Section 7.2, Cost

void ARENATimeExp4();  // Exp2 in Section 7.2, suffix Tree
void ARENATimeExp4Hash();  // Exp2 in Section 7.2, Hash Table
void ARENATimeExp4Old();  // Exp2 in Section 7.2, suffix Tree old

void ARENAGTExp();  // Exp 4 in Section 7.2

void ARENAAosExp();  // Exp 5 in Section 7.2

// assistant function
void ARENAOutputExp(std::size_t , std::ofstream &);
void readConfig(char mode='B');

#define ARENA_GTEXP


///**************************************************/
/// Some global variables that will be used
///**************************************************/
double gSWeight = 0.33;  // the weight of structure difference
double gCWeight = 0.33;  
double gCostWeight = 0.33;  
double gLambda = 0.5;  // trade-off between difference and relevance
char gMode = 'B';  // B-TIPS or I-TIPS
std::size_t gARENAK = 5;  // display 5 informative plans defaultly
bool gIsGTFilter = false;  // Whether to use the GFP filtering algorithm
int gGTNumThreshold = 50000;  // GFP threshold
double gGTFilterThreshold = 0.5;  // the plan whose difference is larger than this value will be filtered out
ULLONG gSampleThreshold = 10;  // LAPS threshold
bool gisSample = false;  // whether to use the LAPS 
std::size_t gAosStart = 0;
ULLONG gJoin = 0; // the number of join in SQL
bool gTEDFlag = false;  // whether to use TreeEditDistance

// auxiliary variable
double max_cost = 1.0;
std::string gConf;
std::string gResFile;
Sender* web_client = nullptr;
char edgePrefix[2]={'[', ']'};
int new_add = 0;
std::unordered_set<ULLONG> find_index;
int counter = 0;



///**************************************************/
///
/// function related to edit distance
///
///**************************************************/
inline int min(int x, int y) { return x <= y ? x : y; }

double editDist(std::vector<std::string*> word1, std::vector<std::string*> word2)
{
	std::size_t n = word1.size();
	std::size_t m = word2.size();

	if (n * m == 0)
	{
		return 1.0;
	}

	int D[100][100];
	for (std::size_t i = 0; i < n + 1; i++) {
		D[i][0] = i;
	}
	for (std::size_t j = 0; j < m + 1; j++) {
		D[0][j] = j;
	}

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


// Edit distance without normalization
int editDistG(std::vector<std::string*> word1, std::vector<std::string*> word2)
{
	std::size_t n = word1.size();
	std::size_t m = word2.size();

	if (n * m == 0)
	{
		return 1.0;
	}

	int D[100][100];
	for (std::size_t i = 0; i < n + 1; i++) {
		D[i][0] = i;
	}
	for (std::size_t j = 0; j < m + 1; j++) {
		D[0][j] = j;
	}

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
/// Code about Plan Tree
/// 
///**************************************************/
struct NodeData
{
	std::string name;
	std::string tag;  // node type
	std::string info;  // other information of node
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

	PlanTreeNode(NodeData data_in)
	{
		data = data_in;
	}

	~PlanTreeNode()
	{
		for (std::size_t i = 0; i < child.size(); ++i)
			delete child[i];
	}


	// add a child node
	void push_back(PlanTreeNode* c)
	{
		child.push_back(c);
	}

	void pop_back()
	{
		if (child.size() > 0)
		{
			child.pop_back();
		}
	}

	std::size_t size() const
	{
		return child.size();
	}

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

	// serialize the node
	std::string get_string()
	{
		if (!child.size())
			return std::string("[]");
		else
		{
			std::vector<std::string> child_string;
			child_string.reserve(child.size());
			for (std::size_t i = 0; i < child.size(); ++i)
			{
				child_string.push_back(child[i]->get_string());
			}
			
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
		if (child.size())
		{
			res["child"] = json::array();
			for (std::size_t i = 0; i < child.size(); ++i)
				res["child"].push_back(child[i]->generate_json());
		}
		return res;
	}

	// generate the string used for tree edit distance
	void ted_json(std::string & target, int * current_id)
	{
		target += "{\"id\":" + std::to_string(*current_id) + ",\"type\":\"" + data.name + "\", \"children\":[";
		(*current_id)++;
		for (std::size_t i=0;i<child.size();++i)
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
//	serialize a tree structure
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
struct PlanTreeHash  // use hash table to calculate the structure difference
{
	using json = nlohmann::json;

	// reference
	double offset;

	PlanTreeNode *root;
	bool init_flag;
	std::vector<std::string*> str_of_node;  // used to calculate the content difference(edit distance)
	
	std::string json_str;
	std::string str_serialize;
	int node_num;
	double cost_log;
	double s_dist_best, c_dist_best, cost_dist_best;  // difference with qep
	double self_kernel;  // used to normalize subtree kernel
	T * inOriginal;

	std::unordered_map<std::string, int> subTree;

	const char* get_name(T* exp)
	{
		if (NULL != exp)
		{

			const char * name_start = exp->Pop()->SzId();
			const char * base_str = "CPhysical";

			if(strlen(name_start) > 9)
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

	double get_cost(){
		return root->data.cost;
	}

	void insert_child_detailed(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";
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
				} else if(name_len > 4 && !strcmp(expression_name + name_len -4, "Join"))
				{
					cur->data.tag = "JOIN";
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child_detailed((*child_expression)[i], *cur);
				}

				const char * motionOp = "Motion";
				if (cur->data.name.size() > 6 && cur->data.name.compare(0, 6, motionOp) == 0 && cur->size() == 1 && 
					cur->data.cost - (*cur)[0]->data.cost < 0.1 * cur->data.cost )
				{
					parent.child[parent.size()-1] = (*cur)[0];
					
					cur->child[0] = nullptr; 
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

#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";
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

	void getNodeStr()
	{
		std::queue<PlanTreeNode*> node_queue;
		PlanTreeNode* current_node = NULL;
		node_queue.push(root);
		while (!node_queue.empty())
		{
			current_node = node_queue.front();

			str_of_node.push_back(&(current_node->data.name));
			for (std::size_t i = 0; i < current_node->child.size(); ++i)
			{
				node_queue.push(current_node->child[i]);
			}
			node_queue.pop();
		}
	}

	void init_detailed(T* plan)
	{
		if (NULL != plan)
		{
			inOriginal = plan;
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
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
				insert_child_detailed((*plan)[i], *root);
			}

			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )
			{
				auto temp = (*root)[0];
				root->child[0] = nullptr;
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

	void init(T* plan, int flag)
	{
		if (flag == 0 && NULL != plan)
		{
			inOriginal = plan;
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
			for (std::size_t i = 0; i < plan->Arity(); ++i)
			{
				insert_child((*plan)[i], *root);
			}
		}

		init_flag = true;
		if (flag == 1)
		{
			str_serialize = postOrder(root);
		}
		else if (flag == 2)
		{
			getNodeStr();
		}
		else if (flag == 3)
		{
			structure_dist_self();

			if(gTEDFlag)
			{
				TED_json();
			}
		}
		else if (flag == 4)
		{
			str_serialize = postOrderNone(root);
		}
		else if (flag == 5)
		{
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
		if (root->size() == 0) {
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
		if (root->size() == 0) {
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
	//****************** function about difference ********************************/
	double distance(PlanTreeHash& other)
	{
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);
		double cost = cost_dist(other);
		return (1-gLambda)*(offset + other.offset) / 2 +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	double distance_with_best(PlanTreeHash &other)
	{
		double s_dist = structure_dist(other);
		double c_dist = editDist(str_of_node, other.str_of_node);
		double cost = cost_dist(other);

		s_dist_best = s_dist;
		c_dist_best = c_dist;
		cost_dist_best = cost;
		double a[7]{1.0000,    1.0000,    1.0000,   -1.0000,   -2.0000,   -2.0000,    2.0000};
		offset = a[0]*s_dist+ a[1]*c_dist+a[2]*cost+a[3]*s_dist*c_dist+a[4]*s_dist*cost+a[5]*c_dist*cost+a[6]*s_dist*c_dist*cost;
		if(offset < 0.0)
		{
			offset = 0.0;
		}

		return (1-gLambda)*offset +gLambda * (s_dist * gSWeight + c_dist * gCWeight + cost * gCostWeight);
	}

	double structure_dist(PlanTreeHash& other)
	{
		int res = 0;
		
		std::unordered_map<std::string, int> * small, *large;
		if (subTree.size() < other.subTree.size()){
			small = &subTree;
			large = &(other.subTree);
		} else {
			small = &(other.subTree);
			large = &subTree;
		}

		for (auto iter=small->begin(); iter != small->end() ; iter++){
			const std::string & key = iter->first;
			auto iter2 = large->find(key);
			if (iter2 != large->end()) {
				res += iter->second * iter2->second;
			}
		}

		double temp  = sqrt(self_kernel * other.self_kernel);
		temp = (double)(res) / temp;

		if(!gTEDFlag)
		{
			return sqrt(1.0 - temp);
		}
		else
		{
			TreeEdit te;
			return te(json_str, other.json_str, node_num, other.node_num);
		}
	}

	double structure_dist(gtTree& other)
	{
		int res = 0;
		
		std::unordered_map<std::string, int> * small, *large;
		if (subTree.size() < other.inSubtreeInfo.size()){
			small = &subTree;
			large = &(other.inSubtreeInfo);
		} else {
			small = &(other.inSubtreeInfo);
			large = &subTree;
		}

		for (auto iter=small->begin(); iter != small->end() ; iter++){
			const std::string & key = iter->first;
			auto iter2 = large->find(key);
			if (iter2 != large->end()) {
				res += iter->second * iter2->second;
			}
		}

		double temp  = sqrt(self_kernel * other.inSelfKernel);
		temp = (double)(res) / temp;
		return sqrt(1.0 - temp);
	}

	void structure_dist_self()
	{
		int res = 0;
		
		std::unordered_map<std::string, int> * small;
		small = &subTree;

		for (auto iter=small->begin(); iter != small->end() ; iter++){
			res += iter->second * iter->second;
		}

		self_kernel = res;
	}

	double content_dist(PlanTreeHash& other)
	{
		return editDist(str_of_node, other.str_of_node);
	}

	int content_distG(PlanTreeHash& other)
	{
		return editDistG(str_of_node, other.str_of_node);
	}

	double cost_dist(PlanTreeHash& other)
	{
		double res = abs(root->data.cost - other.root->data.cost) / max_cost; 
		return res;
	}

	void TED_json()
	{
		json_str = "{\"root\":";
		node_num = 0;
		root->ted_json(json_str, &node_num);
		json_str += "}";
	}

	void write(std::ostream &out)
	{
		if (NULL != root)
			root->output(out);
	}

	void write_json(std::ostream &out)
	{
		if (NULL != root)
			root->output_json(out);
	}
};

// GFP strategy
void ARENAGTFilter(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep, std::unordered_map<int, CCost>* id2Cost);
// LAPS strategy
void ARENAAos(std::unordered_map<std::string, std::vector<int>> & groupTreePlus, std::unordered_set<int> & record, PlanTreeHash<CExpression>& qep);

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

		for (int j = 0; j < prefix; j++)
		{
			fout << ' ';
		}

		edge* e = m_pEdgeVector[i];
		for (int j = e->m_startIndex; j < e->m_endIndex; j++)
		{
			fout << s[j];
		}
		fout << '\n';

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

		if (m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix == '\0')
		{
			if (m_activate.m_pNode->m_pEdgeVector[index] == nullptr)
			{
				edge* e = new edge();
				node* tempNode = new node();
				e->m_pStartNode = m_activate.m_pNode;
				e->m_pEndNode = tempNode;
				e->m_startIndex = (int)i;
				e->m_endIndex = (int)(m_originalStr.size());
				m_activate.m_pNode->m_pEdgeVector[index] = e;
				m_remainder--;
			}
			else
			{
				m_activate.m_edgePrefix = c;
				m_activate.m_len = 1;
			}
		} 
	
		else if(m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix != '\0')
		{
			int subIndex = getIndex(m_activate.m_edgePrefix);
			edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
			if (m_activate.m_len >= e->size())
			{
				node* tempNode = e->m_pEndNode;
				if (tempNode->m_pEdgeVector[index] != nullptr)
				{
					m_activate.m_pNode = tempNode;
					m_activate.m_edgePrefix = c;
					m_activate.m_len = 1;
				}
				else
				{
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
			else
			{
				char subC = m_originalStr[e->m_startIndex + m_activate.m_len];
				if (c == subC)
				{
					m_activate.m_len++;
				}
				else
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
		else if (!m_activate.m_pNode->m_isRoot)
		{
			int subIndex = getIndex(m_activate.m_edgePrefix);
			edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
			if (m_activate.m_len >= e->size())
			{
				node* tempNode = e->m_pEndNode;
				if (tempNode->m_pEdgeVector[index] != nullptr)
				{
					m_activate.m_pNode = tempNode;
					m_activate.m_edgePrefix = c;
					m_activate.m_len = 1;
				}
				else
				{
					bool isNext = true;
					node* pre = nullptr;
					do
					{
						isNext = onePoint(i, pre);
					} while (m_remainder > 0 && isNext);
				}
			}
			else
			{
				char subC = m_originalStr[e->m_startIndex + m_activate.m_len];
				if (c == subC)
				{
					m_activate.m_len++;
				}
				else
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
		if (m_activate.m_pNode->m_pEdgeVector[index] == nullptr)
		{
			edge* e = new edge();
			node* tempNode = new node();
			e->m_pStartNode = m_activate.m_pNode;
			e->m_pEndNode = tempNode;
			e->m_startIndex = (int)currIndex;
			e->m_endIndex = (int)(m_originalStr.size());
			m_activate.m_pNode->m_pEdgeVector[index] = e;
			m_remainder--;
		}
		else
		{
			m_activate.m_edgePrefix = c;
			m_activate.m_len = 1;
		}
		return false;
	} 

	if(m_activate.m_pNode->m_isRoot && m_activate.m_edgePrefix != '\0')
	{
		int subIndex = getIndex(m_activate.m_edgePrefix);
		edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
		
		if (m_activate.m_len >= e->size())
		{
			node* tempNode = e->m_pEndNode;
			if (tempNode->m_pEdgeVector[index] != nullptr)
			{
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
			
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
			
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

			
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
			char subC = m_originalStr[e->m_startIndex + m_activate.m_len];
			if (c == subC)
			{
				m_activate.m_len++;
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

				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;

				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

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

	if (!m_activate.m_pNode->m_isRoot)
	{
		int subIndex = getIndex(m_activate.m_edgePrefix);
		edge* e = m_activate.m_pNode->m_pEdgeVector[subIndex];
		if (m_activate.m_len >= e->size())
		{
			node* tempNode = e->m_pEndNode;
			if (tempNode->m_pEdgeVector[index] != nullptr)
			{
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
		
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
		
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				m_remainder--;
				if (m_activate.m_pNode->m_pSuffixLink != nullptr)
				{
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
		{
			char subC = m_originalStr[e->m_startIndex + m_activate.m_len];
			if (c == subC)
			{
				m_activate.m_len++;
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
		
				newEdge = new edge();
				newEdge->m_startIndex = currIndex;
				newEdge->m_endIndex = m_originalStr.size();
				newEdge->m_pEndNode = new node();
				newEdge->m_pStartNode = e->m_pEndNode;
				e->m_pEndNode->m_pEdgeVector[index] = newEdge;
		
				if (preNode != nullptr)
				{
					preNode->m_pSuffixLink = e->m_pEndNode;
				}
				preNode = e->m_pEndNode;

				m_remainder--;
				if (m_activate.m_pNode->m_pSuffixLink != nullptr)
				{
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
	node* start = &m_root;
	std::size_t alreadyMatch = 0;
	std::size_t maxMatch = 0;
	
	for (std::size_t i = 0; i < s.size(); i++)
	{
		node* pre = start;
		std::size_t j = alreadyMatch;
		edge* e = start->m_pEdgeVector[getIndex(s[i+j])];
		while (j < maxMatch && j+e->size() < maxMatch)
		{
			node* temp = e->m_pEndNode;
			pre = temp;
			j += e->size();
			e = temp->m_pEdgeVector[getIndex(s[i + j])];
		}

		int k = 0;
		if (j == maxMatch)
		{
			while (e != nullptr && i + j < s.size() && s[i + j] == getChar(e, k))
			{
				j++;
				k++;
				if (k == e->size())
				{
					if (i + j == s.size())
					{
						break;
					}
					pre = e->m_pEndNode;
					e = findEdge(e, s[i + j]);
					k = 0;
				}
			}

		}
		maxMatch = j;
		if (k == 0)
		{
			matchingStatistic.push_back(std::make_pair(maxMatch, pre));
		}
		else
		{
			matchingStatistic.push_back(std::make_pair(maxMatch, e->m_pEndNode));
		}
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
struct PlanTreeExp  // use the suffix tree to calculate subtree kernel
{
	double offset;

	PlanTreeNode *root;
	suffixTreeNew *m_stn;
	bool init_flag;
	std::string str_serialize;
	int node_num;
	double s_dist_best, c_dist_best, cost_dist_best;
	double self_kernel;

	const char* get_name(T* exp)
	{
		if (NULL != exp)
		{

			const char * name_start = exp->Pop()->SzId();
			const char * base_str = "CPhysical";

			if(strlen(name_start) > 9)
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

	double get_cost(){
		return root->data.cost;
	}

	void insert_child(T* child_expression, PlanTreeNode& parent)
	{
		if (NULL != child_expression)
		{
			PlanTreeNode* cur = NULL;

#ifdef ARENA_PHYSICAL
			if (child_expression->Pop()->FPhysical())  // CExpression* -> Coperator* -> bool
			{
#endif
				const char * expression_name = get_name(child_expression);
				cur = new PlanTreeNode(NodeData(get_cost(child_expression), expression_name));
				parent.push_back(cur);
				int name_len = strlen(expression_name);

				if (name_len > 4 && !strcmp(expression_name + name_len - 4, "Scan"))
				{
					cur->data.tag = "SCAN";
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
				} else if(name_len > 4 && !strcmp(expression_name + name_len -4, "Join"))
				{
					cur->data.tag = "JOIN";
				}

				for (std::size_t i = 0; i < child_expression->Arity(); ++i)
				{
					insert_child((*child_expression)[i], *cur);
				}

				const char * motionOp = "Motion";
				if (cur->data.name.size() > 6 && cur->data.name.compare(0, 6, motionOp) == 0 && cur->size() == 1 && 
					cur->data.cost - (*cur)[0]->data.cost < 0.1 * cur->data.cost )
				{
					parent.child[parent.size()-1] = (*cur)[0];
					cur->child[0] = nullptr;
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


	void init(T* plan, int flag)
	{
		if (flag == 0 && NULL != plan)
		{
			root = new PlanTreeNode(NodeData(get_cost(plan), get_name(plan)));
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

			if (root->data.name.size() > 6 && root->data.name.compare(0, 6, "Motion") == 0 && root->size() == 1 && 
				root->data.cost - (*root)[0]->data.cost < 0.1 * root->data.cost )
			{
				auto temp = (*root)[0];
				root->child[0] = nullptr;
				delete root;
				root = temp;
			}
		}

		init_flag = true;
		if (flag == 1)
		{
			str_serialize = postOrder(root);
			m_stn = new suffixTreeNew();
			m_stn->init(str_serialize);
		}
	}

	std::string postOrder(PlanTreeNode* root)
	{
		if (root == nullptr) {
			return "";
		}

		std::string treeStr;
		if (root->size() == 0) {
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
};

///**************************************************/
/// some global variables
///**************************************************/
std::queue<CExpression *> plan_buffer;
std::vector<CExpression *> plan_buffer_for_exp; 
std::vector<PlanTreeHash<CExpression>> plan_trees_hash;
std::vector<PlanTreeHash<CExpression>> plan_trees_send;


///**************************************************/
/// send the result to web server
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

	j["content"] = plan_trees_send.back().root->generate_json();
	std::string result= j.dump();
    if(web_client->isStart())
    {
        web_client->send_result(result);
    }

	++counter;
}


// B-TIPS algorithm
template<class T>
void FindK(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record)
{
	res.push_back(0);  // the first element is QEP
	addResult(0);  // send plan

    // if the total number is smaller than target number
    if (plans.size() <= gARENAK)
    {
		for (std::size_t i = 1; i < plans.size(); ++i)
        {
			if (plans[i].root->data.cost != plans[0].root->data.cost)
			{
				res.push_back(i);
				addResult(i);
			}
        }
    }
    // find k informative plans
    else {
		std::vector<MinDist> tempRemoved;
		tempRemoved.reserve(plans.size() / 3);

		// iterate gARENAK times, select a valid maximum value each time
        for (std::size_t i = 0; i < gARENAK; ++i)
        {
			MinDist maxValue;
			maxValue.dist = -100;
			while(!dist_record.empty() && dist_record.top() > maxValue) {
				if (dist_record.top().distNum == i) { // if current value is valid
					tempRemoved.push_back(maxValue);
					maxValue = dist_record.top();
				} else {
					MinDist topElement = dist_record.top();
					for(std::size_t j=topElement.distNum+1; j<=i ; j++) {
						double dist = plans[topElement.index].distance(plans[res[j]]);
						topElement.distNum = j;
						if(dist < topElement.dist)
							topElement.dist = dist;
					}

					if (topElement > maxValue)
					{
						tempRemoved.push_back(maxValue);
						maxValue = topElement;
					} else {
						tempRemoved.push_back(topElement);
					}
				}

				dist_record.pop();
			}

			// update the heap
			res.push_back(maxValue.index);
			addResult(maxValue.index);
			for(auto & md : tempRemoved)
				dist_record.push(md);
			tempRemoved.clear();
        }
    }
}

void handler(int a) {
    a++;
}

void changeHandler(){
    struct sigaction newAct;
    newAct.sa_handler = handler;
    sigemptyset(&newAct.sa_mask);
    newAct.sa_flags = 0;

    sigaction(SIGUSR1, &newAct, nullptr);
}

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

void ARENAWrite(int fd, std::string & s)
{
	const char * res = s.c_str();
	write(fd, res, strlen(res));
}

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

// output the I-TIPS result
void IAQPOutputRes(int id)
{
	// open the file and lock it
	std::string fileName = "/tmp/" + gResFile;
	int fd = open(fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU|S_IRWXG);
	if(fd == -1)
	{
		return;
	}
	if(lockFile(fd, 1) != 0)
	{
		return;
	}

	std::string result;
	if(id == -1)
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

		j["content"] = plan_trees_send.back().root->generate_json();
		result= j.dump();
		counter++;
	}
	ARENAWrite(fd, result);
	write(fd, "\n", 1);
	result = std::to_string(getpid());
	ARENAWrite(fd, result);

	lockFile(fd, 0);
	close(fd);
}


// I-TIPS algorithm
template<class T>
void FindK_I(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record)
{
	changeHandler();

	res.push_back(0);  // the first element is the QEP
	IAQPOutputRes(0);
	unsigned int t;
	t = sleep(300);
	if (t == 0)
	{
		ARENATellFrontStop(getpid());
		return;
	}

	std::vector<MinDist> tempRemoved;
	tempRemoved.reserve(plans.size() / 3);

	for (std::size_t i = 0; i < plans.size(); ++i)
	{
		MinDist maxValue;
		maxValue.dist = -100;
		while(!dist_record.empty() && dist_record.top() > maxValue) {
			if (dist_record.top().distNum == i) {
				tempRemoved.push_back(maxValue);
				maxValue = dist_record.top();
			} else {
				MinDist topElement = dist_record.top();
				for(std::size_t j=topElement.distNum+1; j<=i ; j++) {
					double dist = plans[topElement.index].distance(plans[res[j]]);
					topElement.distNum = j;
					if(dist < topElement.dist)
						topElement.dist = dist;
				}

				if (topElement > maxValue)
				{
					tempRemoved.push_back(maxValue);
					maxValue = topElement;
				} else {
					tempRemoved.push_back(topElement);
				}
			}

			dist_record.pop();
		}

		res.push_back(maxValue.index);
		for(auto & md : tempRemoved)
			dist_record.push(md);
		tempRemoved.clear();

		IAQPOutputRes(maxValue.index);
		t = sleep(300);
		if (t != 0)
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
		t = sleep(60);
		if (t != 0)
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

// calculate the plan interestringness of a plan set
double ARENACalculateDist(std::vector<int> & res)
{
	plan_trees_hash.reserve(res.size());
	for(std::size_t i=0;i<plan_buffer_for_exp.size();i++)
	{
		double temp_cost = plan_buffer_for_exp[i]->Cost().Get();
		if (temp_cost> max_cost)
		{
			max_cost = temp_cost;
		}
	}

	for (std::size_t i = 0; i < res.size(); i++)
	{
		plan_trees_hash.push_back(PlanTreeHash<CExpression>());
		plan_trees_hash[i].init(plan_buffer_for_exp[res[i]], 0);
	}

	for(std::size_t i=0;i<plan_trees_hash.size();i++)
	{
		plan_trees_hash[i].init(NULL, 1);
	}

	for (std::size_t i = 0; i < plan_trees_hash.size(); i++)
	{
		plan_trees_hash[i].init(NULL, 2);
	}

	for(std::size_t i=0;i<plan_trees_hash.size();i++)
	{
		plan_trees_hash[i].init(NULL, 3);
	}
	max_cost -= plan_trees_hash[0].root->data.cost;

	
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

// random algorithm in Exp1
template<class T>
double FindKRandomExp(std::vector<T>& plans, std::vector<int>& res, std::size_t k)
{
	std::default_random_engine rand(time(NULL));
    std::uniform_int_distribution<int> dis(1, plans.size()-1);
	double distance = -100;
	for(int iter=0;iter<30;iter++)  // Iterate 30 times
	{
		std::unordered_set<int> res_set;
		res_set.insert(0);
		std::size_t num = 0;
		while(num < k)  // generate a random result
		{
			int id = dis(rand) % plans.size();
			if(res_set.find(id) == res_set.end())
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

// Cost algorithm in Exp1
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
	res.push_back(0);

	std::vector<MinDist> tempRemoved;
	tempRemoved.reserve(plans.size() / 3);

	for (std::size_t i = 0; i < 100; ++i)
	{
		MinDist maxValue;
		maxValue.dist = -100;
		while(!dist_record.empty() && dist_record.top() > maxValue) {
			if (dist_record.top().distNum == i) {
				tempRemoved.push_back(maxValue);
				maxValue = dist_record.top();
			} else {
				MinDist topElement = dist_record.top();
				for(std::size_t j=topElement.distNum+1; j<=i ; j++) {
					double dist = plans[topElement.index].distance(plans[res[j]]);
					topElement.distNum = j;
					if(dist < topElement.dist)
						topElement.dist = dist;
				}

				if (topElement > maxValue)
				{
					tempRemoved.push_back(maxValue);
					maxValue = topElement;
				} else {
					tempRemoved.push_back(topElement);
				}
			}

			dist_record.pop();
		}

		res.push_back(maxValue.index);
		for(auto & md : tempRemoved)
			dist_record.push(md);
		tempRemoved.clear();
	}
}

// B-TIPS-BASIC
template<class T>
void FindKTimeExpOld(std::vector<T>& plans, std::vector<int>& res, std::unordered_map<int, double> & dist_record, std::size_t k=0)
{
    std::vector<char> falive(plans.size(), 'a');
    res.push_back(0);
	if(k == 0)
	{
		k = 10;
	}

	for (std::size_t i = 0; i < k; ++i)
	{
		int index = -1;
		double max_min = -DBL_MIN;
		double dist;

		// the first iteration
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
					dist = plans[new_add].distance(plans[j]);
					if (dist < dist_record[j])
					{
						dist_record[j] = dist;
					}
					else
					{
						dist = dist_record[j];
					}

					if (dist > max_min)
					{
						index = j;
						max_min = dist;
					}
				}
			}
		}

		res.push_back(index);
		falive[index] = 0;
		new_add = index;
	}
}

// B-TIPS-HEAP with plan interestingness
template<class T>
double FindKDiffMethodExp(std::vector<T>& plans, std::vector<int>& res, std::priority_queue<MinDist> &dist_record, std::size_t k, std::vector<double> * dist_list = nullptr)
{
	res.push_back(0);

	std::vector<MinDist> tempRemoved;
	tempRemoved.reserve(plans.size() / 3);
	double res_dist = 0.0;

	for (std::size_t i = 0; i < k; ++i)
	{
		MinDist maxValue;
		maxValue.dist = -100;
		while(!dist_record.empty() && dist_record.top() > maxValue) {
			if (dist_record.top().distNum == i) {
				tempRemoved.push_back(maxValue);
				maxValue = dist_record.top();
			} else {
				MinDist topElement = dist_record.top();
				for(std::size_t j=topElement.distNum+1; j<=i ; j++) {
					double dist = plans[topElement.index].distance(plans[res[j]]);
					topElement.distNum = j;
					if(dist < topElement.dist)
						topElement.dist = dist;
				}

				if (topElement > maxValue) 
				{
					tempRemoved.push_back(maxValue);
					maxValue = topElement;
				} else {
					tempRemoved.push_back(topElement);
				}
			}

			dist_record.pop();
		}

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

		/************************* ARENA *************************/
		auto start = std::chrono::steady_clock::now();
		std::ofstream fout_time("/tmp/treeNode");
		m_pmemo->BuildTreeMap(poc);
		optimizer_config->GetEnumeratorCfg()->SetPlanSpaceSize(
			m_pmemo->Pmemotmap()->UllCount());
		fout_time << "time of generating Group Frost is(ms) : " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

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
				 (ULONG)(m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory jf(m_mp, ulJobs); 
	CScheduler sched(m_mp, ulJobs); 

	CSchedulerContext sc;
	sc.Init(m_mp, &jf, &sched, this);

	const ULONG ulSearchStages = m_search_stage_array->Size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// optimize root group
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_mp) COptimizationContext(
			m_mp, PgroupRoot(), m_pqc->Prpp(),
			GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(
				m_mp)),	 // pass empty required relational properties initially
			GPOS_NEW(m_mp)
				IStatisticsArray(m_mp),	 // pass empty stats context initially
			m_ulCurrSearchStage);

		// schedule main optimization job
		ScheduleMainJob(&sc, poc);

		// run optimization job
		CScheduler::Run(&sc);

		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan = m_pmemo->PexprExtractPlan(
			m_mp, m_pmemo->PgroupRoot(), m_pqc->Prpp(),
			m_search_stage_array->Size());
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
	// ULLONG ullCount = Pmemotmap()->UllCount();
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
	} while( find_index.find(plan_id) != find_index.end());
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
	auto time_start = std::chrono::steady_clock::now();
	auto all_start = time_start;
	std::ofstream fout_time("/tmp/time_record");
	std::ofstream fout_plan("/tmp/Plans");

	readConfig();
	// If /tmp/gtArg exists, the parameters about LAPS and GFP will be read from it, used for experiment
	std::ifstream fin_gt("/tmp/gtArg");
	if(fin_gt.is_open())
	{
		// read the first line, the format is : GFP threshold, difference threshold, LAPS threshold
		fin_gt >> gGTNumThreshold;
		fin_gt >> gGTFilterThreshold;
		fin_gt >> gSampleThreshold;
		fin_gt.close();
	}

	fout_plan << "The configuration parameters are as follows: \n";
	fout_plan << "plan number: " << gARENAK << '\n';
	fout_plan << "s_weight: " << gSWeight << '\n';
	fout_plan << "c_weight: " << gCWeight << '\n';
	fout_plan << "cost_weight: " << gCostWeight << '\n';
	fout_plan << "lambda: " << gLambda << '\n';
	fout_plan << "resultFileName: " << gResFile << '\n';
	fout_plan << "GFP threshold: " << gGTNumThreshold << '\n';
	fout_plan << "distance threshold: " << gGTFilterThreshold << '\n';
	fout_plan << "LAPS threshold: " << gSampleThreshold << '\n';
	if(gTEDFlag)
		fout_plan << "use Tree Edit Flag to calculate structure difference.\n";
	else
		fout_plan << "use subtree kernel to calculate structure difference.\n";

	//************************ARENA END**************************/
	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	GPOS_ASSERT(NULL != optimizer_config);

	CEnumeratorConfig *pec = optimizer_config->GetEnumeratorCfg();

	pec->ClearSamples();

	ULLONG ullCount = Pmemotmap()->UllCount();

	//************************ARENA**************************/
	gIsGTFilter = ullCount >= (ULLONG)gGTNumThreshold;
	gisSample = gJoin >= gSampleThreshold;
	if(gisSample)
	{
		gIsGTFilter = false;
	}
	std::unordered_set<int> filteredId;  // the plans to be filtered
	//***********************ARENA END***************************/

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
	std::vector<std::string>& groupTree = m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTree;
	fout_plan << "the number of GroupTree is: " << groupTree.size() << std::endl;
	std::unordered_map<std::string, std::vector<int>>& groupTreePlus = m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus;
#ifdef ARENA_COSTFT
	std::unordered_map<int, CCost> & id2Cost = m_pmemo->Pmemotmap()->ProotNode()->ARENA_id2Cost;
	fout_plan << std::showpoint;
#endif
	fout_plan << "the number of GroupTree in ARENA_groupTreePlus is: " << groupTreePlus.size() << '\n';

	// record the QEP
	plan_buffer.push(pexpr);
	plan_buffer_for_exp.push_back(pexpr);
	{
		PlanTreeHash<CExpression> tempTree;
		tempTree.init(pexpr);

		// if use the GFP strategy
		if(gIsGTFilter)
		{
			fout_plan << "use GFP\n";
#ifdef ARENA_COSTFT
			ARENAGTFilter(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree, &id2Cost);
#else
			ARENAGTFilter(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree, nullptr);
#endif
			fout_plan << "the plans need to be filtered out:  ";
			for(auto tempIter=filteredId.begin(); tempIter != filteredId.end(); tempIter++)
			{
				fout_plan << *tempIter << ' ';
			}
			fout_plan << '\n';
		}
		// if use the LAPS
		if(gisSample)
		{
			fout_plan << "use LAPS\n";
			ullTargetSamples = 10000;
			ARENAAos(m_pmemo->Pmemotmap()->ProotNode()->ARENA_groupTreePlus, filteredId, tempTree);

			for(auto tempIter=filteredId.begin(); tempIter != filteredId.end(); tempIter++)
			{
				fout_plan << *tempIter << ' ';
			}
			fout_plan << '\n';
		}

		fout_plan << tempTree.root->generate_json();
		fout_plan << "\n";
	}
	//************************ARENA END**************************/

	// generate randomized seed using local time
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL /*timezone*/);
	ULONG seed = CombineHashes((ULONG) tv.tv_sec, (ULONG) tv.tv_usec);

	// set maximum number of iterations based number of samples
	// we use maximum iteration to prevent infinite looping below
	ULLONG ull = 0;

	// std::vector<ULLONG> plan_id_list;
	// {
	// 	plan_id_list.reserve(40000);
	// 	std::ifstream fin_temp("/tmp/plan_id.txt");
	// 	if(fin_temp.is_open())
	// 	{
	// 		fout_plan << "成功打开 id 文件\n";
	// 		ULLONG temp_id = 0;
	// 		for(int i=0;i<40000;i++)
	// 		{
	// 			fin_temp >> temp_id;
	// 			plan_id_list.push_back(temp_id);
	// 		}
	// 		fin_temp.close();
	// 	}
	// }

	while (ull < ullTargetSamples)
	{
		// generate id of plan to be extracted
		ULLONG plan_id = ull;
		
		if(gisSample)  // if use the LAPS strategy
		{
			plan_id = UllRandomPlanId(&seed);
			// plan_id = plan_id_list[(int)plan_id];
		}
		fout_plan << "plan id: " << plan_id << '\n';

		if(gIsGTFilter && filteredId.find(plan_id) != filteredId.end())  // if use the GFP strategy
		{
			fout_plan << "filter out plan: " << plan_id << '\n';
			ull++;
			continue;
		}

		pexpr = NULL;
		
		if (FValidPlanSample(pec, plan_id, &pexpr))
		{
			plan_buffer.push(pexpr);
			plan_buffer_for_exp.push_back(pexpr);
			{
				PlanTreeHash<CExpression> tempTree;
				tempTree.init(pexpr);
				fout_plan << '$' << tempTree.root->generate_json();
				fout_plan << "\n";
			}
			pexpr = NULL;
		}
		else
		{
#ifdef ARENA_DEBUG
			fout_plan << "# FValidPlanSample fail\n";
#endif
		}

		ull++;
	}
	
	// if use the LAPS strategy, add the plans selected by LAPS to random plans
	if(gisSample)
	{
		gAosStart = plan_buffer_for_exp.size();
		for(auto id: filteredId)
		{
			ULLONG plan_id = (ULLONG)id;
			fout_plan << "plan id: " << plan_id << '\n';

			pexpr = NULL;
			
			if (FValidPlanSample(pec, plan_id, &pexpr))
			{
				plan_buffer.push(pexpr);
				plan_buffer_for_exp.push_back(pexpr);
				{
					PlanTreeHash<CExpression> tempTree;
					tempTree.init(pexpr);
					fout_plan << '$' << tempTree.root->generate_json();
					fout_plan << "\n";
				}
				pexpr = NULL;
			}
			else
			{
				fout_plan << "# FValidPlanSample fail\n";
			}
		}
	}

	fout_plan << "ull: " << ull << "\tullTargetSamples: " << ullTargetSamples << std::endl;
	fout_plan << "the number of valid plans is: " << plan_buffer.size() << '\n';
	if (gResFile.size() == 0 || gResFile.compare("###") == 0){
		fout_plan << "# Experiment \n";
	}
	fout_plan.close();

	//************************ARENA**************************/
	auto time_end = std::chrono::steady_clock::now();
	if (fout_time.is_open())
	{
		fout_time << "The sampling time is: " << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count() << "ms\n";
	}
	time_start = std::chrono::steady_clock::now();

	// Normal ARENA system or experiment
	if (gResFile.size() > 0 && gResFile.compare("###") != 0)
	{
		fout_time << "normal ARENA system\n";
		DealWithPlan();
	}
	else
	{
		fout_time << "experiment\n";
		// DealWithPlan();
		// Adjust the experiments that need to be done

		/* Exp1 */
		// ARENATimeExp3();
		// ARENATimeExp3Random();
		// ARENATimeExp3Cost();

		/* Exp2 */
		ARENATimeExp4();
		// ARENATimeExp4Hash();

		/* Exp4 */
		// ARENAGTExp();

		ARENAAosExp();
	}

	time_end = std::chrono::steady_clock::now();
	if (fout_time.is_open())
	{
		fout_time << "the time of TIPS is :" << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start)).count() << "ms\n";
		fout_time << "total time: " << (std::chrono::duration_cast<std::chrono::milliseconds>(time_end - all_start)).count() << '\n';
	}
	fout_time.close();
	//************************ARENA END**************************/
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
size_t writeToConf(char *ptr, size_t size, size_t nmemb, void *userdata){
    char buffer[4096];
    for(size_t i=0;i<nmemb;i++) {
        buffer[i] = ptr[i];
    }
    buffer[nmemb] = '\0';
    gConf += buffer;

	if(userdata != nullptr){
		size++;
		size--;
	}
    return nmemb;
}

// get configure information from web server
bool arenaGetConfig(char mode) {
	gConf = ""; 
	curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL * handle = curl_easy_init();
	CURLcode res;

    if (handle == nullptr){
        std::exit(1);
    } else {
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


// read the configure information of  ARENA system
void readConfig(char mode)
{
	if(!arenaGetConfig(mode)){  // if wrong, exit
		return;
	}
	gConf = gConf.substr(1);
	
	size_t  index = 0;
	size_t  pre = 0;
	int i= 1;
	for(index = gConf.find(';', pre); index != std::string::npos; i++)
	{
		std::string s = gConf.substr(pre, index-pre);
		pre = index+1;
		index = gConf.find(';', pre);

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
	index = gConf.find('"', pre);
	gResFile = gConf.substr(pre, index-pre);
}

// output the results to a file
void ARENA_result(std::vector<int> & res)
{
	std::string filename = "/tmp/";
	std::ofstream fout(filename+"ARENA_result");
	if(fout.is_open())
	{
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

			fout << j;
			fout << ",\n";
		}
		fout.seekp(-1, std::ios::cur);
		fout << "]";
		fout.close();
	}
	// reset the global variables
	std::vector<PlanTreeHash<CExpression>> ().swap(plan_trees_hash);
	find_index.clear();
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


	// 重置全局变量, plan_trees_hash
	delete web_client;
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
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
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
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
	find_index.clear();
	new_add = 0;
	max_cost = 0.0;
	counter = 0;
	gARENAK = 5;
}

// get a file name to output the execution information
std::string getLogFileName()
{
	std::string res("/home/");

	char * userName = nullptr;
	userName = getlogin();  // get the user name
	if(userName != nullptr)
	{
		res += userName;
		res += "/timeRecord.txt";
	}
	else
	{
		res = "/tmp/timeRecord.txt";
	}

	return res;
}

// test the effectiveness and efficiency of TIPS
void ARENATimeExp3()
{
	plan_trees_hash.reserve(plan_buffer.size());

	std::string logFile = getLogFileName();  // used to output execution informative
	std::ofstream fout_time(logFile);
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*the number of informative plans is: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();  // record time
			for (std::size_t i = 0; i < plan_buffer_for_exp.size(); i++)
			{
				plan_trees_hash.push_back(PlanTreeHash<CExpression>());
				plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);
				plan_trees_hash[i].init(NULL, 1);
				plan_trees_hash[i].init(NULL, 2);
				plan_trees_hash[i].init(NULL, 3);

				// find the max cost
				if (plan_trees_hash[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[i].root->data.cost;
				}
			}
			max_cost -= plan_trees_hash[0].root->data.cost;
			

			// if we use the B-TIPS-Basic method, we use a hash table to record the distance
            std::unordered_map<int, double> dist_record;
			for(std::size_t i=1;i<plan_trees_hash.size();++i)  // this 'for' statement is used to compute relevance
			{
				double temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
				dist_record[i] = temp_dist;
			}

			// if we use the B-TIPS-Heap method, we use a Max-heap to record the distance
			// std::priority_queue<MinDist> dist_record;
			// {
			// 	double temp_dist;
			// 	for (std::size_t i = 1; i < plan_trees_hash.size(); ++i)  // to calculate relevance
			// 	{
			// 		temp_dist = plan_trees_hash[i].distance_with_best(plan_trees_hash[0]);
			// 		MinDist temp;
			// 		temp.index = i;
			// 		temp.dist = temp_dist;
			// 		dist_record.push(temp);
			// 	}
			// }
			
			std::vector<int> res;  // record the result plan id
			double min_dist = 0.0;  // record the plan interestingness

			// min_dist = FindKDiffMethodExp(plan_trees_hash, res, dist_record, tempPlanNum);  // B-TIPS-H
			FindKTimeExpOld(plan_trees_hash, res, dist_record, tempPlanNum);  // B-TIPS-B

			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			fout_time << "*plan interestingness: " << min_dist << '\n';
			fout_time << "*total time(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

			// reset the global variables
			plan_trees_hash.clear();
			new_add = 0;
			max_cost = 0.0;
		}
		fout_time.close();
	}
}

// test the effectiveness and efficiency of Random
void ARENATimeExp3Random()
{
	std::string logFile = getLogFileName();
	std::ofstream fout_time(logFile);
	std::vector<int> res;  // record the result plan id

	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*the number of informative plan is: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			double min_dist = FindKRandomExp(plan_buffer_for_exp, res, tempPlanNum);
			fout_time << "*plan interestingness: " << min_dist << '\n';
			fout_time << "*total time(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

			// reset the global variables
			plan_trees_hash.clear();
			res.clear();
			max_cost = 0.0;
		}
		fout_time.close();
	}
}

// test the effectiveness and efficiency of Cost
void ARENATimeExp3Cost()
{
	std::string logFile = getLogFileName();
	std::ofstream fout_time(logFile);
	std::vector<int> res;
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 10; tempPlanNum < 60; tempPlanNum += 10)
		{
			fout_time << "\n*the number of informative plan is: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			FindKCostExp(plan_buffer_for_exp, res, tempPlanNum);
			fout_time << std::setiosflags(std::ios::fixed)<<std::setprecision(3);
			fout_time << "*plan interestingness: " << ARENACalculateDist(res) << '\n';
			fout_time << "*total time(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;

			plan_trees_hash.clear();
			res.clear();
			max_cost = 0.0;
		}
		fout_time.close();
	}
}

// test the time-consuming of suffix tree in different stages
void ARENATimeExp4()
{
	std::vector<PlanTreeExp<CExpression>> plan_trees_exp;
	plan_trees_exp.reserve(plan_buffer.size());

	std::string logFile = getLogFileName();
	std::ofstream fout_time(logFile);
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n*the number of informative plans: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			// construct the plan tree
			for (std::size_t i = 0; i < tempPlanNum; i++)
			{
				plan_trees_exp.push_back(PlanTreeExp<CExpression>());
				plan_trees_exp[i].init(plan_buffer_for_exp[i], 0);

				// find the max cost
				if (plan_trees_exp[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_exp[i].root->data.cost;
				}
			}
			fout_time << "the time to initialize the plan tree(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// construct the sufffix tree
			for(std::size_t i=0;i<plan_trees_exp.size();i++)
			{
				plan_trees_exp[i].init(NULL, 1);
			}

			// calculate the number of leaves, which is also the initialization phase of the suffix tree
			for(std::size_t i=0;i<plan_trees_exp.size();i++)
			{
				plan_trees_exp[i].m_stn->calculateNum();
			}
			fout_time << "*the time to initialize suffix tree(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();
			
			
			// calculate the subtree kernel
			for(std::size_t i=0;i<plan_trees_exp.size();i++)
			{
				plan_trees_exp[i].m_stn->distance(*(plan_trees_exp[i].m_stn));
			}
			fout_time << "*calculate the subtree kernel(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			plan_trees_exp.clear();
		}
		fout_time.close();
	}
}

// test the time-consuming of hash table in different stages
void ARENATimeExp4Hash()
{
	plan_trees_hash.reserve(plan_buffer.size());

	std::string logFile = getLogFileName();
	std::ofstream fout_time(logFile);
	if (fout_time.is_open())
	{
		for (std::size_t tempPlanNum = 1000; tempPlanNum < 11000; tempPlanNum += 1000)
		{
			fout_time << "\n*the number of informative plans: " << tempPlanNum << '\n';
			auto start = std::chrono::steady_clock::now();
			// construct the plan tree
			for (std::size_t i = 0; i < tempPlanNum; i++)
			{
				plan_trees_hash.push_back(PlanTreeHash<CExpression>());
				plan_trees_hash[i].init(plan_buffer_for_exp[i], 0);

				if (plan_trees_hash[i].root->data.cost > max_cost)
				{
					max_cost = plan_trees_hash[i].root->data.cost;
				}
			}
			fout_time << "the time to initialize the plan tree(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// construct the hash table
			for(std::size_t i=0;i<plan_trees_hash.size();i++)
			{
				plan_trees_hash[i].init(NULL, 1);
			}
			fout_time << "*the time to initialize the hash table(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			// calculate the subtree kernel
			for(std::size_t i=0;i<plan_trees_hash.size();i++)
			{
				plan_trees_hash[i].init(NULL, 3);
			}
			fout_time << "*calculate the subtree kernel(ms): " << (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() << std::endl;
			start = std::chrono::steady_clock::now();

			max_cost -= plan_trees_hash[0].root->data.cost;

			// reset the global variables
			plan_trees_hash.clear();
			new_add = 0;
			max_cost = 0.0;
		}

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
		// for(std::size_t i=1;i<plan_trees_hash.size();++i)
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

//************************ARENA**************************/

// EOF
