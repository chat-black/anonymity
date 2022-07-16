#ifndef ARENA_EDGE_H
#define ARENA_EDGE_H
#define ARENA_EXP

#ifdef ARENA_EXP
#include <string>
#include <iostream>
#include <vector>

struct edge;  // 前向声明

int getIndex(char c);
edge* findEdge(edge*, char);

struct node
{
	edge* m_pEdgeVector[3];
	node* m_pSuffixLink;
	bool m_isRoot;
	int m_num;
	int m_length;

	node()
	{
		m_num = 0;
		m_length = 3;
		m_pSuffixLink = nullptr;
		m_isRoot = false;
		for (int i = 0; i < m_length; i++)
		{
			m_pEdgeVector[i] = nullptr;
		}
	}

	~node();

	// 进行先序遍历，打印树型结构
	void preOrder(int prefix, std::string& s, std::ostream& fout);
	int calculateNum();
};

struct edge {
	node* m_pStartNode;
	node* m_pEndNode;
	int m_startIndex;
	int m_endIndex;  // index 是左闭右开区间

	edge()
	{
		m_pStartNode = nullptr;
		m_pEndNode = nullptr;
		m_startIndex = -1;
		m_endIndex = -1;
	}

	~edge()
	{
		delete m_pEndNode;
	}

	int size()
	{
		return m_endIndex - m_startIndex;
	}
};

struct ActivatePoint
{
	node* m_pNode;
	char m_edgePrefix;
	int m_len;
};

struct suffixTreeNew
{
	std::string m_originalStr;
	std::vector<std::pair<int, node*>> matchingStatistic;
	std::vector<std::pair<int,int>> m_treeRecord;
	ActivatePoint m_activate;
	node m_root;
	int m_remainder;

	suffixTreeNew()
	{
		m_remainder = 0;
		m_root.m_isRoot = true;
		m_activate.m_pNode = &m_root;
		m_activate.m_edgePrefix = '\0';
		m_activate.m_len = 0;
	}

	void init(std::string& s)
	{
		m_originalStr = s;
		construct();
	}
	
	char getChar(edge* e, int index)
	{
		if (e != nullptr)
		{
			return m_originalStr[e->m_startIndex + index];
		}
		else
		{
			return '\0';
		}
	}

	void generateMS(std::string& s);
	void generateSS();  // 生成字符串的统计信息

	// 计算每个节点中字符串出现的次数
	void calculateNum()
	{
		m_root.calculateNum();
	}

	double distance(suffixTreeNew& other);

	// 用于打印后缀树的格式
	friend std::ostream& operator<<(std::ostream& fout, suffixTreeNew&stn) {
		stn.m_root.preOrder(0, stn.m_originalStr, fout);
		return fout;
	}

private:
	void construct();
	bool onePoint(std::size_t currIndex, node* preNode);
};

#endif

struct suffixTreeContext;
struct Edge
{
	// Edges are hash-searched on the basis of startNode.
	// startNode = -1 means that this edge is not valid yet.
	int startNode;  //开始节点
	int endNode;  // 结束节点
	int startLabelIndex;  // 这两个 index 用于确定字符串的内容
	int endLabelIndex;

	// Constructors.
	Edge() : startNode(-1),startLabelIndex(0), endLabelIndex(0) {};

	// everytime a new edge is created, a new node is also created and thus the endNode is declared as below.
	// start: startNode first:startLabelIndex  last:endLabelIndex  endNode:context.noofNodes++
	Edge(int start, int first, int last, suffixTreeContext* context);

	std::size_t size() const
	{
		// return startNode == -1 ? 0 : endLabelIndex - startLabelIndex + 1;
		return endLabelIndex - startLabelIndex + 1;  // 这条边上的字符数量
	}

	// 返回当前边是否是有效边
	bool valid() const
	{
		return startNode != -1;
	}

	// Destructor
	~Edge() { };
};



#endif