//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMap.h
//
//	@doc:
//		Map of tree components to count, rank, and unrank abstract trees;
//
//		For description of algorithm, see also:
//
//			F. Waas, C. Galindo-Legaria, "Counting, Enumerating, and
//			Sampling of Execution Plans in a Cost-Based Query Optimizer",
//			ACM SIGMOD, 2000
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMap_H
#define GPOPT_CTreeMap_H

#define ARENA_GTFILTER
// #define ARENA_COSTFT

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpopt/cost/CCost.h"
#include <fstream>
#include <vector>
#include <unordered_set>
#include <unordered_map>


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CTreeMap
//
//	@doc:
//		Lookup table for counting/unranking of trees;
//
//		Enables client to associate objects of type T (e.g. CGroupExpression)
//		with a topology solely(单独地) given by edges between the object. The
//		unranking utilizes client provided function and generates results of
//		type R (e.g., CExpression);
//		U is a global context accessible to recursive rehydrate calls.
//		Pointers to objects of type U are passed through PrUnrank calls to the
//		rehydrate function of type PrFn.
//		T 每个节点原本的类型
//		R 利用 unrank 函数返回 R 类型的结果
//		U 递归调用的过程中可以全局访问的变量。
//
//---------------------------------------------------------------------------
template <class T, class R, class U, ULONG (*HashFn)(const T *),
		  BOOL (*EqFn)(const T *, const T *)>
class CTreeMap
{
	// array of source pointers (sources owned by 3rd party)
	typedef CDynamicPtrArray<T, CleanupNULL> DrgPt;

	// array of result pointers (results owned by the tree we unrank)
	typedef CDynamicPtrArray<R, CleanupRelease<R> > DrgPr;

	// generic rehydrate function
	typedef R *(*PrFn)(CMemoryPool *, T *, DrgPr *, U *);

private:
	// fwd declaration
	class CTreeNode;

	// arrays of internal nodes
	typedef CDynamicPtrArray<CTreeNode, CleanupNULL> CTreeNodeArray;
	typedef CDynamicPtrArray<CTreeNodeArray, CleanupRelease> CTreeNode2dArray;

	//---------------------------------------------------------------------------
	//	@class:
	//		STreeLink
	//
	//	@doc:
	//		Internal structure to monitor tree links for duplicate detection
	//		purposes
	//
	//---------------------------------------------------------------------------
	struct STreeLink
	{
	private:
		// parent node
		const T *m_ptParent;

		// child index
		ULONG m_ulChildIndex;

		// child node
		const T *m_ptChild;

	public:
		// ctor
		STreeLink(const T *ptParent, ULONG child_index, const T *ptChild)
			: m_ptParent(ptParent),
			  m_ulChildIndex(child_index),
			  m_ptChild(ptChild)
		{
			GPOS_ASSERT(NULL != ptParent);
			GPOS_ASSERT(NULL != ptChild);
		}

		// dtor
		virtual ~STreeLink()
		{
		}

		// hash function
		static ULONG
		HashValue(const STreeLink *ptlink)
		{
			ULONG ulHashParent = HashFn(ptlink->m_ptParent);
			ULONG ulHashChild = HashFn(ptlink->m_ptChild);
			ULONG ulHashChildIndex =
				gpos::HashValue<ULONG>(&ptlink->m_ulChildIndex);

			return CombineHashes(ulHashParent,
								 CombineHashes(ulHashChild, ulHashChildIndex));
		}

		// equality function
		static BOOL
		Equals(const STreeLink *ptlink1, const STreeLink *ptlink2)
		{
			return EqFn(ptlink1->m_ptParent, ptlink2->m_ptParent) &&
				   EqFn(ptlink1->m_ptChild, ptlink2->m_ptChild) &&
				   ptlink1->m_ulChildIndex == ptlink2->m_ulChildIndex;
		}
	};	// struct STreeLink

	//---------------------------------------------------------------------------
	//	@class:
	//		CTreeNode
	//
	//	@doc:
	//		Internal structure to manage source objects and their topology
	//
	//---------------------------------------------------------------------------
	class CTreeNode
	{
	private:
		// state of tree node during counting alternatives
		enum ENodeState
		{
			EnsUncounted,  // counting not initiated
			EnsCounting,   // counting in progress
			EnsCounted,	   // counting complete

			EnsSentinel
		};

		// memory pool
		CMemoryPool *m_mp;

		// id of node
		// 节点的 id
		ULONG m_ul;

		// element
		// const T *m_value;
		T *m_value;

		// array of children arrays
		CTreeNode2dArray *m_pdrgdrgptn;

		// number of trees rooted in this node
		ULLONG m_ullCount;

		// number of incoming edges
		ULONG m_ulIncoming;

		// node state used for counting alternatives
		ENodeState m_ens;

		// total tree count for a given child
		// 以某个子节点为根节点的树的数量(判断某个 Group 中树的数量)
		ULLONG
		UllCount(ULONG ulChild)
		{
			GPOS_CHECK_STACK_SIZE;

			ULLONG ull = 0;

			// ulCandidates 是 TreeNode 列表的长度
			ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size();  // pdrgpdrgptn[ulChild] 取得的是一个 TreeNode 列表，所有的 expression
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
			{
				CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];  // 某个 Group 的树的个数
				ULLONG ullCount = ptn->UllCount();
				ull = gpos::Add(ull, ullCount);
			}

			return ull;
		}


		/******************** ARENA ********************/
		// 根据某个 tree 构成的所有子树的编号获得该 tree 的所有编号
		// Args:
		//		subId: 子树对应的 id 序列
		//		num: 每个子树原本的数量
		/***********************************************/
		void
		ARENA_getIdList(const std::vector<std::vector<int>>& subId,const std::vector<int>& num, std::vector<int>& idList, std::vector<std::unordered_map<int, CCost>> * costList = nullptr)
		{
			if(subId.size() == 0 || num.size() == 0)
			{
				return;
			}

			// 右边是低位
			// int i = ((int)subId.size()) - 1;
			// for(auto n : subId[i])
			// {
			// 	idList.push_back(n);
			// }

			// for(i--;i>=0;i--)
			// {
			// 	int len = (int)idList.size();
			// 	int maxSize = (int)subId[i].size();  // 当前子树的所有的不同 id
			// 	int k=0;
			// 	for(;k<maxSize-1;k++)
			// 	{
			// 		int addNum = (subId[i][k] - 1) * num[i+1];
			// 		for(int j=0;j<len;j++)
			// 		{
			// 			idList.push_back(idList[j] + addNum);
			// 		}
			// 	}

			// 	int addNum = (subId[i][k] - 1) * num[i+1];
			// 	for(int j=0;j<len;j++)
			// 	{
			// 		idList[j] += addNum;
			// 	}
			// }

			// 左边是低位
			std::vector<std::vector<int>> recordIdList;
			int i = 0;
			for(auto n : subId[i])
			{
				idList.push_back(n);
				if(costList != nullptr)
				{
					recordIdList.push_back(std::vector<int> {n});
				}
			}

			for(i++;i<(int)subId.size();i++)
			{
				int len = (int)idList.size();
				int maxSize = (int)subId[i].size();  // 当前子树的所有的不同 id
				int k=0;
				for(;k<maxSize-1;k++)
				{
					int addNum = (subId[i][k] - 1) * num[i-1];
					for(int j=0;j<len;j++)
					{
						idList.push_back(idList[j] + addNum);
						if(costList != nullptr)  // 记录生成这个 id 用到的所有 subGroup 的 id
						{
							recordIdList.push_back(recordIdList[j]);
							recordIdList.back().push_back(subId[i][k]);
						}
					}
				}

				int addNum = (subId[i][k] - 1) * num[i-1];
				for(int j=0;j<len;j++)
				{
					idList[j] += addNum;
					if(costList != nullptr)
					{
						recordIdList[j].push_back(subId[i][k]);
					}
				}
			}

			if (costList != nullptr)
			{
				if(! ARENA_isVirtual)
				{
					for (std::size_t j = 0; j < idList.size(); j++)  // 遍历每一个 id
					{
						std::vector<CCost *> tempArray;
						for (std::size_t k = 0; k < recordIdList[j].size(); k++)
						{
							int subGroupId = recordIdList[j][k];  // recordIdList[j] 代表第 j 个 id 形成时用到的所有子 Group 的id列表，[k] 代表第 k 个子Group
							CCost & tempCost = costList->at(k).at(subGroupId);
							tempArray.push_back(&tempCost);	 // costList 是一个列表，其中的每一项代表相应子 Group 的 id2Cost 映射表
						}
#ifdef ARENA_COSTFT
						ARENA_id2Cost[idList[j]] = m_value->CostCompute(m_mp, tempArray);  // idList[j] 是计算出的新的 id
#endif
					}
				}
				else  // 虚拟节点，直接将子 Group 的 CCost 移动过来即可
				{
#ifdef ARENA_COSTFT
					for (std::size_t j = 0; j < idList.size(); j++)  // 遍历每一个 id
					{
						CCost tempCost;
						for (std::size_t k = 0; k < recordIdList[j].size(); k++)
						{
							int subGroupId = recordIdList[j][k];  // recordIdList[j] 代表第 j 个 id 形成时用到的所有子 Group 的id列表，[k] 代表第 k 个子Group
							tempCost = costList->at(k).at(subGroupId);
						}
						ARENA_id2Cost[idList[j]] = tempCost;  // idList[j] 是计算出的新的 id
					}
#endif
				}
			}
		}

		// costList : 记录每个 id 对应的 CCost 对象
		void
		ARENA_TreeCount(ULONG ulChild, std::vector<std::vector<std::string>> & treeRecord, std::vector<std::vector<std::vector<int>>> * idRecord=nullptr, std::vector<std::unordered_map<int, CCost>> * costList = nullptr)
		{
			std::vector<std::string> tempList;
			std::vector<std::vector<int>> tempListId;  // 当前该 group 不同 GroupTree 对应的 id
			std::unordered_map<std::string, int> tempRecord;  // 记录每种不同 GroupTree 对应的索引
			std::unordered_map<int, CCost> id2Cost;  // 记录 id 到 CCost 的映射
			// ulCandidates 是 TreeNode 列表的长度
			ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size();  // pdrgpdrgptn[ulChild] 取得的是一个 TreeNode 列表，所有的 expression
			int prefixNum = 0;  // 每个 expression 都有数个 GroupTree，对于第 k 个 expression, 计算其中 GroupTree 对应的 id 时需要加上前 k-1 个 expression 的总数量
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)  // 遍历每一个 expression
			{
				CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];  // 某个 Group 的树的个数
				// 遍历该 TreeNode 下的所有 groupTree
				if(idRecord == nullptr)  // 不需要统计 GroupTree 对应的 id 信息
				{
					for(auto tree: ptn->ARENA_groupTree)
					{
						if(tempRecord.find(tree) == tempRecord.end())  // 还未记录
						{
							tempList.push_back(tree);
							tempRecord[tree] = 1;
						}
					}
				}
				else
				{
					for(auto& pair: ptn->ARENA_groupTreePlus)  // 需要考虑不同 GroupTree 对应的 plan id
					{
						if(tempRecord.find(pair.first) == tempRecord.end())  // 还未记录
						{
							tempList.push_back(pair.first);
							tempRecord[pair.first] = tempList.size()-1;
							tempListId.push_back(pair.second);
							// 更新相应的 id
							auto& temp = tempListId.back();
							for(std::size_t i=0;i<temp.size();i++)
							{
								if(costList != nullptr)  // 记录相应 id 对应的 CCost
								{
#ifdef ARENA_COSTFT
									id2Cost[temp[i]+prefixNum] = ptn->ARENA_id2Cost[temp[i]];  // 更新这个 CCost 在 Group 中的编号
#endif
								}
								temp[i] += prefixNum;
							}
						}
						else  // 增加记录新的 id
						{
							int tempId = tempRecord[pair.first];
							for (std::size_t i = 0; i < pair.second.size(); i++)
							{
								if(costList != nullptr)  // 记录 id 对应的 cost
								{
#ifdef ARENA_COSTFT
									id2Cost[pair.second[i]+prefixNum] = ptn->ARENA_id2Cost[pair.second[i]];
#endif
								}
								tempListId[tempId].push_back(pair.second[i] + prefixNum);
							}
						}
					}
					prefixNum += ptn->m_ullCount;
				}
			}
			
			if (tempList.size() > 0)
			{
				treeRecord.push_back(tempList);
				if(idRecord != nullptr)  // 不需要统计 GroupTree 对应的 id 信息
					idRecord->push_back(tempListId);
			}

			if (costList != nullptr)
			{
				costList->push_back(id2Cost);
			}
		}

		void ARENA_showInfo()
		{
			// 将 TreeNode 节点的信息打印到文件中
			std::ofstream fout("/tmp/treeNode", std::ios_base::app);
			if(fout.is_open())
			{
				if(0 == m_ul)
				{
					fout << "根节点\n";
				}
				else
				{
					if(nullptr == m_value)
					{
						fout << "TreeNode 的 m_value 为空\n";
					}
					else
					{
						fout << m_value->Pgexpr()->Pop()->SzId() << " : ";
						for(auto & s : ARENA_groupTree)
						{
							fout << s << "  ";
						}
						fout << '\n';
					}
					fout.close();
				}
			}
		}
		/**********************************************/

		// rehydrate tree
		// 用来取得当前节点的 local rank
		R *
		PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, ULONG ulChild,
				 ULLONG ullRank)
		{
			GPOS_CHECK_STACK_SIZE;
			GPOS_ASSERT(ullRank < UllCount(ulChild));

			CTreeNodeArray *pdrgptn = (*m_pdrgdrgptn)[ulChild];
			ULONG ulCandidates = pdrgptn->Size();

			CTreeNode *ptn = NULL;

// ulChild 指定了一个 Group ，此处是遍历该 Group 的不同 expression
// 找到这个 id 的 plan 所在的 expression
			for (ULONG ul = 0; ul < ulCandidates; ul++)  
			{
				ptn = (*pdrgptn)[ul];
				ULLONG ullLocalCount = ptn->UllCount();

				if (ullRank < ullLocalCount)
				{
					// ullRank is now local rank for the child
					break;
				}

				ullRank -= ullLocalCount;
			}

			GPOS_ASSERT(NULL != ptn);
			return ptn->PrUnrank(mp, prfn, pU, ullRank);
		}

	public:
		/******************** ARENA ********************/
		bool ARENA_isScalar;
		bool ARENA_isVirtual;  // 是否是虚假的节点
		ULONG ARENA_groupId;  // 该 TreeNode 所在原 Group 的 id
		std::vector<std::string> ARENA_groupTree;  // 记录 group tree 的结构，暂时用于统计或测试能否实现
		std::unordered_map<std::string, std::vector<int>> ARENA_groupTreePlus;  // key: tree 格式  value: id构成的集合
#ifdef ARENA_COSTFT
		std::unordered_map<int, CCost> ARENA_id2Cost;  // key: plan id  value: 相应的 cost
#endif
		/***********************************************/
		// ctor
		CTreeNode(CMemoryPool *mp, ULONG ul, T *value)
			: m_mp(mp),
			  m_ul(ul),
			  m_value(value),
			  m_pdrgdrgptn(NULL),
			  m_ullCount(gpos::ullong_max),
			  m_ulIncoming(0),
			  m_ens(EnsUncounted)
		{
			m_pdrgdrgptn = GPOS_NEW(mp) CTreeNode2dArray(mp);  // 该 cost context 指向的 group 队列
			/******************** ARENA ********************/
			ARENA_isScalar = false;
			ARENA_isVirtual = false;
			if(m_value != NULL)
			{
				ARENA_groupId = m_value->Pgexpr()->Pgroup()->Id();
			}
			else
			{
				ARENA_groupId = 20220610;
			}
			/**********************************************/
		}

		// dtor
		~CTreeNode()
		{
			m_pdrgdrgptn->Release();
		}

		// add child alternative
		// 向目标 Group (ulPos 指定) 中插入一个等价的 TreeNode
		void
		Add(ULONG ulPos, CTreeNode *ptn)
		{
			GPOS_ASSERT(!FCounted() &&
						"Adding edges after counting not meaningful");

			// insert any child arrays skipped so far; make sure we have a dense(稠密的)
			// array up to the position of ulPos
			ULONG length = m_pdrgdrgptn->Size();
			for (ULONG ul = length; ul <= ulPos; ul++)
			{
				CTreeNodeArray *pdrg = GPOS_NEW(m_mp) CTreeNodeArray(m_mp);
				m_pdrgdrgptn->Append(pdrg);
			}

			// increment count of incoming edges
			ptn->m_ulIncoming++;

			// insert to appropriate array
			// 找到特定 Group ，向其中插入 ptn ( CTreeNode )
			CTreeNodeArray *pdrg = (*m_pdrgdrgptn)[ulPos];
			GPOS_ASSERT(NULL != pdrg);
			pdrg->Append(ptn);
		}

		// accessor
		const T *
		Value() const
		{
			return m_value;
		}

		// number of trees rooted in this node
		// 以该节点为根节点的树的数量(以某个 expression 为树根的树的个数)
		// 每个 expression 中不同 Group Tree 对应的局部编号是固定的
		ULLONG
		UllCount()
		{
			GPOS_CHECK_STACK_SIZE;

			GPOS_ASSERT(EnsCounting != m_ens && "cycle in graph detected");

			if (!FCounted())  // 如果还没有进行计数
			{
				// initiate counting on current node
				m_ens = EnsCounting;

				ULLONG ullCount = 1;

				ULONG arity = m_pdrgdrgptn->Size();  // m_pdrgdrgptn : 孩子队列的队列 (Group 队列)
				/******************** ARENA ********************/
				#ifdef ARENA_GTFILTER
				std::vector<std::vector<std::string>> groupTreeRecord;  // 其中每个元素对应一个 group 中的所有可能的 groupTree
				std::vector<std::vector<std::vector<int>>>  groupTreeId;// 顶层vector:不同的Group  第二层vector:每个Group中的不同GroupTree 第三层vector:每个GroupTree对应的不同id
				std::vector<int> groupTreeNum;
#ifdef ARENA_COSTFT
				std::vector<std::unordered_map<int, CCost>> costList;
#endif
				#endif
				/**********************************************/
				for (ULONG ulChild = 0; ulChild < arity; ulChild++)
				{
					ULLONG ull = UllCount(ulChild);
					/******************** ARENA ********************/
					#ifdef ARENA_GTFILTER
					groupTreeNum.push_back((int)ull);  // 记录每个 Group 中子表达式的数量
#ifdef ARENA_COSTFT
					ARENA_TreeCount(ulChild, groupTreeRecord, &groupTreeId, &costList);  // 统计该 group 中不同 group 树的数量
#else
					ARENA_TreeCount(ulChild, groupTreeRecord, &groupTreeId, nullptr);  // 统计该 group 中不同 group 树的数量
#endif
					#endif
					/**********************************************/
					if (0 == ull)
					{
						// if current child has no alternatives, the parent cannot have alternatives
						ullCount = 0;
						break;
					}

					// otherwise, multiply number of child alternatives by current count
					ullCount = gpos::Multiply(ullCount, ull);
				}

				/******************** ARENA ********************/
				// 生成每个 TreeNode 的结构
				#ifdef ARENA_GTFILTER
				if(ARENA_isScalar)
				{
					ARENA_groupTree.push_back(std::string(""));
				}
				else
				{
					if(groupTreeRecord.size() == 0)  // 没有子 group
					{
						std::string temp = "[[]]";  // 没有子 group 说明应该是扫描操作符，需要将表名考虑进去
						ARENA_groupTree.push_back(temp);
						ARENA_groupTreePlus["[[]]"] = std::vector<int>{1};
#ifdef ARENA_COSTFT
						std::vector<CCost*> tempArray;
						ARENA_id2Cost[1] = m_value->CostCompute(m_mp, tempArray);  // CMemoryPool * 和 CCostArray *
#endif
					}
					else  // 存在子 group ，对所有子 group 进行组合
					{
						std::string prefix = "[";
						if(ARENA_isVirtual)  // 如果是虚假节点，不添加 "[]"
						{
							prefix = "";
						}

						ARENA_groupTree.push_back(prefix);
						std::vector<std::vector<std::vector<int>>> ARENA_recordId;  // 记录每个 GroupTree 对应的 编号序列，std::vector<int>是一个编号组合
						ARENA_recordId.resize(1);
						/**************************************************/// 需要更新 ARENA_TreeCount 函数返回不同 Group Tree 结构对应的不同 id
						for(std::size_t i=0;i<groupTreeRecord.size();i++)  // 遍历每个 group
						{
							std::size_t length = ARENA_groupTree.size();
							for(std::size_t j=0;j<length;j++)  // 对已有的每个字符串
							{
								// 前 l-1个新元素加入到末尾
								std::size_t k = 0;
								for(;k<groupTreeRecord[i].size()-1;k++)  // 遍历每个 group 下的不同group tree
								{
									ARENA_groupTree.push_back(ARENA_groupTree[j] + groupTreeRecord[i][k]);
									ARENA_recordId.push_back(ARENA_recordId[j]);
									ARENA_recordId.back().push_back(groupTreeId[i][k]);
								}
								// 最后一个新元素在原位置替换
								ARENA_groupTree[j] = ARENA_groupTree[j] + groupTreeRecord[i][k];
								ARENA_recordId[j].push_back(groupTreeId[i][k]);
							}
						}
						// 如果不是虚假节点，在最后添加 ']'
						if(!ARENA_isVirtual)
						{
							for(std::size_t i=0;i<ARENA_groupTree.size();i++)
							{
								ARENA_groupTree[i] += ']';
							}
						}
						
						// 生成 ARENA_gropuTreePlus 的信息
						if(ARENA_groupTree.size() != ARENA_recordId.size())  // GroupTree的数量一定等于相应编号序列的数量
						{
							std::ofstream fout("/tmp/ARENA_error", std::ios_base::app);
							fout << "在做 GroupTree 到序号的映射时, GroupTree的数量与编号序列的数量不相等: GroupTree " << ARENA_groupTree.size() << "    编号序列 " << ARENA_recordId.size() << '\n';
							fout.close();
						}
						else  // 正常情况下计算每个 GroupTree 对应的编号
						{
							// 计算每个位置的后续组合数
							// 右边是低位
							// for(int i=((int)groupTreeNum.size())-2;i>=0;i--)
							// {
							// 	groupTreeNum[i] *= groupTreeNum[i+1];
							// }
							// 左边是低位
							for(int i=1;i<(int)groupTreeNum.size();i++)
							{
								groupTreeNum[i] *= groupTreeNum[i-1];
							}

							for(std::size_t i=0;i<ARENA_groupTree.size();i++)
							{
								std::vector<int> tempIdList;
#ifdef ARENA_COSTFT
								ARENA_getIdList(ARENA_recordId[i], groupTreeNum, tempIdList, &costList);
#else
								ARENA_getIdList(ARENA_recordId[i], groupTreeNum, tempIdList, nullptr);
#endif
								ARENA_groupTreePlus[ARENA_groupTree[i]] = tempIdList;
							}
						}
					}
				}
				#endif
				/**********************************************/
				// counting is complete
				m_ullCount = ullCount;
				m_ens = EnsCounted;
			}

			return m_ullCount;
		}

		// check if count has been determined for this node
		// 检查这个节点的计数是否已经决定
		BOOL
		FCounted() const
		{
			return (EnsCounted == m_ens);
		}

		// number of incoming edges
		ULONG
		UlIncoming() const
		{
			return m_ulIncoming;
		}

		// unrank tree of a given rank with a given rehydrate function
		// 利用给定的 rehydrate 函数，通过给定的编号获取 plan tree.
		// 这个函数用来确定 ulChild 的值，之后调用 prUnrank(5) 找到正确的plan
		// 这一步用来取得每个子 Group 的 sub_rank
		R *
		PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, ULLONG ullRank)
		{
			GPOS_CHECK_STACK_SIZE;

			R *pr = NULL;

			if (0 == this->m_ul)
			{
				// global root, just unrank 0-th child
				pr = PrUnrank(mp, prfn, pU, 0 /* ulChild */, ullRank);
			}
			else
			{
				DrgPr *pdrg = GPOS_NEW(mp) DrgPr(mp);  // DrgPr 结果指针的队列，CExpression 的队列

				ULLONG ullRankRem = ullRank;

				// 孩子队列的长度
				ULONG ulChildren = m_pdrgdrgptn->Size();
				// 确定每个 group 的 local rank
				for (ULONG ulChild = 0; ulChild < ulChildren; ulChild++)
				{
					ULLONG ullLocalCount = UllCount(ulChild);  // 给定孩子，获得树的数量
					GPOS_ASSERT(0 < ullLocalCount);
					ULLONG ullLocalRank = ullRankRem % ullLocalCount;

					pdrg->Append(PrUnrank(mp, prfn, pU, ulChild, ullLocalRank));

					ullRankRem /= ullLocalCount;
				}

				pr = prfn(mp, const_cast<T *>(this->Value()), pdrg, pU);
			}
			// ARENA_showInfo();

			return pr;  // 类型是  CExpression
		}

#ifdef GPOS_DEBUG

		// debug print
		IOstream &
		OsPrint(IOstream &os)
		{
			ULONG ulChildren = m_pdrgdrgptn->Size();

			os << "=== Node " << m_ul << " [" << *Value()
			   << "] ===" << std::endl
			   << "# children: " << ulChildren << std::endl
			   << "# count: " << this->UllCount() << std::endl;

			for (ULONG ul = 0; ul < ulChildren; ul++)
			{
				os << "--- child: #" << ul << " ---" << std::endl;
				ULONG ulAlt = (*m_pdrgdrgptn)[ul]->Size();

				for (ULONG ulChild = 0; ulChild < ulAlt; ulChild++)
				{
					CTreeNode *ptn = (*(*m_pdrgdrgptn)[ul])[ulChild];
					os << "  -> " << ptn->m_ul << " [" << *ptn->Value() << "]"
					   << std::endl;
				}
			}

			return os;
		}

#endif	// GPOS_DEBUG
	};

	// memory pool
	CMemoryPool *m_mp;

	// counter for nodes
	ULONG m_ulCountNodes;

	// counter for links
	// 所有 links 的计数
	ULONG m_ulCountLinks;

	// rehydrate function
	PrFn m_prfn;

	// universal root (internally used only)
	// 统一的根节点
	CTreeNode *m_ptnRoot;

	// map of all nodes
	// 节点的映射表，由 cost context 映射到 TreeNode
	typedef gpos::CHashMap<T, CTreeNode, HashFn, EqFn, CleanupNULL,
						   CleanupDelete<CTreeNode> >
		TMap;
	typedef gpos::CHashMapIter<T, CTreeNode, HashFn, EqFn, CleanupNULL,
							   CleanupDelete<CTreeNode> >
		TMapIter;

	// map of created links
	// 边的映射表，key: StreeLink  value: BOOL
	typedef CHashMap<STreeLink, BOOL, STreeLink::HashValue, STreeLink::Equals,
					 CleanupDelete<STreeLink>, CleanupDelete<BOOL> >
		LinkMap;

	TMap *m_ptmap;

	// map of nodes to outgoing links
	LinkMap *m_plinkmap;

	// recursive count starting in given node
	ULLONG UllCount(CTreeNode *ptn);

	// Convert to corresponding treenode, create treenode as necessary
	// 根据需要创建 treenode 节点
	CTreeNode *
	// Ptn(const T *value)  // 原来的声明
	Ptn(T *value, bool ARENA_isScalar)  // 针对 ARENA 系统的声明
	{
		GPOS_ASSERT(NULL != value);
		CTreeNode *ptn = const_cast<CTreeNode *>(m_ptmap->Find(value));

		if (NULL == ptn)
		{
			ptn = GPOS_NEW(m_mp) CTreeNode(m_mp, ++m_ulCountNodes, value);
			ptn->ARENA_isScalar = ARENA_isScalar;
			// (void) m_ptmap->Insert(const_cast<T *>(value), ptn);
			(void) m_ptmap->Insert(value, ptn);
		}

		return ptn;
	}

	// private copy ctor
	CTreeMap(const CTreeMap &);

public:
	// ctor
	CTreeMap(CMemoryPool *mp, PrFn prfn)
		: m_mp(mp),
		  m_ulCountNodes(0),
		  m_ulCountLinks(0),
		  m_prfn(prfn),
		  m_ptnRoot(NULL),
		  m_ptmap(NULL),
		  m_plinkmap(NULL)
	{
		GPOS_ASSERT(NULL != mp);
		GPOS_ASSERT(NULL != prfn);

		m_ptmap = GPOS_NEW(mp) TMap(mp);
		m_plinkmap = GPOS_NEW(mp) LinkMap(mp);

		// insert dummy node as global root -- the only node with NULL payload
		m_ptnRoot =
			GPOS_NEW(mp) CTreeNode(mp, 0 /* ulCounter */, NULL /* value */);
	}

	// dtor
	~CTreeMap()
	{
		m_ptmap->Release();
		m_plinkmap->Release();

		GPOS_DELETE(m_ptnRoot);
	}

	/******************** ARENA ********************/
	CTreeNode *
	ProotNode()
	{
		return m_ptnRoot;
	}
	/**********************************************/

	// insert edge as n-th child
	void
	Insert(T *ptParent, ULONG ulPos, T *ptChild, bool isScalar=false)  // 针对 ARENA 系统的定义
	// Insert(const T *ptParent, ULONG ulPos, const T *ptChild)  // 原来的定义
	{
		GPOS_ASSERT(ptParent != ptChild);

		// exit function if link already exists
		STreeLink *ptlink = GPOS_NEW(m_mp) STreeLink(ptParent, ulPos, ptChild);  // 父节点和子节点之间的链接关系
		if (NULL != m_plinkmap->Find(ptlink))
		{
			GPOS_DELETE(ptlink);
			return;
		}

		CTreeNode *ptnParent = Ptn(ptParent, isScalar);
		CTreeNode *ptnChild = Ptn(ptChild, isScalar);

		ptnParent->Add(ulPos, ptnChild);
		++m_ulCountLinks;

		// add created link to links map
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			m_plinkmap->Insert(ptlink, GPOS_NEW(m_mp) BOOL(true));
		GPOS_ASSERT(fInserted);
	}

	// insert a root node
	void
	InsertRoot(T *value)
	{
		GPOS_ASSERT(NULL != value);
		GPOS_ASSERT(NULL != m_ptnRoot);

		// add logical root as 0-th child to global root
		// 逻辑根节点作为全局根节点的第 0 个孩子
		m_ptnRoot->Add(0 /*ulPos*/, Ptn(value));
	}

	// count all possible combinations
	ULLONG
	UllCount()
	{
		// first, hookup all logical root nodes to the global root
		// 第一步，将所有的逻辑根节点连接到全局根上
		TMapIter mi(m_ptmap);
		ULONG ulNodes = 0;
		for (ulNodes = 0; mi.Advance(); ulNodes++)  // 这里遍历了所有的 TreeNode ，然后将入边数量为0的 TreeNode 作为了全局 root 上
		{
			CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());

			if (0 == ptn->UlIncoming())  // (UlIncoming: 每个节点入边的数量)
			{
				// add logical root as 0-th child to global root
				m_ptnRoot->Add(0 /*ulPos*/, ptn);
			}
		}

		// special case of empty map
		if (0 == ulNodes)
		{
			return 0;
		}

		m_ptnRoot->ARENA_isVirtual = true;
		return m_ptnRoot->UllCount();
	}

	// unrank a specific tree
	// ullRank: planId
	// pU: CDrvdPropCtxtPlan
	// 对外开放的接口，根据 id 生成树结构
	R *
	PrUnrank(CMemoryPool *mp, U *pU, ULLONG ullRank) const
	{
		return m_ptnRoot->PrUnrank(mp, m_prfn, pU, ullRank);
	}

	// return number of nodes
	ULONG
	UlNodes() const
	{
		return m_ulCountNodes;
	}

	// return number of links
	ULONG
	UlLinks() const
	{
		return m_ulCountLinks;
	}

#ifdef GPOS_DEBUG

	// retrieve count for individual element
	ULLONG
	UllCount(const T *value)
	{
		CTreeNode *ptn = m_ptmap->Find(value);
		GPOS_ASSERT(NULL != ptn);

		return ptn->UllCount();
	}

	// debug print of entire map
	IOstream &
	OsPrint(IOstream &os) const
	{
		TMapIter mi(m_ptmap);
		ULONG ulNodes = 0;
		for (ulNodes = 0; mi.Advance(); ulNodes++)
		{
			CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());
			(void) ptn->OsPrint(os);
		}

		os << "total number of nodes: " << ulNodes << std::endl;

		return os;
	}

#endif	// GPOS_DEBUG

};	// class CTreeMap

}  // namespace gpopt

#endif	// !GPOPT_CTreeMap_H

// EOF
