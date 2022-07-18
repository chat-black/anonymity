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
//		with a topology solely given by edges between the object. The
//		unranking utilizes client provided function and generates results of
//		type R (e.g., CExpression);
//		U is a global context accessible to recursive rehydrate calls.
//		Pointers to objects of type U are passed through PrUnrank calls to the
//		rehydrate function of type PrFn.
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
		ULONG m_ul;

		// element
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
		ULLONG
		UllCount(ULONG ulChild)
		{
			GPOS_CHECK_STACK_SIZE;

			ULLONG ull = 0;

			ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size();
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
			{
				CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];
				ULLONG ullCount = ptn->UllCount();
				ull = gpos::Add(ull, ullCount);
			}

			return ull;
		}


		void
		ARENA_getIdList(const std::vector<std::vector<int>>& subId,const std::vector<int>& num, std::vector<int>& idList, std::vector<std::unordered_map<int, CCost>> * costList = nullptr)
		{
			if(subId.size() == 0 || num.size() == 0)
			{
				return;
			}

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
				int maxSize = (int)subId[i].size();
				int k=0;
				for(;k<maxSize-1;k++)
				{
					int addNum = (subId[i][k] - 1) * num[i-1];
					for(int j=0;j<len;j++)
					{
						idList.push_back(idList[j] + addNum);
						if(costList != nullptr)
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
					for (std::size_t j = 0; j < idList.size(); j++)
					{
						std::vector<CCost *> tempArray;
						for (std::size_t k = 0; k < recordIdList[j].size(); k++)
						{
							int subGroupId = recordIdList[j][k];
							CCost & tempCost = costList->at(k).at(subGroupId);
							tempArray.push_back(&tempCost);
						}
#ifdef ARENA_COSTFT
						ARENA_id2Cost[idList[j]] = m_value->CostCompute(m_mp, tempArray);
#endif
					}
				}
				else
				{
#ifdef ARENA_COSTFT
					for (std::size_t j = 0; j < idList.size(); j++)  
					{
						CCost tempCost;
						for (std::size_t k = 0; k < recordIdList[j].size(); k++)
						{
							int subGroupId = recordIdList[j][k];
							tempCost = costList->at(k).at(subGroupId);
						}
						ARENA_id2Cost[idList[j]] = tempCost;
					}
#endif
				}
			}
		}

		void
		ARENA_TreeCount(ULONG ulChild, std::vector<std::vector<std::string>> & treeRecord, std::vector<std::vector<std::vector<int>>> * idRecord=nullptr, std::vector<std::unordered_map<int, CCost>> * costList = nullptr)
		{
			std::vector<std::string> tempList;
			std::vector<std::vector<int>> tempListId;
			std::unordered_map<std::string, int> tempRecord;
			std::unordered_map<int, CCost> id2Cost;  

			ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size(); 
			int prefixNum = 0; 
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
			{
				CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];

				if(idRecord == nullptr)
				{
					for(auto tree: ptn->ARENA_groupTree)
					{
						if(tempRecord.find(tree) == tempRecord.end())
						{
							tempList.push_back(tree);
							tempRecord[tree] = 1;
						}
					}
				}
				else
				{
					for(auto& pair: ptn->ARENA_groupTreePlus) 
					{
						if(tempRecord.find(pair.first) == tempRecord.end())
						{
							tempList.push_back(pair.first);
							tempRecord[pair.first] = tempList.size()-1;
							tempListId.push_back(pair.second);

							auto& temp = tempListId.back();
							for(std::size_t i=0;i<temp.size();i++)
							{
								if(costList != nullptr)
								{
#ifdef ARENA_COSTFT
									id2Cost[temp[i]+prefixNum] = ptn->ARENA_id2Cost[temp[i]];
#endif
								}
								temp[i] += prefixNum;
							}
						}
						else
						{
							int tempId = tempRecord[pair.first];
							for (std::size_t i = 0; i < pair.second.size(); i++)
							{
								if(costList != nullptr)
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
				if(idRecord != nullptr)
					idRecord->push_back(tempListId);
			}

			if (costList != nullptr)
			{
				costList->push_back(id2Cost);
			}
		}

		void ARENA_showInfo()
		{
			std::ofstream fout("/tmp/treeNode", std::ios_base::app);
			if(fout.is_open())
			{
				if(0 == m_ul)
				{
					fout << "root Node\n";
				}
				else
				{
					if(nullptr == m_value)
					{
						fout << "TreeNode's m_value is empty\n";
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
		R *
		PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, ULONG ulChild,
				 ULLONG ullRank)
		{
			GPOS_CHECK_STACK_SIZE;
			GPOS_ASSERT(ullRank < UllCount(ulChild));

			CTreeNodeArray *pdrgptn = (*m_pdrgdrgptn)[ulChild];
			ULONG ulCandidates = pdrgptn->Size();

			CTreeNode *ptn = NULL;

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
		bool ARENA_isVirtual;
		ULONG ARENA_groupId; 
		std::vector<std::string> ARENA_groupTree; 
		std::unordered_map<std::string, std::vector<int>> ARENA_groupTreePlus;
#ifdef ARENA_COSTFT
		std::unordered_map<int, CCost> ARENA_id2Cost; 
#endif
		/******************** ARENA END ***************************/
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
			m_pdrgdrgptn = GPOS_NEW(mp) CTreeNode2dArray(mp);
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
			/******************* ARENA END ***************************/
		}

		// dtor
		~CTreeNode()
		{
			m_pdrgdrgptn->Release();
		}

		// add child alternative
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
		ULLONG
		UllCount()
		{
			GPOS_CHECK_STACK_SIZE;

			GPOS_ASSERT(EnsCounting != m_ens && "cycle in graph detected");

			if (!FCounted())
			{
				// initiate counting on current node
				m_ens = EnsCounting;

				ULLONG ullCount = 1;

				ULONG arity = m_pdrgdrgptn->Size();
				/******************** ARENA ********************/
				#ifdef ARENA_GTFILTER
				std::vector<std::vector<std::string>> groupTreeRecord;  
				std::vector<std::vector<std::vector<int>>>  groupTreeId;
				std::vector<int> groupTreeNum;
#ifdef ARENA_COSTFT
				std::vector<std::unordered_map<int, CCost>> costList;
#endif
				#endif
				/******************** ARENA END **************************/
				for (ULONG ulChild = 0; ulChild < arity; ulChild++)
				{
					ULLONG ull = UllCount(ulChild);
					/******************** ARENA ********************/
					#ifdef ARENA_GTFILTER
					groupTreeNum.push_back((int)ull);
#ifdef ARENA_COSTFT
					ARENA_TreeCount(ulChild, groupTreeRecord, &groupTreeId, &costList);
#else
					ARENA_TreeCount(ulChild, groupTreeRecord, &groupTreeId, nullptr);
#endif
					#endif
					/******************** ARENA END **************************/
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
				#ifdef ARENA_GTFILTER
				if(ARENA_isScalar)
				{
					ARENA_groupTree.push_back(std::string(""));
				}
				else
				{
					if(groupTreeRecord.size() == 0)
					{
						std::string temp = "[[]]";
						ARENA_groupTree.push_back(temp);
						ARENA_groupTreePlus["[[]]"] = std::vector<int>{1};
#ifdef ARENA_COSTFT
						std::vector<CCost*> tempArray;
						ARENA_id2Cost[1] = m_value->CostCompute(m_mp, tempArray);  
#endif
					}
					else 
					{
						std::string prefix = "[";
						if(ARENA_isVirtual)
						{
							prefix = "";
						}

						ARENA_groupTree.push_back(prefix);
						std::vector<std::vector<std::vector<int>>> ARENA_recordId; 
						ARENA_recordId.resize(1);

						for(std::size_t i=0;i<groupTreeRecord.size();i++)
						{
							std::size_t length = ARENA_groupTree.size();
							for(std::size_t j=0;j<length;j++) 
							{
								std::size_t k = 0;
								for(;k<groupTreeRecord[i].size()-1;k++)
								{
									ARENA_groupTree.push_back(ARENA_groupTree[j] + groupTreeRecord[i][k]);
									ARENA_recordId.push_back(ARENA_recordId[j]);
									ARENA_recordId.back().push_back(groupTreeId[i][k]);
								}

								ARENA_groupTree[j] = ARENA_groupTree[j] + groupTreeRecord[i][k];
								ARENA_recordId[j].push_back(groupTreeId[i][k]);
							}
						}

						if(!ARENA_isVirtual)
						{
							for(std::size_t i=0;i<ARENA_groupTree.size();i++)
							{
								ARENA_groupTree[i] += ']';
							}
						}
						
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
				#endif
				/**********************************************/
				// counting is complete
				m_ullCount = ullCount;
				m_ens = EnsCounted;
			}

			return m_ullCount;
		}

		// check if count has been determined for this node
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
				DrgPr *pdrg = GPOS_NEW(mp) DrgPr(mp);

				ULLONG ullRankRem = ullRank;

				ULONG ulChildren = m_pdrgdrgptn->Size();

				for (ULONG ulChild = 0; ulChild < ulChildren; ulChild++)
				{
					ULLONG ullLocalCount = UllCount(ulChild);
					GPOS_ASSERT(0 < ullLocalCount);
					ULLONG ullLocalRank = ullRankRem % ullLocalCount;

					pdrg->Append(PrUnrank(mp, prfn, pU, ulChild, ullLocalRank));

					ullRankRem /= ullLocalCount;
				}

				pr = prfn(mp, const_cast<T *>(this->Value()), pdrg, pU);
			}
			// ARENA_showInfo();

			return pr;
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
	ULONG m_ulCountLinks;

	// rehydrate function
	PrFn m_prfn;

	// universal root (internally used only)
	CTreeNode *m_ptnRoot;

	// map of all nodes
	typedef gpos::CHashMap<T, CTreeNode, HashFn, EqFn, CleanupNULL,
						   CleanupDelete<CTreeNode> >
		TMap;
	typedef gpos::CHashMapIter<T, CTreeNode, HashFn, EqFn, CleanupNULL,
							   CleanupDelete<CTreeNode> >
		TMapIter;

	// map of created links
	typedef CHashMap<STreeLink, BOOL, STreeLink::HashValue, STreeLink::Equals,
					 CleanupDelete<STreeLink>, CleanupDelete<BOOL> >
		LinkMap;

	TMap *m_ptmap;

	// map of nodes to outgoing links
	LinkMap *m_plinkmap;

	// recursive count starting in given node
	ULLONG UllCount(CTreeNode *ptn);

	// Convert to corresponding treenode, create treenode as necessary
	CTreeNode *
	Ptn(T *value, bool ARENA_isScalar) 
	{
		GPOS_ASSERT(NULL != value);
		CTreeNode *ptn = const_cast<CTreeNode *>(m_ptmap->Find(value));

		if (NULL == ptn)
		{
			ptn = GPOS_NEW(m_mp) CTreeNode(m_mp, ++m_ulCountNodes, value);
			ptn->ARENA_isScalar = ARENA_isScalar;
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
	Insert(T *ptParent, ULONG ulPos, T *ptChild, bool isScalar=false)
	{
		GPOS_ASSERT(ptParent != ptChild);

		// exit function if link already exists
		STreeLink *ptlink = GPOS_NEW(m_mp) STreeLink(ptParent, ulPos, ptChild);
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
		m_ptnRoot->Add(0 /*ulPos*/, Ptn(value));
	}

	// count all possible combinations
	ULLONG
	UllCount()
	{
		// first, hookup all logical root nodes to the global root
		TMapIter mi(m_ptmap);
		ULONG ulNodes = 0;
		for (ulNodes = 0; mi.Advance(); ulNodes++)
		{
			CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());

			if (0 == ptn->UlIncoming())
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
