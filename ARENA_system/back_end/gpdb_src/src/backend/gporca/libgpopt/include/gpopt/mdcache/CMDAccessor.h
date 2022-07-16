//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDAccessor.h
//
//	@doc:
//		Metadata cache accessor.
//---------------------------------------------------------------------------



#ifndef GPOPT_CMDAccessor_H
#define GPOPT_CMDAccessor_H

#include "gpos/base.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheAccessor.h"

#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/mdcache/CMDKey.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/IStatistics.h"

// fwd declarations
namespace gpdxl
{
class CDXLDatum;
}

namespace gpmd
{
class IMDCacheObject;
class IMDRelation;
class IMDRelationExternal;
class IMDScalarOp;
class IMDAggregate;
class IMDTrigger;
class IMDIndex;
class IMDCheckConstraint;
class IMDProvider;
class CMDProviderGeneric;
class IMDColStats;
class IMDRelStats;
class CDXLBucket;
class IMDCast;
class IMDScCmp;
}  // namespace gpmd

namespace gpnaucrates
{
class CHistogram;
class CBucket;
class IStatistics;
}  // namespace gpnaucrates

namespace gpopt
{
using namespace gpos;
using namespace gpmd;


typedef IMDId *MdidPtr;

//---------------------------------------------------------------------------
//	@class:
//		CMDAccessor
//
//	@doc:
//		Gives the optimizer access to metadata information of a particular
//		object (e.g., a Table).
//
//		CMDAccessor maintains a cache of metadata objects (IMDCacheObject)
//		keyed on CMDKey (wrapper over IMDId). It also provides various accessor
//		methods (such as RetrieveAgg(), RetrieveRel() etc.) to request for corresponding
//		metadata objects (such as aggregates and relations respectively). These
//		methods in turn call the private method GetImdObj().

//      CMDAccessor 维护了 元数据对象的缓存，并以 CMDKey(IMDId 的一个包装) 作为键值，它同时也
//		提供了各种各样的访问方法（例如：RetrieveAgg(), RetrieveRel() 等)来请求相应的元数据对象（例如：aggregates和relations
//		等).这些方法之后再调用私有方法  GetImdObj()
//
//		GetImdObj() first looks up the object in the MDCache. If no information
//		is available in the cache, it goes to a CMDProvider (e.g., GPDB
//		relcache or Minidump) to retrieve the required information.
//		GetImdObj() 首先在MDCache中寻找该对象，如果缓存中不存在，它会从 CMDProvider 中获得请求的信息
//
//---------------------------------------------------------------------------
class CMDAccessor
{
public:
	// ccache template for mdcache
	typedef CCache<IMDCacheObject *, CMDKey *> MDCache;  // 定义了 MDCache 类型

private:
	// element in the hashtable of cache accessors maintained by the MD accessor
	// MD accessor 维护的缓存访问哈希表中的元素
	struct SMDAccessorElem;
	struct SMDProviderElem;


	// cache accessor for objects in a MD cache
	// MD cache 中对象的缓存访问器
	typedef CCacheAccessor<IMDCacheObject *, CMDKey *> CacheAccessorMD;

	// hashtable for cache accessors indexed by the md id of the accessed object
	// 
	typedef CSyncHashtable<SMDAccessorElem, MdidPtr> MDHT;

	typedef CSyncHashtableAccessByKey<SMDAccessorElem, MdidPtr> MDHTAccessor;

	// iterator for the cache accessors hashtable
	typedef CSyncHashtableIter<SMDAccessorElem, MdidPtr> MDHTIter;
	typedef CSyncHashtableAccessByIter<SMDAccessorElem, MdidPtr>
		MDHTIterAccessor;

	// hashtable for MD providers indexed by the source system id
	// MD provider 的哈希表，键值为：system id
	typedef CSyncHashtable<SMDProviderElem, SMDProviderElem> MDPHT;

	typedef CSyncHashtableAccessByKey<SMDProviderElem, SMDProviderElem>
		MDPHTAccessor;

	// iterator for the providers hashtable
	typedef CSyncHashtableIter<SMDProviderElem, SMDProviderElem> MDPHTIter;
	typedef CSyncHashtableAccessByIter<SMDProviderElem, SMDProviderElem>
		MDPHTIterAccessor;

	// element in the cache accessor hashtable maintained by the MD Accessor
	// 缓存访问器中的元素的哈希表由  MD Accessor 所维护
	struct SMDAccessorElem
	{
	private:
		// hashed object
		IMDCacheObject *m_imd_obj;

	public:
		// hash key
		IMDId *m_mdid;

		// generic link
		SLink m_link;

		// invalid key
		static const MdidPtr m_pmdidInvalid;

		// ctor
		SMDAccessorElem(IMDCacheObject *pimdobj, IMDId *mdid);

		// dtor
		~SMDAccessorElem();

		// hashed object
		IMDCacheObject *
		GetImdObj()
		{
			return m_imd_obj;
		}

		// return the key for this hashtable element
		IMDId *MDId();

		// equality function for hash tables
		static BOOL Equals(const MdidPtr &left_mdid, const MdidPtr &right_mdid);

		// hash function for cost contexts hash table
		static ULONG HashValue(const MdidPtr &mdid);
	};

	// element in the MD provider hashtable
	// MD provider 哈希表中的元素
	struct SMDProviderElem
	{
	private:
		// source system id
		// 资源的系统 id
		CSystemId m_sysid;

		// value of the hashed element
		// 哈希后的元素的值
		IMDProvider *m_pmdp;

	public:
		// generic link
		SLink m_link;

		// invalid key
		static const SMDProviderElem m_mdpelemInvalid;

		// ctor
		SMDProviderElem(CSystemId sysid, IMDProvider *pmdp);

		// dtor
		~SMDProviderElem();

		// return the MD provider
		IMDProvider *Pmdp();

		// return the system id
		CSystemId Sysid() const;

		// equality function for hash tables
		static BOOL Equals(const SMDProviderElem &mdpelemLeft,
						   const SMDProviderElem &mdpelemRight);

		// hash function for MD providers hash table
		static ULONG HashValue(const SMDProviderElem &mdpelem);
	};

private:
	// memory pool
	CMemoryPool *m_mp;

	// metadata cache
	MDCache *m_pcache;

	// generic metadata provider
	CMDProviderGeneric *m_pmdpGeneric;

	// hashtable of cache accessors
	// 缓存访问器的哈希表
	MDHT m_shtCacheAccessors;

	// hashtable of MD providers
	MDPHT m_shtProviders;

	// total time consumed in looking up MD objects (including time used to fetch objects from MD provider)
	CDouble m_dLookupTime;

	// total time consumed in fetching MD objects from MD provider,
	// this time is currently dominated by serialization time
	CDouble m_dFetchTime;

	// private copy ctor
	CMDAccessor(const CMDAccessor &);

	// interface to a MD cache object
	const IMDCacheObject *GetImdObj(IMDId *mdid);

	// return the type corresponding to the given type info and source system id
	const IMDType *RetrieveType(CSystemId sysid, IMDType::ETypeInfo type_info);

	// return the generic type corresponding to the given type info
	const IMDType *RetrieveType(IMDType::ETypeInfo type_info);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyAccessorElement(SMDAccessorElem *pmdaccelem);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyProviderElement(SMDProviderElem *pmdpelem);

	// lookup an MD provider by system id
	IMDProvider *Pmdp(CSystemId sysid);

	// initialize hash tables
	void InitHashtables(CMemoryPool *mp);

	// return the column statistics meta data object for a given column of a table
	const IMDColStats *Pmdcolstats(CMemoryPool *mp, IMDId *rel_mdid,
								   ULONG ulPos);

	// record histogram and width information for a given column of a table
	void RecordColumnStats(CMemoryPool *mp, IMDId *rel_mdid, ULONG colid,
						   ULONG ulPos, BOOL isSystemCol, BOOL isEmptyTable,
						   UlongToHistogramMap *col_histogram_mapping,
						   UlongToDoubleMap *colid_width_mapping,
						   CStatisticsConfig *stats_config);

	// construct a stats histogram from an MD column stats object
	CHistogram *GetHistogram(CMemoryPool *mp, IMDId *mdid_type,
							 const IMDColStats *pmdcolstats);

	// construct a typed bucket from a DXL bucket
	CBucket *Pbucket(CMemoryPool *mp, IMDId *mdid_type,
					 const CDXLBucket *dxl_bucket);

	// construct a typed datum from a DXL bucket
	IDatum *GetDatum(CMemoryPool *mp, IMDId *mdid_type,
					 const CDXLDatum *dxl_datum);

public:
	// ctors
	CMDAccessor(CMemoryPool *mp, MDCache *pcache);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache, CSystemId sysid,
				IMDProvider *pmdp);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache,
				const CSystemIdArray *pdrgpsysid,
				const CMDProviderArray *pdrgpmdp);

	//dtor
	~CMDAccessor();

	// return MD cache
	// 返回 metadata 的缓存
	MDCache *
	Pcache() const
	{
		return m_pcache;
	}

	// register a new MD provider
	void RegisterProvider(CSystemId sysid, IMDProvider *pmdp);

	// register given MD providers
	void RegisterProviders(const CSystemIdArray *pdrgpsysid,
						   const CMDProviderArray *pdrgpmdp);

//////////////////////////////// 相关信息的接口   ////////////////////////////////
	// interface to a relation object from the MD cache
	// 从 MD cache 中获取关系对象的接口
	const IMDRelation *RetrieveRel(IMDId *mdid);

	// interface to type's from the MD cache given the type's mdid
	// 给定 type 的 mdid 返回 type 的接口
	const IMDType *RetrieveType(IMDId *mdid);

	// obtain the specified base type given by the template parameter
	// 获得给定模板参数指定的特定基类型
	template <class T>
	const T *
	PtMDType()
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(type_info));
	}

	// obtain the specified base type given by the template parameter
	template <class T>
	const T *
	PtMDType(CSystemId sysid)
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(sysid, type_info));
	}

	// interface to a scalar operator from the MD cache
	// 从 MD cache 中获得标量运算符的接口
	const IMDScalarOp *RetrieveScOp(IMDId *mdid);

	// interface to a function from the MD cache
	// 函数信息的接口
	const IMDFunction *RetrieveFunc(IMDId *mdid);

	// interface to check if the window function from the MD cache is an aggregate window function
	// 检查一个窗口函数是否为聚集窗口函数
	BOOL FAggWindowFunc(IMDId *mdid);

	// interface to an aggregate from the MD cache
	// 聚集信息的接口
	const IMDAggregate *RetrieveAgg(IMDId *mdid);

	// interface to a trigger from the MD cache
	// 触发器信息的接口
	const IMDTrigger *RetrieveTrigger(IMDId *mdid);

	// interface to an index from the MD cache
	// 索引信息的接口  ***
	const IMDIndex *RetrieveIndex(IMDId *mdid);

	// interface to a check constraint from the MD cache
	// 检查约束的接口
	const IMDCheckConstraint *RetrieveCheckConstraints(IMDId *mdid);

	// retrieve a column stats object from the cache
	// 从缓存中获得列统计信息
	const IMDColStats *Pmdcolstats(IMDId *mdid);

	// retrieve a relation stats object from the cache
	// 从缓存中获得关系的缓存信息
	const IMDRelStats *Pmdrelstats(IMDId *mdid);

	// retrieve a cast object from the cache
	// 获取 cast 对象的信息
	const IMDCast *Pmdcast(IMDId *mdid_src, IMDId *mdid_dest);

	// retrieve a scalar comparison object from the cache
	// 从缓存中获得标量比较对象
	const IMDScCmp *Pmdsccmp(IMDId *left_mdid, IMDId *right_mdid,
							 IMDType::ECmpType cmp_type);

	// construct a statistics object for the columns of the given relation
	IStatistics *Pstats(
		CMemoryPool *mp, IMDId *rel_mdid,
		CColRefSet
			*pcrsHist,	// set of column references for which stats are needed
		CColRefSet *
			pcrsWidth,	// set of column references for which the widths are needed
		CStatisticsConfig *stats_config = NULL);

	// serialize object to passed stream
	void Serialize(COstream &oos);

	// serialize system ids to passed stream
	void SerializeSysid(COstream &oos);
};
}  // namespace gpopt



#endif	// !GPOPT_CMDAccessor_H

// EOF
