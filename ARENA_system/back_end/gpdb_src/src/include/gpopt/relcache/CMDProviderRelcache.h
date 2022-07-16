//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDProviderRelcache.h
//
//	@doc:
//		Relcache-based provider of metadata objects.
//		基于 Relcache 的元数据对象提供者
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderRelcache_H
#define GPMD_CMDProviderRelcache_H

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

// fwd decl
namespace gpopt
{
class CMDAccessor;
}

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDProviderRelcache
//
//	@doc:
//		Relcache-based provider of metadata objects.
//
//---------------------------------------------------------------------------
class CMDProviderRelcache : public IMDProvider
{
private:
	// memory pool
	// 私有的内存池
	CMemoryPool *m_mp;

	// private copy ctor
	CMDProviderRelcache(const CMDProviderRelcache &);

public:
	// ctor/dtor
	explicit CMDProviderRelcache(CMemoryPool *mp);

	~CMDProviderRelcache()
	{
	}

	// returns the DXL string of the requested metadata object
	// 返回请求的元数据对象的 DXL 字符串
	virtual CWStringBase *GetMDObjDXLStr(CMemoryPool *mp,
										 CMDAccessor *md_accessor,
										 IMDId *md_id) const;

	// return the mdid for the requested type
	// 返回请求类型的 mdid
	virtual IMDId *
	MDId(CMemoryPool *mp, CSystemId sysid, IMDType::ETypeInfo type_info) const
	{
		return GetGPDBTypeMdid(mp, sysid, type_info);
	}
};
}  // namespace gpmd



#endif	// !GPMD_CMDProviderRelcache_H

// EOF
