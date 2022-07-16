//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtableAccessByKey.h
//
//	@doc:
//		Accessor for allocation-less static hashtable;
//		The Accessor is instantiated with a target key.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableAccessByKey_H
#define GPOS_CSyncHashtableAccessByKey_H


#include "gpos/common/CSyncHashtableAccessorBase.h"


namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSyncHashtableAccessByKey<T, K, S>
//
//	@doc:
//		Accessor class to encapsulate locking of a hashtable bucket based on
//		a passed key; has to know all template parameters of the hashtable
//		class in order to link to the target hashtable; see file doc for more
//		details on the rationale(想法) behind this class
//
//      访问器类用于压缩一个哈希表 bucket 的锁定（？？？？）基于传入的 key 值，
//		必须知道这个哈希表的所有模板参数才能链接到目标哈希表中。
//
//---------------------------------------------------------------------------
template <class T, class K>
class CSyncHashtableAccessByKey : public CSyncHashtableAccessorBase<T, K>
{
private:
	// shorthand for accessor's base class
	typedef class CSyncHashtableAccessorBase<T, K> Base;

	// target key
	const K &m_key;

	// no copy ctor
	CSyncHashtableAccessByKey<T, K>(const CSyncHashtableAccessByKey<T, K> &);

	// finds the first element matching target key starting from
	// the given element
	T *
	NextMatch(T *value) const
	{
		T *curr = value;

		// 当 curr 不为空，且于目标不相符时，寻找下一个
		while (NULL != curr && !Base::GetHashTable().m_eqfn(  // m_eqfn 用于判断两者是否相等
								   Base::GetHashTable().Key(curr), m_key))
		{
			curr = Base::Next(curr);
		}

		return curr;
	}

#ifdef GPOS_DEBUG
	// returns true if current bucket matches key
	BOOL
	CurrentBucketMatchesKey(const K &key) const
	{
		ULONG bucket_idx = Base::GetHashTable().GetBucketIndex(key);

		return &(Base::GetHashTable().GetBucket(bucket_idx)) ==
			   &(Base::GetBucket());
	}
#endif	// GPOS_DEBUG

public:
	// ctor
	// 构造函数只需知道  T 和 K 两个类型
	// 构造函数中传入的两个参数为：目标哈希表， key
	CSyncHashtableAccessByKey<T, K>(CSyncHashtable<T, K> &ht, const K &key)
		: Base(ht, ht.GetBucketIndex(key)), m_key(key)
	{
	}

	// dtor
	virtual ~CSyncHashtableAccessByKey()
	{
	}

	// finds the first bucket's element with a matching key
	T *
	Find() const
	{
		return NextMatch(Base::First());
	}

	// finds the next element with a matching key
	T *
	Next(T *value) const
	{
		GPOS_ASSERT(NULL != value);

		return NextMatch(Base::Next(value));
	}

	// insert at head of target bucket's hash chain
	// 将值插入到目标散列桶的头部
	void
	Insert(T *value)
	{
		GPOS_ASSERT(NULL != value);

#ifdef GPOS_DEBUG
		K &key = Base::GetHashTable().Key(value);
#endif	// GPOS_DEBUG

		// make sure this is a valid key
		GPOS_ASSERT(Base::GetHashTable().IsValid(key));

		// make sure this is the right bucket
		GPOS_ASSERT(CurrentBucketMatchesKey(key));

		// inserting at bucket's head is required by hashtable iteration
		Base::Prepend(value);
	}

};	// class CSyncHashtableAccessByKey

}  // namespace gpos

#endif	// !GPOS_CSyncHashtableAccessByKey_H

// EOF
