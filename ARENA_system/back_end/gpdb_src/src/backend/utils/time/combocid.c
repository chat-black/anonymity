/*-------------------------------------------------------------------------
 *
 * combocid.c
 *	  Combo command ID support routines
 *
 * Before version 8.3, HeapTupleHeaderData had separate fields for cmin
 * and cmax.  To reduce the header size, cmin and cmax are now overlayed
 * in the same field in the header.  That usually works because you rarely
 * insert and delete a tuple in the same transaction, and we don't need
 * either field to remain valid after the originating transaction exits.
 * To make it work when the inserting transaction does delete the tuple,
 * we create a "combo" command ID and store that in the tuple header
 * instead of cmin and cmax. The combo command ID can be mapped to the
 * real cmin and cmax using a backend-private array, which is managed by
 * this module.
 *
 * To allow reusing existing combo cids, we also keep a hash table that
 * maps cmin,cmax pairs to combo cids.  This keeps the data structure size
 * reasonable in most cases, since the number of unique pairs used by any
 * one transaction is likely to be small.
 *
 * With a 32-bit combo command id we can represent 2^32 distinct cmin,cmax
 * combinations. In the most perverse case where each command deletes a tuple
 * generated by every previous command, the number of combo command ids
 * required for N commands is N*(N+1)/2.  That means that in the worst case,
 * that's enough for 92682 commands.  In practice, you'll run out of memory
 * and/or disk space way before you reach that limit.
 *
 * The array and hash table are kept in TopTransactionContext, and are
 * destroyed at the end of each transaction.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/time/combocid.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "utils/combocid.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/tqual.h"
#include "cdb/cdbdtxcontextinfo.h"

#include "access/twophase.h"  /* max_prepared_xacts */

#include "storage/buffile.h"
#include "storage/proc.h"

/*
 * We now maintain two hashtables.
 *
 * 1) local hash for lookup of combocid with the key (cmin, cmax) by the writer
 * 2) shared-hash for lookup of cmin/cmax with the key (parent-xid, combocid, writer-pid) by the readers.
 */

/* HASH TABLE 1 */

/* Hash table to lookup combo cids by cmin and cmax */
static HTAB *comboHash = NULL;

typedef struct
{
	ComboCidKeyData key;
	CommandId	combocid;
} ComboCidEntryData;

typedef ComboCidEntryData *ComboCidEntry;

/* Initial size of the hash table */
#define CCID_HASH_SIZE			100


/*
 * An array of cmin,cmax pairs, indexed by combo command id.
 * To convert a combo cid to cmin and cmax, you do a simple array lookup.
 */
volatile ComboCidKey comboCids = NULL;
volatile int usedComboCids = 0;			/* number of elements in comboCids */
volatile int sizeComboCids = 0;			/* allocated size of array */

/* Initial size of the array */
#define CCID_ARRAY_SIZE			100

/*
 * HASH TABLE 2:
 *
 * Used by reader gangs to lookup using combocid/xmin to find cmin/cmax.
 */
static HTAB *readerComboHash = NULL;

/*
 * Key structure:
 */
typedef struct
{
	int			session;
	int			writer_pid;
	TransactionId	xmin;
	CommandId	combocid;
} readerComboCidKeyData;

typedef struct
{
	readerComboCidKeyData key;
	CommandId cmin, cmax;
} readerComboCidEntryData;

/* prototypes for internal functions */
static CommandId GetComboCommandId(TransactionId xmin, CommandId cmin, CommandId cmax);
static CommandId GetRealCmin(TransactionId xmin, CommandId combocid);
static CommandId GetRealCmax(TransactionId xmin, CommandId combocid);

static BufFile *combocid_map = NULL;
static void dumpSharedComboCommandId(TransactionId xmin, CommandId cmin, CommandId cmax, CommandId combocid);
static void loadSharedComboCommandId(TransactionId xmin, CommandId combocid, CommandId *cmin, CommandId *cmax);

/**** External API ****/

/*
 * GetCmin and GetCmax assert that they are only called in situations where
 * they make sense, that is, can deliver a useful answer.  If you have
 * reason to examine a tuple's t_cid field from a transaction other than
 * the originating one, use HeapTupleHeaderGetRawCommandId() directly.
 */

CommandId
HeapTupleHeaderGetCmin(HeapTupleHeader tup)
{
	CommandId	cid = HeapTupleHeaderGetRawCommandId(tup);

	Assert(!(tup->t_infomask & HEAP_MOVED));

	if (tup->t_infomask & HEAP_COMBOCID)
		return GetRealCmin(HeapTupleHeaderGetXmin(tup), cid);
	else
		return cid;
}

CommandId
HeapTupleHeaderGetCmax(HeapTupleHeader tup)
{
	CommandId	cid = HeapTupleHeaderGetRawCommandId(tup);

	Assert(!(tup->t_infomask & HEAP_MOVED));

	/*
	 * Because GetUpdateXid() performs memory allocations if xmax is a
	 * multixact we can't Assert() if we're inside a critical section. This
	 * weakens the check, but not using GetCmax() inside one would complicate
	 * things too much.
	 */
	/*
	 * MPP-8317: cursors can't always *tell* that this is the current transaction.
	 */
	Assert(QEDtxContextInfo.cursorContext ||
		   CritSectionCount > 0 ||
		   TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tup)));

	if (tup->t_infomask & HEAP_COMBOCID)
		return GetRealCmax(HeapTupleHeaderGetXmin(tup), cid);
	else
		return cid;
}

/*
 * Given a tuple we are about to delete, determine the correct value to store
 * into its t_cid field.
 *
 * If we don't need a combo CID, *cmax is unchanged and *iscombo is set to
 * FALSE.  If we do need one, *cmax is replaced by a combo CID and *iscombo
 * is set to TRUE.
 *
 * The reason this is separate from the actual HeapTupleHeaderSetCmax()
 * operation is that this could fail due to out-of-memory conditions.  Hence
 * we need to do this before entering the critical section that actually
 * changes the tuple in shared buffers.
 */
void
HeapTupleHeaderAdjustCmax(HeapTupleHeader tup,
						  CommandId *cmax,
						  bool *iscombo)
{
	/*
	 * If we're marking a tuple deleted that was inserted by (any
	 * subtransaction of) our transaction, we need to use a combo command id.
	 * Test for HeapTupleHeaderXminCommitted() first, because it's cheaper
	 * than a TransactionIdIsCurrentTransactionId call.
	 */
	if (!HeapTupleHeaderXminCommitted(tup) &&
		TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tup)))
	{
		CommandId	cmin = HeapTupleHeaderGetCmin(tup);

		*cmax = GetComboCommandId(HeapTupleHeaderGetXmin(tup), cmin, *cmax);
		*iscombo = true;
	}
	else
	{
		*iscombo = false;
	}
}

/*
 * Combo command ids are only interesting to the inserting and deleting
 * transaction, so we can forget about them at the end of transaction.
 */
void
AtEOXact_ComboCid(void)
{
	/*
	 * Don't bother to pfree. These are allocated in TopTransactionContext, so
	 * they're going to go away at the end of transaction anyway.
	 */
	comboHash = NULL;

	readerComboHash = NULL;

	comboCids = NULL;
	usedComboCids = 0;
	sizeComboCids = 0;
}


/**** Internal routines ****/

/*
 * Get a combo command id that maps to cmin and cmax.
 *
 * We try to reuse old combo command ids when possible.
 */
static CommandId
GetComboCommandId(TransactionId xmin, CommandId cmin, CommandId cmax)
{
	CommandId	combocid;
	ComboCidKeyData key;
	ComboCidEntry entry;
	bool		found;

	if (Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
	{
		if (IS_QUERY_DISPATCHER())
			elog(ERROR, "EntryReader qExec tried to allocate a Combo Command Id");
		else
			elog(ERROR, "Reader qExec tried to allocate a Combo Command Id");
	}

	/* We're either GP_ROLE_DISPATCH, GP_ROLE_UTILITY, or a QE-writer */

	/*
	 * Create the hash table and array the first time we need to use combo
	 * cids in the transaction.
	 */
	if (comboHash == NULL)
	{
		HASHCTL		hash_ctl;

		/* Make array first; existence of hash table asserts array exists */
		comboCids = (ComboCidKeyData *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(ComboCidKeyData) * CCID_ARRAY_SIZE);
		sizeComboCids = CCID_ARRAY_SIZE;
		usedComboCids = 0;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(ComboCidKeyData);
		hash_ctl.entrysize = sizeof(ComboCidEntryData);
		hash_ctl.hash = tag_hash;
		hash_ctl.hcxt = TopTransactionContext;

		comboHash = hash_create("Combo CIDs",
								CCID_HASH_SIZE,
								&hash_ctl,
								HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	/*
	 * Grow the array if there's not at least one free slot.  We must do this
	 * before possibly entering a new hashtable entry, else failure to
	 * repalloc would leave a corrupt hashtable entry behind.
	 */
	if (usedComboCids >= sizeComboCids)
	{
		int			newsize = sizeComboCids * 2;

		comboCids = (ComboCidKeyData *)
			repalloc(comboCids, sizeof(ComboCidKeyData) * newsize);
		sizeComboCids = newsize;
	}

	/* Lookup or create a hash entry with the desired cmin/cmax */

	/* We assume there is no struct padding in ComboCidKeyData! */
	memset(&key, 0, sizeof(key));
	key.cmin = cmin;
	key.cmax = cmax;
	key.xmin = xmin;
	entry = (ComboCidEntry) hash_search(comboHash,
										(void *) &key,
										HASH_ENTER,
										&found);

	if (found)
	{
		/* Reuse an existing combo cid */
		return entry->combocid;
	}

	/*
	 * We have to create a new combo cid. Check that there's room for it in
	 * the array, and grow it if there isn't.
	 */
	if (usedComboCids >= sizeComboCids)
	{
		/* We need to grow the array */
		int			newsize = sizeComboCids * 2;

		comboCids = (ComboCidKeyData *)
			repalloc(comboCids, sizeof(ComboCidKeyData) * newsize);
		sizeComboCids = newsize;
	}

	/* We are about to create a new combocid */

	combocid = usedComboCids;

	comboCids[combocid].cmin = cmin;
	comboCids[combocid].cmax = cmax;
	comboCids[combocid].xmin = xmin;
	usedComboCids++;

	entry->combocid = combocid;

	/* If we're in utility mode, we don't have to worry about sharing. */
	if (Gp_role == GP_ROLE_UTILITY)
	{
		return combocid;
	}

	/* We are either a QE-writer, or the dispatcher. */
	dumpSharedComboCommandId(xmin, cmin, cmax, combocid);

	return combocid;
}

enum minmax
{
	CMIN,
	CMAX
};

static CommandId
getSharedComboCidEntry(TransactionId xmin, CommandId combocid, enum minmax min_or_max)
{
	bool		found;
	readerComboCidKeyData reader_key;
	readerComboCidEntryData *reader_entry;

	CommandId	cmin = 0,
				cmax = 0;

	if (lockHolderProcPtr == NULL)
	{
		/* get lockholder! */
		elog(ERROR, "getSharedComboCidEntry: NO LOCK HOLDER POINTER.");
	}

	/*
	 * Create the reader hash table and array the first time we need
	 * to use combo cids in the transaction.
	 */
	if (readerComboHash == NULL)
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(readerComboCidKeyData);
		hash_ctl.entrysize = sizeof(readerComboCidEntryData);
		hash_ctl.hash = tag_hash;
		hash_ctl.hcxt = TopTransactionContext;

		readerComboHash = hash_create("Combo CIDs", CCID_HASH_SIZE, &hash_ctl,
									  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	memset(&reader_key, 0, sizeof(reader_key));
	reader_key.writer_pid = lockHolderProcPtr->pid;
	reader_key.xmin = xmin;
	reader_key.session = gp_session_id;
	reader_key.combocid = combocid;

	reader_entry = (readerComboCidEntryData *)
		hash_search(readerComboHash, &reader_key, HASH_FIND, &found);

	if (reader_entry != NULL)
	{
		cmin = reader_entry->cmin;
		cmax = reader_entry->cmax;
	}
	else
	{
		loadSharedComboCommandId(xmin, combocid, &cmin, &cmax);
	}

	return (min_or_max == CMIN ? cmin : cmax);
}

static CommandId
GetRealCmin(TransactionId xmin, CommandId combocid)
{
	if (combocid >= usedComboCids)
	{
		if (Gp_is_writer)
			ereport(ERROR, (errmsg("writer segworker group unable to resolve visibility %u/%u", combocid, usedComboCids)));

		/* We're a reader */
		return getSharedComboCidEntry(xmin, combocid, CMIN);
	}

	Assert(combocid < usedComboCids);
	return comboCids[combocid].cmin;
}

static CommandId
GetRealCmax(TransactionId xmin, CommandId combocid)
{
	if (combocid >= usedComboCids)
	{
		if (Gp_is_writer)
			ereport(ERROR, (errmsg("writer segworker group unable to resolve visibility %u/%u", combocid, usedComboCids)));

		/* We're a reader */
		return getSharedComboCidEntry(xmin, combocid, CMAX);
	}

	Assert(combocid < usedComboCids);
	return comboCids[combocid].cmax;
}

#define ComboCidMapName(path, gp_session_id, pid) \
	snprintf(path, MAXPGPATH, "sess%u_w%u_combocid_map", gp_session_id, pid)

void
dumpSharedComboCommandId(TransactionId xmin, CommandId cmin, CommandId cmax, CommandId combocid)
{
	/*
	 * In any given segment, there are many readers, but only one writer. The
	 * combo cid file information is stored in the MyProc of the writer process,
	 * and is referenced by reader process via lockHolderProcPtr.  The writer
	 * will setup and/or dump combocids to a combo cid file when appropriate.
	 * The writer keeps track of the number of entries in the combo cid file in
	 * MyProc->combocid_map_count. Readers reference the count via
	 * lockHolderProcPtr->combocid_map_count.
	 *
	 * Since combo cid file entries are always appended to the end of a combo
	 * cid file and because there is only one writer, it is not necessary to
	 * lock the combo cid file during reading or writing. A new combo cid will
	 * not become visable to the reader until the combocid_map_count variable
	 * has been incremented.
	 */

	ComboCidEntryData entry;

	Assert(Gp_role != GP_ROLE_EXECUTE || Gp_is_writer);

	if (combocid_map == NULL)
	{
		/* This is the first time a combo cid is to be written by this writer. */
		MemoryContext oldCtx;
		char			path[MAXPGPATH];

		MyProc->combocid_map_count = 0;

		ComboCidMapName(path, gp_session_id, MyProc->pid);

		/* open our file, as appropriate: this will throw an error if the create-fails. */
		oldCtx = MemoryContextSwitchTo(TopMemoryContext);

		/*
		 * XXX: We could probably close and delete the file at the end of
		 * transaction.  We would then need to keep combocid_map_count
		 * synchronized with open files at (sub-) xact boundaries.
		 */
		combocid_map = BufFileCreateNamedTemp(path,
											  true /* interXact */,
											  NULL /* work_set */);
		MemoryContextSwitchTo(oldCtx);
	}
	Assert(combocid_map != NULL);

	/* Seek to the end: BufFileSeek() doesn't support SEEK_END! */

	/* build our entry */
	memset(&entry, 0, sizeof(entry));
	entry.key.cmin = cmin;
	entry.key.cmax = cmax;
	entry.key.xmin = xmin;
	entry.combocid = combocid;

	/* write our entry */
	if (BufFileWrite(combocid_map, &entry, sizeof(entry)) != sizeof(entry))
	{
		elog(ERROR, "Combocid map I/O error!");
	}

	/* flush our output */
	BufFileFlush(combocid_map);

	/* Increment combocid count to make new combocid visible to Readers */
	MyProc->combocid_map_count += 1;
}

void
loadSharedComboCommandId(TransactionId xmin, CommandId combocid, CommandId *cmin, CommandId *cmax)
{
	bool		found = false;
	ComboCidEntryData entry;
	int			i;

	Assert(Gp_role == GP_ROLE_EXECUTE);
	Assert(!Gp_is_writer);
	Assert(cmin != NULL);
	Assert(cmax != NULL);

	if (lockHolderProcPtr == NULL)
	{
		/* get lockholder! */
		elog(ERROR, "loadSharedComboCommandId: NO LOCK HOLDER POINTER.");
	}

	if (combocid_map == NULL)
	{
		MemoryContext oldCtx;
		char			path[MAXPGPATH];

		ComboCidMapName(path, gp_session_id, lockHolderProcPtr->pid);
		/* open our file, as appropriate: this will throw an error if the create-fails. */
		oldCtx = MemoryContextSwitchTo(TopMemoryContext);
		combocid_map = BufFileOpenNamedTemp(path,
											true /* interXact */);
		MemoryContextSwitchTo(oldCtx);
	}
	Assert(combocid_map != NULL);

	/* Seek to the beginning to start our search ? */
	if (BufFileSeek(combocid_map, 0 /* fileno */, 0 /* offset */, SEEK_SET) != 0)
	{
		elog(ERROR, "loadSharedComboCommandId: seek to beginning failed.");
	}

	/*
	 * Read this entry in ...
	 *
	 * We're going to read in the entire table, caching all occurrences of
	 * our xmin.
	 */
	for (i = 0; i < lockHolderProcPtr->combocid_map_count; i++)
	{
		if (BufFileRead(combocid_map, &entry, sizeof(ComboCidEntryData)) != sizeof(ComboCidEntryData))
		{
			elog(ERROR, "loadSharedComboCommandId: read failed I/O error.");
		}

		if (entry.key.xmin == xmin)
		{
			bool		cached = false;
			readerComboCidKeyData reader_key;
			readerComboCidEntryData *reader_entry;

			memset(&reader_key, 0, sizeof(reader_key));
			reader_key.writer_pid = lockHolderProcPtr->pid;
			reader_key.xmin = entry.key.xmin;
			reader_key.session = gp_session_id;
			reader_key.combocid = entry.combocid;

			reader_entry = (readerComboCidEntryData *)
				hash_search(readerComboHash, &reader_key, HASH_ENTER, &cached);

			if (!cached)
			{
				reader_entry->cmin = entry.key.cmin;
				reader_entry->cmax = entry.key.cmax;
			}

			/*
			 * This was our entry -- we're going to continue our scan,
			 * to pull in any additional entries for our xmin
			 */
			if (entry.combocid == combocid)
			{
				*cmin = entry.key.cmin;
				*cmax = entry.key.cmax;
				found = true;
			}
		}
	}

	if (!found)
	{
		elog(ERROR, "loadSharedComboCommandId: no combocid entry found for %u/%u", xmin, combocid);
	}
}
