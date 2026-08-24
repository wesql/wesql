/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>

/** Max table name length as defined in univ.i */
#define MAX_TABLE_NAME_LEN      320
#define MAX_DATABASE_NAME_LEN   MAX_TABLE_NAME_LEN
#define MAX_FULL_NAME_LEN                               \
        (MAX_TABLE_NAME_LEN + MAX_DATABASE_NAME_LEN + 14)

/** Representation of a byte within smartengine */
typedef unsigned char se_byte_t;
/** Representation of an unsigned long int within smartengine */

typedef unsigned long int se_ulint_t;

/** A signed 8 bit integral type. */
typedef int8_t se_i8_t;

/** An unsigned 8 bit integral type. */
typedef uint8_t se_u8_t;

/** A signed 16 bit integral type. */
typedef int16_t se_i16_t;

/** An unsigned 16 bit integral type. */
typedef uint16_t se_u16_t;

/** A signed 32 bit integral type. */
typedef int32_t se_i32_t;

/** An unsigned 32 bit integral type. */
typedef uint32_t se_u32_t;

/** A signed 64 bit integral type. */
typedef int64_t se_i64_t;

/** An unsigned 64 bit integral type. */
typedef uint64_t se_u64_t;

/** Generical smartengine callback prototype. */
typedef void (*se_cb_t)(void);

typedef void*			se_opaque_t;
typedef se_opaque_t		SE_CHARset_t;
typedef se_ulint_t		se_bool_t;
typedef se_u64_t		se_id_u64_t;

namespace se {

enum dberr_t {
  /**like DB_SUCCESS, but a new explicit record lock was created */
	DB_SUCCESS_LOCKED_REC = 9,
	DB_SUCCESS = 10,

	/**The following are error codes */
	DB_ERROR,
	DB_INTERRUPTED,
	DB_OUT_OF_MEMORY,
	DB_OUT_OF_FILE_SPACE,
	DB_LOCK_WAIT,
	DB_DEADLOCK,
	DB_ROLLBACK,
	DB_DUPLICATE_KEY,
	DB_QUE_THR_SUSPENDED,
  /**required history data has been deleted due to lack of space in
	rollback segment */
	DB_MISSING_HISTORY,
	DB_CLUSTER_NOT_FOUND = 30,
	DB_TABLE_NOT_FOUND,
  /**the database has to be stopped and restarted with more file space */
	DB_MUST_GET_MORE_FILE_SPACE,
	DB_TABLE_IS_BEING_USED,
  /**a record in an index would not fit on a compressed page, or it would
  become bigger than 1/2 free space in an uncompressed page frame */
	DB_TOO_BIG_RECORD,					 
  /**lock wait lasted too long */
	DB_LOCK_WAIT_TIMEOUT,
  /**referenced key value not found for a foreign key in an insert or
  update of a row */
	DB_NO_REFERENCED_ROW,				 
  /**cannot delete or update a row because it contains a key value
  which is referenced */
	DB_ROW_IS_REFERENCED,	
  /**adding a foreign key constraint to a table failed */
	DB_CANNOT_ADD_CONSTRAINT,
  /**data structure corruption noticed */
	DB_CORRUPTION,							 
  /**dropping a foreign key constraint from a table failed */
	DB_CANNOT_DROP_CONSTRAINT,	
  /**no savepoint exists with the given name */
	DB_NO_SAVEPOINT,						 
  /**we cannot create a new single-table tablespace because a file of the same
	name already exists */
	DB_TABLESPACE_EXISTS,				
  /**tablespace was deleted or is being dropped right now */
	DB_TABLESPACE_DELETED,			 
  /**Attempt to delete a tablespace instance that was not found in the 
  tablespace hash table */
	DB_TABLESPACE_NOT_FOUND,		 
  /**lock structs have exhausted the buffer pool (for big transactions,
  InnoDB stores the lock structs in the buffer pool) */
	DB_LOCK_TABLE_FULL,	
  /**foreign key constraints activated by the operation would lead to a
  duplicate key in some table */
	DB_FOREIGN_DUPLICATE_KEY,		 
  /**when InnoDB runs out of the preconfigured undo slots, this can
	only happen when there are too many concurrent transactions */
	DB_TOO_MANY_CONCURRENT_TRXS, 
  /**when InnoDB sees any artefact or a feature that it can't recoginize or
  work with e.g., FT indexes created by a later version of the engine. */
	DB_UNSUPPORTED,							 
  /**a NOT NULL column was found to be NULL during table rebuild */
	DB_INVALID_NULL, 
  /**an operation that requires the persistent storage, used for recording
  table and index statistics, was requested but this storage does not
  exist itself or the stats for a given table do not exist */
	DB_STATS_DO_NOT_EXIST,			
  /**Foreign key constraint related cascading delete/update exceeds
  maximum allowed depth */
	DB_FOREIGN_EXCEED_MAX_CASCADE, 
  /**the child (foreign) table does not have an index that contains the
  foreign keys as its prefix columns */
	DB_CHILD_NO_INDEX,						 
  /**the parent table does not have an index that contains the
  foreign keys as its prefix columns */
	DB_PARENT_NO_INDEX,						 
  /**index column size exceeds maximum limit */
	DB_TOO_BIG_INDEX_COL,
  /**we have corrupted index */
	DB_INDEX_CORRUPT,							 
  /**the undo log record is too big */
	DB_UNDO_RECORD_TOO_BIG,
  /**Update operation attempted in a read-only transaction */
	DB_READ_ONLY,
  /**FTS Doc ID cannot be zero */
	DB_FTS_INVALID_DOCID,
  /**table is being used in foreign key check */
	DB_TABLE_IN_FK_CHECK,
  /**Modification log grew too big during online index creation */
	DB_ONLINE_LOG_TOO_BIG,
  /**Identifier name too long */
	DB_IDENTIFIER_TOO_LONG,
  /**FTS query memory exceeds result cache limit */
	DB_FTS_EXCEED_RESULT_CACHE_LIMIT,
  /**Temp file write failure */
	DB_TEMP_FILE_WRITE_FAIL,
  /**Cannot create specified Geometry data object */
	DB_CANT_CREATE_GEOMETRY_OBJECT,
  /**Cannot open a file */
	DB_CANNOT_OPEN_FILE,
	/**Too many words in a phrase */
	DB_FTS_TOO_MANY_WORDS_IN_PHRASE,
  /**tablespace was truncated */
	DB_TABLESPACE_TRUNCATED,
  /**Generic IO error */
	DB_IO_ERROR = 100,
  /**Failure to decompress a page after reading it from disk */
	DB_IO_DECOMPRESS_FAIL,
  /**Punch hole not supported by InnoDB */
	DB_IO_NO_PUNCH_HOLE,
  /**The file system doesn't support punch hole */
	DB_IO_NO_PUNCH_HOLE_FS,
  /**The tablespace doesn't support punch hole */
	DB_IO_NO_PUNCH_HOLE_TABLESPACE,
  /**Failure to decrypt a page after reading it from disk */
	DB_IO_DECRYPT_FAIL,
  /**The tablespace doesn't support encrypt */
	DB_IO_NO_ENCRYPT_TABLESPACE,
  /**Partial IO request failed */
	DB_IO_PARTIAL_FAILED,
  /**Transaction was forced to rollback by a higher priority transaction */
	DB_FORCED_ABORT,
  /**Table/clustered index is corrupted */
	DB_TABLE_CORRUPT,
  /**Invalid Filename */
	DB_WRONG_FILE_NAME,
  /**Compute generated value failed */
	DB_COMPUTE_VALUE_FAILED,
  /**Cannot add foreign constrain placed on the base column of stored column */
	DB_NO_FK_ON_S_BASE_COL,
	/**The following are partial failure codes */
	DB_FAIL = 1000,
	DB_OVERFLOW,
	DB_UNDERFLOW,
	DB_STRONG_FAIL,
	DB_ZIP_OVERFLOW,
	DB_RECORD_NOT_FOUND = 1500,
	DB_END_OF_INDEX,
  /**Generic error code for "Not found" type of errors */
	DB_NOT_FOUND,

	/**The following are API only error codes. */
  /**Column update or read failed because the types mismatch */
	DB_DATA_MISMATCH = 2000,
  /**If an API function expects the schema to be locked in exclusive mode
  and if it's not then that API function will return this error code */
	DB_SCHEMA_NOT_LOCKED,
  /**Secondary read needs a snapshot, if not returh this */
	DB_SNAPSHOT_ERROR,
  /**Failed to unpack record */
	DB_UNPACK_ERROR, 
  /**Failed to call DB::Get */
	DB_GET_ERROR,
	
	/**The following are only for x_protocol */
	DB_X_PROTOCOL_WRONG_VERSION = 3000,
	DB_X_PROTOCOL_EINVAL,
	DB_X_PROTOCOL_INVISIBLE_CONFIG,
	DB_RES_OVERFLOW,
	/**meta information in container is invalid */
	DB_META_INVAL
};

}  // namespace se

typedef se::dberr_t se_err_t;

#define SE_SQL_NULL 0xFFFFFFFF
#define SE_CFG_BINLOG_ENABLED 0x1
#define SE_CFG_MDL_ENABLED 0x2
#define SE_CFG_DISABLE_ROWLOCK 0x4

/** @enum se_col_type_t  column types that are supported. */
typedef enum se_col_enum {
  SE_VARCHAR = 1, /* Character varying length. The
					column is not padded. */

  SE_CHAR = 2, /* Fixed length character string. The
					column is padded to the right. */

  SE_BINARY = 3, /* Fixed length binary, similar to
					SE_CHAR but the column is not padded
					to the right. */

  SE_VARBINARY = 4, /* Variable length binary */

  SE_BLOB = 5, /* Binary large object, or
					a TEXT type */

  SE_INT = 6, /* Integer: can be any size
					from 1 - 8 bytes. If the size is
					1, 2, 4 and 8 bytes then you can use
					the typed read and write functions. For
					other sizes you will need to use the
					se_col_get_value() function and do the
					conversion yourself. */

  SE_SYS = 8, /* System column, this column can
					be one of DATA_TRX_ID, DATA_ROLL_PTR
					or DATA_ROW_ID. */

  SE_FLOAT = 9, /* C (float)  floating point value. */

  SE_DOUBLE = 10, /* C (double) floating point value. */

  SE_DECIMAL = 11, /* Decimal stored as an ASCII string */

  SE_VARCHAR_ANYCHARSET = 12, /* Any charset, varying length */

  SE_CHAR_ANYCHARSET = 13, /* Any charset, fixed length */

	SE_DATETIME = 14,

	SE_DATETIME2 = 15,

	SE_NEWDECIMAL = 16
} se_col_type_t;

/** @enum se_col_attr_t SMARTENGINE column attributes */
typedef enum {
  SE_COL_NONE = 0, /* No special attributes. */

  SE_COL_NOT_NULL = 1, /* Column data can't be NULL. */

  SE_COL_UNSIGNED = 2, /* Column is SE_INT and unsigned. */

  SE_COL_NOT_USED = 4, /* Future use, reserved. */

  SE_COL_CUSTOM1 = 8, /* Custom precision type, this is
					a bit that is ignored by SMARTENGINE and so
					can be set and queried by users. */

  SE_COL_CUSTOM2 = 16, /* Custom precision type, this is
					a bit that is ignored by SMARTENGINE and so
					can be set and queried by users. */

  SE_COL_CUSTOM3 = 32 /* Custom precision type, this is
					a bit that is ignored by SMARTENGINE and so
					can be set and queried by users. */
} se_col_attr_t;

typedef enum {
  SE_CLUSTERED = 1, /* clustered index */
  SE_UNIQUE = 2,    /* unique index */
  SE_SECONDARY = 8,      /* non-unique secondary */
  SE_CORRUPT = 16,  /* bit to store the corrupted flag
										in SYS_INDEXES.TYPE */
  SE_FTS = 32,      /* FTS index; can't be combined with the
											other flags */
  SE_SPATIAL = 64,  /* SPATIAL index; can't be combined with the
										other flags */
  SE_VIRTUAL = 128  /* Index on Virtual column */
} se_index_type_t;

/* Note: must match lock0types.h */
/** @enum ib_lck_mode_t InnoDB lock modes. */
typedef enum {
	SE_LOCK_IS = 0, /*!< Intention shared, an intention
					lock should be used to lock tables */

	SE_LOCK_IX, /*!< Intention exclusive, an intention
					lock should be used to lock tables */

	SE_LOCK_S, /*!< Shared locks should be used to lock rows */

	SE_LOCK_X, /*!< Exclusive locks should be used to lock rows*/

	SE_LOCK_TABLE_X, /*!< exclusive table lock */

	SE_LOCK_NONE, /*!< This is used internally to note consistent read */

	SE_LOCK_NUM = SE_LOCK_NONE /*!< number of lock modes */
} se_lck_mode_t;

/** @enum ib_srch_mode_t InnoDB cursor search modes for ib_cursor_moveto().
Note: Values must match those found in page0cur.h */
typedef enum {
	SE_CUR_G = 1, /*!< If search key is not found then
					position the cursor on the row that
					is greater than the search key */

	SE_CUR_GE = 2, /*!< If the search key not found then
					position the cursor on the row that
					is greater than or equal to the search
					key */

	SE_CUR_L = 3, /*!< If search key is not found then
					position the cursor on the row that
					is less than the search key */

	SE_CUR_LE = 4 /*!< If search key is not found then
					position the cursor on the row that
					is less than or equal to the search
					key */
} se_srch_mode_t;

typedef void *se_opaque_t;
typedef se_opaque_t SE_CHARset_t;
/** @struct se_col_meta_t SMARTENGINE column meta data. */
typedef struct {
  uint32_t type; /* Type of the column */

  uint32_t attr; /* Column attributes */

  uint16_t prtype; /* Precise type of the column*/

  uint32_t type_len; /* Length of type */

  uint16_t client_type; /* 16 bits of data relevant only to
					the client. SMARTENGINE doesn't care */

  SE_CHARset_t *charset; /* Column charset */
} se_col_meta_t;

/* Note: Must be in sync with trx0trx.h */
/** @enum se_trx_level_t Transaction isolation levels */
typedef enum {
  SE_TRX_READ_UNCOMMITTED = 0, /* Dirty read: non-locking SELECTs are
					performed so that we do not look at a
					possible earlier version of a record;
					thus they are not 'consistent' reads
					under this isolation level; otherwise
					like level 2 */

  SE_TRX_READ_COMMITTED = 1, /* Somewhat Oracle-like isolation,
					except that in range UPDATE and DELETE
					we must block phantom rows with
					next-key locks; SELECT ... FOR UPDATE
					and ...  LOCK IN SHARE MODE only lock
					the index records, NOT the gaps before
					them, and thus allow free inserting;
					each consistent read reads its own
					snapshot */

  SE_TRX_REPEATABLE_READ = 2, /* All consistent reads in the same
					trx read the same snapshot; full
					next-key locking used in locking reads
					to block insertions into gaps */

  SE_TRX_SERIALIZABLE = 3 /* All plain SELECTs are converted to
					LOCK IN SHARE MODE reads */
} se_trx_level_t;

/** @enum se_match_mode_t Various match modes used by se_iter_seek() */
typedef enum {
  SE_CLOSEST_MATCH, /* Closest match possible */

  SE_EXACT_MATCH, /* Search using a complete key
					value */

  SE_EXACT_PREFIX /* Search using a key prefix which
					must match to rows: the prefix may
					contain an incomplete field (the
					last field in prefix may be just
					a prefix of a fixed length column) */
} se_match_mode_t;

typedef struct {
	void *db_snapshot_;
} se_trx_t;

typedef void *se_key_def_t;

struct se_table_define {
	int64_t table_name_len_;
	int64_t db_name_len_;
	const char *table_name_;
	const char *db_name_;
	int64_t null_bytes_;
	unsigned char *null_mask_array_;
	int64_t *null_byte_offset_array_;
	int64_t col_buffer_size_;
	int64_t col_array_len_;
	int64_t *col_length_array_;
	void **field_array_;
	const char **col_name_array_;
	void *db_obj_; 	// smartengine::SeTableDef *db_obj_
	void *table_; 	// TABLE *table_
	se_key_def_t pk_def_;
	int64_t pk_parts_;
};

typedef struct se_table_define *se_table_def_t;

struct se_request {
	void *buf_;
	se_table_def_t table_def_;
	se_match_mode_t match_mode_;
	se_trx_t *read_trx_;
	int64_t lock_type_;
	void *thd_;
};

typedef struct {
	int64_t size_;
	char *data_;
	int64_t *offset_array_;
	uint64_t *length_array_; // use uint64_t to include SE_SQL_NULL
	char **data_array_;
	int64_t *null_offset_array_;
	unsigned char *null_mask_array_;
	int64_t key_parts_;
  void *buffer_;
} se_tuple_t;

typedef struct {
	se_key_def_t key_def_;
	uint32_t cf_id_;
	uint32_t index_number_;
	void *iter_;
	se_tuple_t key_tuple_;  // just a buffer, different from Iterator::key
	se_tuple_t value_tuple_;
	char *upper_bound_;
	uint64_t upper_bound_size_;
	char *lower_bound_;
	uint64_t lower_bound_size_;
	se_bool_t is_primary_;
	int64_t seek_key_parts_;
} se_iter_t;

typedef struct se_request *se_request_t;
typedef se_tuple_t *se_tpl_t;

/* Open an smartengine table by name and return a cursor handle to it.
@return DB_SUCCESS or err code */
se_err_t se_open_table(
		const char *db_name,				/* in: db name */
		const char *table_name,			/* in: table name */
		void *thd,
		void *mysql_table,
		int64_t lock_type,
		se_request_t *req); /* out, own: smartengine request context */

/* Start a transaction that's been rolled back. This special function
exists for the case when smartengine's deadlock detector has rolledack
a transaction. While the transaction has been rolled back the handle
is still valid and can be reused by calling this function. If you
don't want to reuse the transaction handle then you can free the handle
by calling se_trx_release().
@return smartengine txn handle */
se_err_t se_trx_start(
		se_trx_t *trx,						 	/* in: transaction to restart */
		se_trx_level_t trx_level, 	/* in: trx isolation level */
		se_bool_t read_write,		  /* in: true if read write transaction */
		se_bool_t auto_commit,			/* in: auto commit after each single DML */
		void *thd);								 			/* in: THD */

/* Begin a transaction. This will allocate a new transaction handle and
put the transaction in the active state.
@return smartengine txn handle */
se_trx_t *se_trx_begin(
		se_trx_level_t trx_level, 	/* in: trx isolation level */
		se_bool_t read_write,			/* in: true if read write transaction */
		se_bool_t auto_commit);		/* in: auto commit after each single DML */

/* Check if the transaction is read_only */
uint32_t se_trx_read_only(
		se_trx_t *trx); 					 /* in: trx handle */

/* Release the resources of the transaction. If the transaction was
selected as a victim by smartengine and rolled back then use this function
to free the transaction handle.
@return DB_SUCCESS or err code */
se_err_t se_trx_release(
		se_trx_t *se_trx); /* in: trx handle */

/* Commit a transaction. This function will release the schema latches too.
It will also free the transaction handle.
@return DB_SUCCESS or err code */
se_err_t se_trx_commit(
		se_trx_t *se_trx); /* in: trx handle */

/* Rollback a transaction. This function will release the schema latches too.
It will also free the transaction handle.
@return DB_SUCCESS or err code */
se_err_t se_trx_rollback(
		se_trx_t *se_trx); /* in: trx handle */

/* Open an smartengine table and return a cursor handle to it.
@return DB_SUCCESS or err code */
se_err_t se_open_table_using_id(
		se_id_u64_t table_id, 	/* in: table id of table to open */
		se_trx_t *se_trx,	/* in: Current transaction handle can be NULL */
		se_request_t *req);		/* out,own: smartengine cursor */

/* Open an smartengine secondary index cursor and return a cursor handle to it.
@return DB_SUCCESS or err code */
se_err_t se_open_index_using_name(
		se_request_t req, 				/* in: open/active req */
		const char *index_name, 			/* in: secondary index name */
		se_iter_t **iter,					/* out,own: smartengine index iter */
		int *idx_type,								/* out: index is cluster index */
		se_id_u64_t *idx_id);		/* out: index id */

/* Reset the cursor.
@return DB_SUCCESS or err code */
se_err_t se_reset(se_request_t se_crsr); /* in/out: smartengine cursor */

/* Close an smartengine table and free the cursor.
@return DB_SUCCESS or err code */
se_err_t se_close(se_request_t se_crsr); /* in/out: smartengine cursor */

/* Close the table, decrement n_ref_count count.
@return DB_SUCCESS or err code */
se_err_t se_close_table(se_request_t se_crsr); /* in/out: smartengine cursor */

/* update the cursor with new transactions and also reset the cursor
@return DB_SUCCESS or err code */
se_err_t se_new_trx(se_request_t se_crsr, 	/* in/out: smartengine cursor */
		                se_trx_t *se_trx);		/* in: transaction */

/* Commit the transaction in a cursor
@return DB_SUCCESS or err code */
se_err_t se_commit_trx(
		se_request_t se_crsr, 	/* in/out: smartengine cursor */
		se_trx_t *se_trx);		/* in: transaction */

/* Insert a row to a table.
@return DB_SUCCESS or err code */
se_err_t se_insert_row(
		se_request_t se_crsr,			/* in/out: smartengine cursor instance */
		const se_tpl_t se_tpl); /* in: tuple to insert */

/*
Update a row in a table.
@return DB_SUCCESS or err code */
se_err_t se_update_row(
		se_request_t se_crsr,					/* in: smartengine cursor instance */
		const se_tpl_t se_old_tpl,	/* in: Old tuple in table */
		const se_tpl_t se_new_tpl); /* in: New tuple to update */

/* Delete a row in a table.
@return DB_SUCCESS or err code */
se_err_t se_delete_row(se_request_t se_crsr); /* in: cursor instance */

se_err_t se_pk_search(
		se_request_t req,
		se_tuple_t *key_tuple,
		se_tuple_t *value_tuple);

/* Read current row.
@return DB_SUCCESS or err code */
se_err_t se_read_row(
		se_request_t req,
		se_iter_t *iter,		 /* in: smartengine cursor instance */
		se_tpl_t cmp_tpl,			 /* in: tuple to compare and stop
					reading */
		int mode,							 /* in: mode determine when to
					stop read */
		void **row_buf,				 	/* in/out: row buffer */
		se_ulint_t *row_len,	 	/* in/out: row buffer len */
		se_ulint_t *used_len); /* in/out: row buffer len used */

/* Move cursor to the first record in the table.
@return DB_SUCCESS or err code */
se_err_t se_iter_first(
		se_iter_t *iter);

/* Move cursor to the next record in the table.
@return DB_SUCCESS or err code */
se_err_t se_iter_next(
		se_iter_t *iter);

/* Search for key.
@return DB_SUCCESS or err code */
se_err_t se_iter_seek(
		se_request_t req,					 /* in: smartengine cursor instance */
		se_tpl_t se_tpl,						 /* in: Key to search for */
		se_iter_t *iter,
		se_srch_mode_t se_srch_mode, /* in: search mode */
		se_ulint_t direction);			 /* in: search direction */

/* Set the match mode for se_move(). */
void se_set_match_mode(
		se_request_t se_crsr,					 /* in: Cursor instance */
		se_match_mode_t match_mode); /* in: se_iter_seek match mode */

/* Set a column of the tuple. Make a copy using the tuple's heap.
@return DB_SUCCESS or error code */
se_err_t se_col_set_value(
		se_tpl_t se_tpl,		 /* in: tuple instance */
		se_ulint_t col_no,	 /* in: column index in tuple */
		const void *src,		 /* in: data value */
		se_ulint_t len,			 /* in: data value len */
		se_bool_t need_cpy); /* in: if need memcpy */

/* Get the size of the data available in the column the tuple.
@return bytes avail or SE_SQL_NULL */
se_ulint_t se_col_get_len(
		se_tpl_t se_tpl, 	/* in: tuple instance */
		se_ulint_t i);	 				/* in: column index in tuple */

/* Copy a column value from the tuple.
@return bytes copied or SE_SQL_NULL */
se_ulint_t se_col_copy_value(
		se_tpl_t se_tpl, 	/* in: tuple instance */
		se_ulint_t i,		 			/* in: column index in tuple */
		void *dst,			 	 					/* out: copied data value */
		se_ulint_t len);  			/* in: max data value len to copy */

/* Read a signed int 8 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_i8(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 			/* in: column number */
		se_i8_t *ival);				/* out: integer value */

/* Read an unsigned int 8 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_u8(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 			/* in: column number */
		se_u8_t *ival);				/* out: integer value */

/* Read a signed int 16 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_i16(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 			/* in: column number */
		se_i16_t *ival); 			/* out: integer value */

/* Read an unsigned int 16 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_u16(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,					/* in: column number */
		se_u16_t *ival); 			/* out: integer value */

/* Read a signed int 24 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_i24(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 	/* in: column number */
		se_i32_t *ival); 	/* out: integer value */

/* Read an unsigned int 24 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_u24(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 	/* in: column number */
		se_u32_t *ival); 	/* out: integer value */

/* Read a signed int 32 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_i32(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 	/* in: column number */
		se_i32_t *ival); 	/* out: integer value */

/* Read an unsigned int 32 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_u32(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 	/* in: column number */
		se_u32_t *ival); 	/* out: integer value */

/* Read a signed int 64 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_i64(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 			/* in: column number */
		se_i64_t *ival); 			/* out: integer value */

/* Read an unsigned int 64 bit column from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_u64(
		se_tpl_t se_tpl, 	/* in: smartengine tuple */
		se_ulint_t i,		 			/* in: column number */
		se_u64_t *ival); 			/* out: integer value */

/* Get a column value pointer from the tuple.
@return NULL or pointer to buffer */
const void *se_col_get_value(
		se_tuple_t *tpl, 	/* in: smartengine tuple */
		se_ulint_t i);	 		/* in: column number */

/* Get a column type, length and attributes from the tuple.
@return len of column data */
se_ulint_t se_col_get_meta(
		se_request_t req,
		se_tuple_t *tpl,						 		/* in: smartengine tuple */
		se_ulint_t i,								 	/* in: column number */
		se_col_meta_t *col_meta); 			/* out: column meta data */

/* "Clear" or reset an smartengine tuple. We free the heap and recreate the tuple.
@return new tuple, or NULL */
void se_tuple_clear(
		se_iter_t **iter,
		se_tuple_t **tpl); 				/* in: smartengine tuple */

/* Create a new cluster key search tuple and copy the contents of  the
secondary index key tuple columns that refer to the cluster index record
to the cluster key. It does a deep copy of the column data.
@return DB_SUCCESS or error code */
se_err_t se_tuple_get_cluster_key(
		se_request_t se_crsr,					/* in: secondary index cursor */
		se_tpl_t *se_dst_tpl,					/* out,own: destination tuple */
		const se_tpl_t se_src_tpl); 	/* in: source tuple */

/* Create an smartengine tuple used for index/table search.
@return tuple for current index */
se_err_t se_key_tuple_create(
		se_request_t req,
		se_key_def_t key_def,
		int64_t key_parts,
		se_tuple_t **tpl); /* in: Cursor instance */

se_err_t se_value_tuple_create(
		se_table_def_t table_def,
    se_tuple_t **tuple);

/* Create an smartengine tuple used for index/table search.
@return tuple for current index */
se_err_t se_sec_iter_create(
		se_request_t req,
		const char *index_name,
		se_iter_t **iter); /* in: Cursor instance */

/* Create an smartengine tuple used for table key operations.
@return tuple for current table */
se_err_t se_clust_search_tuple_create(
		se_request_t req,
		se_iter_t *iter); /* in: Cursor instance */

/* Create an smartengine tuple for table row operations.
@return tuple for current table */
se_err_t se_clust_iter_create(
		se_request_t req,
		se_iter_t **iter);

/* Return the number of user columns in the tuple definition.
@return number of user columns */
se_ulint_t se_iter_get_n_user_cols(
		const se_iter_t *iter); /* in: Tuple for current table */

/* Return the number of columns in the tuple definition.
@return number of columns */
se_ulint_t se_iter_get_n_cols(
		const se_iter_t *iter); /* in: Tuple for current table */

/* Sets number of fields used in record comparisons.*/
void se_tuple_set_n_fields_cmp(
		const se_tpl_t se_tpl,	/* in: Tuple for current index */
		uint32_t n_fields_cmp); 	/* in: number of fields used in
					comparisons*/

/* Destroy an smartengine tuple. */
void se_delete_iter(
		se_iter_t *iter);	/* in,own: Iter instance to delete */

void se_delete_tuple(
		se_tuple_t *tuple);

/* Truncate a table. The cursor handle will be closed and set to NULL
on success.
@return DB_SUCCESS or error code */
se_err_t se_truncate(
		se_request_t *se_crsr,			/* in/out: cursor for table
					to truncate */
		se_id_u64_t *table_id); 	/* out: new table id */

/* Get a table id.
@return DB_SUCCESS if found */
se_err_t se_table_get_id(
		const char *table_name, 	/* in: table to find */
		se_id_u64_t *table_id); 	/* out: table id if found */

/* Check if cursor is positioned.
@return se_TRUE if positioned */
se_bool_t se_is_positioned(
		const se_request_t se_crsr); /* in: smartengine cursor instance */

/* Checks if the data dictionary is latched in exclusive mode by a
user transaction.
@return TRUE if exclusive latch */
se_bool_t se_schema_lock_is_exclusive(
		const se_trx_t *se_trx); 	/* in: transaction */

/* Lock an smartengine cursor/table.
@return DB_SUCCESS or error code */
se_err_t se_lock(
		se_request_t se_crsr,					/* in/out: smartengine cursor */
		se_lck_mode_t se_lck_mode); /* in: smartengine lock mode */

/* Set the Lock an smartengine table using the table id.
@return DB_SUCCESS or error code */
se_err_t se_table_lock(
		se_trx_t *se_trx,						/* in/out: transaction */
		se_id_u64_t table_id,				/* in: table id */
		se_lck_mode_t se_lck_mode); /* in: smartengine lock mode */

/* Set the Lock mode of the cursor.
@return DB_SUCCESS or error code */
se_err_t se_set_lock_mode(
		se_request_t se_crsr,					/* in/out: smartengine cursor */
		se_lck_mode_t se_lck_mode); /* in: smartengine lock mode */

/* Set need to access clustered index record flag. */
void se_set_cluster_access(
		se_request_t se_crsr); /* in/out: smartengine cursor */

/* Inform the cursor that it's the start of an SQL statement. */
void se_stmt_begin(
		se_request_t se_crsr); /* in: cursor */

/* Write a double value to a column.
@return DB_SUCCESS or error */
se_err_t se_tuple_write_double(
		se_tpl_t se_tpl, /* in: smartengine tuple */
		int col_no,			 /* in: column number */
		double val);		 /* in: value to write */

/* Read a double column value from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_double(
		se_tpl_t se_tpl,	 	/* in: smartengine tuple */
		se_ulint_t col_no, 	/* in: column number */
		double *dval);		 		/* out: double value */

/* Write a float value to a column.
@return DB_SUCCESS or error */
se_err_t se_tuple_write_float(
		se_tpl_t se_tpl, 	/* in/out: tuple to write to */
		int col_no,			 		/* in: column number */
		float val);			 		/* in: value to write */

/* Read a float value from an smartengine tuple.
@return DB_SUCCESS or error */
se_err_t se_tuple_read_float(
		se_tpl_t se_tpl,	 	/* in: smartengine tuple */
		se_ulint_t col_no, 	/* in: column number */
		float *fval);			 		/* out: float value */

/* Get a column type, length and attributes from the tuple.
@return len of column data */
const char *se_col_get_name(
		se_request_t req, 		/* in: smartengine request instance */
		se_ulint_t i);		 		/* in: column index in tuple */

/* Get an index field name from the cursor.
@return name of the field */
const char *se_get_idx_field_name(
		se_request_t req, 	/* in: smartengine req instance */
		se_iter_t *iter,		/* iter */
		se_ulint_t i);		 	/* in: column index in tuple */

/* Truncate a table.
@return DB_SUCCESS or error code */
se_err_t se_table_truncate(
		const char *table_name, /* in: table name */
		se_id_u64_t *table_id); /* out: new table id */

/* Get generic configure status
@return configure status*/
int se_cfg_get_cfg();

/* Increase/decrease the memcached sync count of table to sync memcached
DML with SQL DDLs.
@return DB_SUCCESS or error number */
se_err_t se_set_memcached_sync(
		se_request_t se_crsr, 	/* in: cursor */
		se_bool_t flag);	 		/* in: true for increasing */

/* Return isolation configuration set by "smartengine_api_trx_level"
@return trx isolation level*/
se_trx_level_t se_cfg_trx_level();

/* Return configure value for background commit interval (in seconds)
@return background commit interval (in seconds) */
se_ulint_t se_cfg_bk_commit_interval();

/* Get a trx start time.
@return trx start_time */
se_u64_t se_trx_get_start_time(
		se_trx_t *se_trx); /* in: transaction */

/* Wrapper of ut_strerr() which converts an smartengine error number to a
human readable text message.
@return string, describing the error */
const char *se_ut_strerr(se_err_t num); /* in: error number */

/** Check the table whether it contains virtual columns.
@param[in]	crsr	smartengine Cursor
@return true if table contains virtual column else false. */
se_bool_t se_is_virtual_table(
		se_request_t crsr);

/**
 * set the tuple to be key type
 * @param se_tpl
 * */
void se_set_tuple_key(
		se_tpl_t se_tpl);

/**SmartEngine API callback function array*/
#define SE_API_FUNCS                    \
  (se_cb_t)se_open_table,               \
  (se_cb_t)se_read_row,                 \
  (se_cb_t)se_insert_row,               \
  (se_cb_t)se_delete_row,               \
  (se_cb_t)se_update_row,               \
  (se_cb_t)se_iter_seek,                \
  (se_cb_t)se_iter_first,               \
  (se_cb_t)se_iter_next,                \
  (se_cb_t)se_set_match_mode,           \
  (se_cb_t)se_key_tuple_create,         \
  (se_cb_t)se_value_tuple_create,       \
  (se_cb_t)se_clust_iter_create,        \
  (se_cb_t)se_sec_iter_create,          \
  (se_cb_t)se_delete_iter,              \
  (se_cb_t)se_tuple_read_u8,            \
  (se_cb_t)se_tuple_read_u16,           \
  (se_cb_t)se_tuple_read_u24,           \
  (se_cb_t)se_tuple_read_u32,           \
  (se_cb_t)se_tuple_read_u64,           \
  (se_cb_t)se_tuple_read_i8,            \
  (se_cb_t)se_tuple_read_i16,           \
  (se_cb_t)se_tuple_read_i24,           \
  (se_cb_t)se_tuple_read_i32,           \
  (se_cb_t)se_tuple_read_i64,           \
  (se_cb_t)se_tuple_read_float,         \
  (se_cb_t)se_tuple_read_double,        \
  (se_cb_t)se_iter_get_n_cols,          \
  (se_cb_t)se_col_set_value,            \
  (se_cb_t)se_col_get_value,            \
  (se_cb_t)se_col_get_meta,             \
  (se_cb_t)se_trx_begin,                \
  (se_cb_t)se_trx_commit,               \
  (se_cb_t)se_trx_rollback,             \
  (se_cb_t)se_trx_start,                \
  (se_cb_t)se_trx_release,              \
  (se_cb_t)se_lock,                     \
  (se_cb_t)se_close_table,              \
  (se_cb_t)se_new_trx,                  \
  (se_cb_t)se_delete_tuple,             \
  (se_cb_t)se_col_get_name,             \
  (se_cb_t)se_table_truncate,           \
  (se_cb_t)se_open_index_using_name,    \
  (se_cb_t)se_cfg_get_cfg,              \
  (se_cb_t)se_set_memcached_sync,       \
  (se_cb_t)se_set_cluster_access,       \
  (se_cb_t)se_commit_trx,               \
  (se_cb_t)se_cfg_trx_level,            \
  (se_cb_t)se_iter_get_n_user_cols,     \
  (se_cb_t)se_set_lock_mode,            \
  (se_cb_t)se_get_idx_field_name,       \
  (se_cb_t)se_trx_get_start_time,       \
  (se_cb_t)se_cfg_bk_commit_interval,   \
  (se_cb_t)se_ut_strerr,                \
  (se_cb_t)se_stmt_begin,               \
  (se_cb_t)se_trx_read_only,            \
  (se_cb_t)se_is_virtual_table,         \
  (se_cb_t)se_set_tuple_key,            \
  (se_cb_t)se_tuple_set_n_fields_cmp,   \
  (se_cb_t)se_pk_search

