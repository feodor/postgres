/*-------------------------------------------------------------------------
 * 
 * anyarray_guc.c
 *		GUC management
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/anyarray/anyarray_gud.c
 *
 *-------------------------------------------------------------------------
 */

#include "anyarray.h"

#include <utils/guc.h>

SimilarityType	SmlType = AA_Cosine;
double			SmlLimit = 0.6;

static bool AnyArrayInited = false;

static const struct config_enum_entry SmlTypeOptions[] = {
	{"cosine", AA_Cosine, false},
	{"overlap", AA_Overlap, false},
	{NULL, 0, false}
};

static void
initAnyArray()
{
	if (AnyArrayInited)
		return;

	DefineCustomRealVariable(
		"anyarray.similarity_threshold",
		"Lower threshold of array's similarity",
		"Array's with similarity lower than threshold are not similar by % operation",
		&SmlLimit,
		SmlLimit,
		0.0,
		1.0,
		PGC_USERSET,
		0,
		NULL,
		NULL,
		NULL
	);
	DefineCustomEnumVariable(
		"anyarray.similarity_type",
		"Type of similarity formula",
		"Type of similarity formula: cosine(default), overlap",
		(int*)&SmlType,
		SmlType,
		SmlTypeOptions,
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL
	);
}

void _PG_init(void);
void
_PG_init(void)
{
	initAnyArray();
}

void _PG_fini(void);
void
_PG_fini(void)
{
	return;
}


