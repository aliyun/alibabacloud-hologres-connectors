#ifndef _KEYWORDS_H_
#define _KEYWORDS_H_

#include <stdint.h>

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3

typedef struct ScanKeyword
{
	const char *name;			/* in lower case */
	int16_t	   value;			/* grammar's token code */
	int16_t	   category;		/* see codes above */
} ScanKeyword;

extern const ScanKeyword ScanKeywords[];
extern const int NumScanKeywords;

extern const ScanKeyword *ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords);

#endif