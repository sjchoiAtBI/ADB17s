/* For BF */
typedef struct BFpage {
    PFpage         fpage;       /* page data from the file                 */
    struct BFpage  *nextpage;   /* next in the linked list of buffer pages */
    struct BFpage  *prevpage;   /* prev in the linked list of buffer pages */
    bool_t         dirty;       /* TRUE if page is dirty                   */
    short          count;       /* pin count associated with the page      */
    int            unixfd;      /* Unix file descriptor                    */
    int            fd;          /* PF file descriptor of this page         */
    int            pageNum;     /* page number of this page                */
} BFpage;

int find_in_disk(BFreq bq, BFpage **bfpage);
int insert_in_LRU(BFpage* bfpage);

#include "uthash.h"

typedef struct BFhash_entry {
    struct BFhash_entry *nextentry;     /* next hash table element or NULL */
    struct BFhash_entry *preventry;     /* prev hash table element or NULL */
    int fd;                             /* file descriptor                 */
    int pageNum;                        /* page number                     */
    struct BFpage *bpage;               /* ptr to buffer holding this page */
    UT_hash_handle hh;                  /* It make this structure hashable. See http://troydhanson.github.io/uthash/ */
} BFhash_entry;

void BF_InitHash();
int BF_InsertHash(BFpage *bpage);
int BF_SearchHash(int fd, int pageNum, BFhash_entry **hash_entry);
int BF_DeleteHash(int fd, int pageNum, BFpage **bpage);
int PF_IsValidPage(int fd, int pagenum);
int PF_GetNumPages(int fd, int * pagenum);

/*
 * PF file table.
 */

#define PFTAB_INVALID NULL

/* PF file header structure definition */
typedef struct PFhdr_str {
	int numpages; /* contains page number of the corresponding PF file */
	char hdrrest[PF_PAGE_SIZE]; /* empty space, to be utilized later */
} PFhdr_str;

/* PF file table element structure definition */
typedef struct PFftab_ele {
	bool_t valid; /* validity of this file table entry, TRUE if corresponding file is valid and open */
	ino_t inode; /* inode number of this PF file. retrieved from UNIX inode */
	char *fname; /* file name */
	int unixfd; /* UNIX file descriptor of the file */
	PFhdr_str hdr; /* PF file header */
	short hdrchanged; /* TRUE if PF file header was changed after it was allocated */
} PFftab_ele;

/* pointer to the array of PF file table elements */
PFftab_ele *pft;


/*
 * HF file table.
 */

typedef struct {
    int RecSize;                 /* Record size */
    int RecPage;                 /* Number of records per page */
    int NumPg;                   /* Number of pages in file */
    int NumFrPgFile;             /* Number of free pages in the file */
} HFHeader;
