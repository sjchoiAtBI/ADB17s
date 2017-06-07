#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "minirel.h"
#include "pf.h"
#include "hf.h"
#include "am.h"
#include "custom.h"

#define FOPEN_NOFILE (-1)
#define AMI_PFD_INVALID (-1)
#define AMIHDR_INVALID (-1)
#define INT_LEN 4
#define REAL_LEN 4
#define STR_MINLEN 1
#define STR_MAXLEN 255
#define NODE_ROOT 'r'
#define NODE_INT 'i'
#define NODE_LEAF 'l'
#define NODE_NULLPTR (-1)
#define NODE_INTNULL (-2)
#define NODE_PARENT (-3)
#define LEAFIDX_PREV (-1)
#define LEAFIDX_NEXT (-2)
#define BTR_LT (-1)
#define BTR_EQ 0
#define BTR_GT 1
#define BTR_NODENUM_INIT 3
#define INAME_LEN 1000
#define ITOA_DECIMAL 10

/*
	when set to 1, all the messages supposed to print by this code are ignored.
	when set to 0, shows all the messages written in this code using 'printf'
*/
#define __MUTE__ 1

#if __MUTE__
#define printf myPrintf
#endif

/* dummy function to replace printf, when debugging mode is off */
void myPrintf(const char *fmt, ...) {

}

int AMerrno;

/* struct of AM index table's header */
typedef struct AMhdr_str{
	int indexNo;
	char attrType;
	int attrLength;
	int maxKeys; /* maximum number of keys a B+ tree node can contain */
	int numNodes; /* number of nodes the B+ tree corresponding to this AM index table element now contains */
	int numRecs; /* number of records the B+ tree corresponding to this AM index table element now contains */
	bool_t isUnique;
	RECID root; /* contains the page number to root node of the B+ tree */
} AMhdr_str;

/* struct of AM index table element */
typedef struct AMitab_ele{
	bool_t valid;
	char * fname;
	int pfd;
	AMhdr_str hdr;
	short hdrchanged;
} AMitab_ele;

/* struct of AM scan table element */
typedef struct AMstab_ele {
	bool_t valid;
	int fd;
	char attrType;
	int attrLength;
	int op;
	char *value;
	RECID current;
	RECID currentNode;
}AMstab_ele;

/* struct of B+ tree node's header */
typedef struct Btr_nodeHdr{
	int entries; /* number of records this node contains */
	RECID parent; /* pointer to parent node */
	bool_t duplicate; /* TRUE if it only contains duplicate values */
} BtrHdr;

AMitab_ele *ait = NULL;
AMstab_ele *ast = NULL;

/*
	checks whether the index with the same file name exists

	*** parameters ***
	char * filename - name of the file corresponding to this index
	int indexNo - number of the index for the given file

	*** return values ***
	TRUE if there is an index with the given file name and index number
	FALSE otherwise
*/
bool_t AM_IndexExists(char * filename, int indexNo){
	int file_fd;
	char iname[INAME_LEN];
	char cache[INAME_LEN];
	strcpy(iname, filename);
	sprintf(cache, "%d", indexNo);
	strcat(iname, cache);

	file_fd = open(iname, O_RDONLY);
	if (file_fd != FOPEN_NOFILE){
		close(file_fd);
		return TRUE;
	}
	return FALSE;
}

/*
	checks whether the given attribute type and attribute length are both valid

	*** parameters ***
	char attrType - type of the attribute
	int attrLength - length of the given attribute

	*** return values ***
	TRUE if both 'attrType' and 'attrLength' are valid.
	FALSE otherwise
*/
int AM_validAttr(char attrType, int attrLength){
	if ((attrType != INT_TYPE) && (attrType != REAL_TYPE) && (attrType != STRING_TYPE)){
		printf("AM_validAttr failed: invalid attrType\n");
		return AME_INVALIDATTRTYPE;
	}
	if ((attrType == INT_TYPE) && (attrLength == INT_LEN)){
		return AME_OK;
	} else if ((attrType == REAL_TYPE) && (attrLength == REAL_LEN)){
		return AME_OK;
	} else if ((attrType == STRING_TYPE) && (attrLength >= STR_MINLEN) && (attrLength <= STR_MAXLEN)){
		return AME_OK;
	}

	printf("AM_validAttr failed: invalid attrLength\n");
	return AME_INVALIDATTRLENGTH;
}

/*
	initialize the AM index table
	- invoke HF_Init()
	- initialize AM index table
	- initialize AM scan table
*/
void AM_Init(void){
	int i;

	HF_Init();

	ait = (AMitab_ele *)calloc(AM_ITAB_SIZE, sizeof(AMitab_ele));
	for (i = 0; i < AM_ITAB_SIZE; i++){
		ait[i].valid = FALSE;
		ait[i].fname = NULL;
		ait[i].pfd = AMI_PFD_INVALID;
		ait[i].hdr.indexNo = AMIHDR_INVALID;
		ait[i].hdr.attrType = AMIHDR_INVALID;
		ait[i].hdr.attrLength = AMIHDR_INVALID;
		ait[i].hdr.maxKeys = AMIHDR_INVALID;
		ait[i].hdr.numNodes = AMIHDR_INVALID;
		ait[i].hdr.numRecs = AMIHDR_INVALID;
		ait[i].hdr.isUnique = FALSE;
		ait[i].hdrchanged = FALSE;
		ait[i].hdr.root.pagenum = NODE_NULLPTR;
		ait[i].hdr.root.recnum = NODE_INTNULL;
	}

	ast = (AMstab_ele *)calloc(MAXISCANS, sizeof(AMstab_ele));
	for (i = 0; i < MAXISCANS; i++){
		ast[i].valid = FALSE;
	}
}

/*
	initializes the specified B+ tree node's header

	*** parameters ***
	BtrHdr * hdr - pointer to the specified B+ tree node's header

	*** return values ***
	AME_OK if finished
*/
int Btr_initHdr(BtrHdr * hdr){
	hdr->entries = 0;
	hdr->parent.pagenum = NODE_NULLPTR; /* -1 */
	hdr->parent.recnum = NODE_NULLPTR;
	hdr->duplicate = FALSE;
	return AME_OK;
}

/*
	assigns a new B+ tree node

	*** parameters ***
	int pfd - file descriptor for the PF file table
	char nodeType - type of the B+ tree node (root, internal, leaf)
	int attrLength - attribute's length
	int keyNum - the maximum number of keys a B+ tree node can contain
	int * pagenum - pointer to the page number of newly assigned node if successful
	char ** pbuf - address of the pointer to the newly assigned B+ tree node if successful
	RECID parent - pointer to the parent of newly assigned B+ tree node

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_assignNode(int pfd, char nodeType, int attrLength, int keyNum, int *pagenum, char ** pbuf, RECID parent){
	int err;
	int initnum = 0;
	int *writeres;
	int i;
	RECID initRid;
	BtrHdr * bhdr;

	initRid.pagenum = NODE_NULLPTR;

	if ((err = PF_AllocPage(pfd, pagenum, pbuf)) == PFE_OK){
		printf("Btr_assignNode: assigned page number: %d\n", *pagenum);

		bhdr = (BtrHdr *) *pbuf;
		Btr_initHdr(bhdr);
		/* setting parent information */
		bhdr->parent.pagenum = parent.pagenum;
		printf("testing Btr_initHdr: BtrHdr_size: %d, hdr->entries: %d, hdr->parent.pagenum: %d, hdr->duplicate: %d\n", (int)sizeof(BtrHdr), bhdr->entries, bhdr->parent.pagenum, bhdr->duplicate);
		/* root & internal node */
		if ((nodeType == NODE_ROOT) || (nodeType == NODE_INT)){
			initRid.recnum = NODE_INTNULL;
			for (i = 0; i < keyNum; i++){
				if (memcpy(*pbuf + sizeof(BtrHdr) + i * (sizeof(RECID) + attrLength), &initRid, sizeof(RECID)) == NULL){
					printf("Btr_assignNode failed: 'memcpy' for assigning internal node entries at %d th iteration\n", i);
					return AME_UNIX;
				}
			}
			if (memcpy(*pbuf + sizeof(BtrHdr) + keyNum * (sizeof(RECID) + attrLength), &initRid, sizeof(RECID)) == NULL){
				printf("Btr_assignNode failed: 'memcpy' for assigning last pointer for internal node\n");
				return AME_UNIX;
			}
		}/* leaf node */
		else if (nodeType == NODE_LEAF){
			initRid.recnum = NODE_NULLPTR;
			if (memcpy(*pbuf + sizeof(BtrHdr), &initRid, sizeof(RECID)) == NULL){
				printf("Btr_assignNode failed: first pointer to adjacent leaf node\n");
				return AME_UNIX;
			}
			for (i = 0; i < keyNum; i++){
				if (memcpy(*pbuf + sizeof(BtrHdr) + sizeof(RECID) + i * (sizeof(RECID) + attrLength), &initRid, sizeof(RECID)) == NULL){
					printf("Btr_assignNode failed: 'memcpy' for assigning leaf node entries at %d th iteration\n", i);
					return AME_UNIX;
				}
			}
			if (memcpy(*pbuf + sizeof(BtrHdr) + sizeof(RECID) + keyNum * (sizeof(RECID) + attrLength), &initRid, sizeof(RECID)) == NULL){
				printf("Btr_assignNode failed: second pointer to adjacent leaf node\n");
				return AME_UNIX;
			}

		} else {
			printf("Btr_assignNode failed: invalid 'nodeType' %c\n", nodeType);
			return AME_INVALIDPARA;
		}

		return AME_OK;
	}

	return AME_PF;
}

/*
	retrieves a pointer to the B+ tree node

	*** parameters ***
	char ** pbuf - address of the pointer to the specified B+ tree node if successful
	int AM_fd - file descriptor of the AM index table
	RECID adr - pointer to the specified B+ tree node

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_getNode(char ** pbuf, int AM_fd, RECID adr){
	int pfd = ait[AM_fd].pfd;

	if (PF_GetThisPage(pfd, adr.pagenum, pbuf) != PFE_OK){
		printf("Btr_getNode failed: PF_GetThisPage \n");
		return AME_PF;
	}
	return AME_OK;
}

/*
	reads a pointer from a specified position of B+ tree node

	*** parameters ***
	char ** pbuf - address to the B+ tree node
	char nodeType - type of the node (root, internal, leaf)
	int attrLength - attribute's length
	int idx - position of the key
	int keyNum - maximum number of keys a B+ tree node can contain
	RECID * rid - points to the pointer to contain the values read from the B+ tree node

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_getPtr(char ** pbuf, char nodeType, int attrLength, int idx, int keyNum, RECID * rid){

	BtrHdr * bhdr = (BtrHdr *) *pbuf;

	if (idx == NODE_PARENT){
		if (memcpy(rid, &(bhdr->parent), sizeof(RECID)) == NULL){
			printf("Btr_getPtr failed: 'memcpy' for reading ptr of parent node\n");
			return AME_UNIX;
		}
	} else if ((nodeType == NODE_ROOT) || (nodeType == NODE_INT)){
		if ((idx > keyNum) || (idx < 0)){
			printf("Btr_getPtr failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(rid, *pbuf + sizeof(BtrHdr) + idx * (sizeof(RECID) + attrLength), sizeof(RECID)) == NULL){
			printf("Btr_getPtr failed: 'memcpy' for reading the pointer of internal(root) node\n");
			return AME_UNIX;
		}
		printf("Btr_getPtr: retrieved rid's pagenum: %d, recnum: %d\n", rid->pagenum, rid->recnum);

	} else if (nodeType == NODE_LEAF){
		if ((idx > keyNum) || (idx < LEAFIDX_NEXT)){
			printf("Btr_getPtr failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (idx == LEAFIDX_PREV){
			if (memcpy(rid, *pbuf + sizeof(BtrHdr), sizeof(RECID)) == NULL){
				printf("Btr_getPtr failed: 'memcpy' for reading ptr of prev leaf node\n");
				return AME_UNIX;
			}
		} else if (idx == LEAFIDX_NEXT){
			if (memcpy(rid, *pbuf + sizeof(BtrHdr) + sizeof(RECID) + keyNum * (sizeof(RECID) + attrLength), sizeof(RECID)) == NULL){
				printf("Btr_getPtr failed: 'memcpy' for reading ptr of next leaf node\n");
				return AME_UNIX;
			}
		} else if (memcpy(rid, *pbuf + sizeof(BtrHdr) + sizeof(RECID) + idx * (sizeof(RECID) + attrLength), sizeof(RECID)) == NULL){
			printf("Btr_getPtr failed: 'memcpy' for reading the pointer of leaf node\n");
			return AME_UNIX;
		}
	} else {
		printf("Btr_getPtr failed: invalid 'nodeType'\n");
		return AME_INVALIDPARA;
	}
	return AME_OK;
}

/*
	reads a key from a specified position of B+ tree node

	*** parameters ***
	char ** pbuf - address to the B+ tree node
	char nodeType - type of the node (root, internal, leaf)
	int attrLength - attribute's length
	int idx - position of the key
	int keyNum - maximum number of keys a B+ tree node can contain
	char * value - points to the value to contain the value read from the specified position

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_getKey(char ** pbuf, char nodeType, int attrLength, int idx, int keyNum, char * value){
	if ((nodeType == NODE_ROOT) || (nodeType == NODE_INT)){
		if ((idx >= keyNum) || (idx < 0)){
			printf("Btr_getKey failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(value, *pbuf + sizeof(BtrHdr) + idx * (sizeof(RECID) + attrLength) + sizeof(RECID), attrLength) == NULL){
			printf("Btr_getKey failed: 'memcpy' for reading the key of internal(root) node \n");
			return AME_UNIX;
		}
	} else if (nodeType == NODE_LEAF) {
		if ((idx >= keyNum) || (idx < 0)){
			printf("Btr_getKey failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(value, *pbuf + sizeof(BtrHdr) + sizeof(RECID) + idx * (sizeof(RECID) + attrLength) + sizeof(RECID), attrLength) == NULL){
			printf("Btr_getKey failed: 'memcpy' for reading the key of leaf node\n");
			return AME_UNIX;
		}
	} else {
		printf("Btr_getKey failed: invalid 'nodeType'\n");
		return AME_INVALIDPARA;
	}
	return AME_OK;
}

/*
	sets the pointer at a specified position of B+ tree node with a given pointer values

	*** parameters ***
	char ** pbuf - address to the B+ tree node
	char nodeType - type of the node (root, internal, leaf)
	int attrLength - attribute's length
	int idx - position of the key
	int keyNum - maximum number of keys a B+ tree node can contain
	RECID * rid - points to the pointer to be written at the specified position

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_setPtr(char ** pbuf, char nodeType, int attrLength, int idx, int keyNum, RECID * rid){
	RECID temp;
	BtrHdr * bhdr = (BtrHdr *) pbuf;
	printf("Btr_setPtr: going to write pagenum %d, recnum %d at idx %d\n", rid->pagenum, rid->recnum, idx);
	if (idx == NODE_PARENT){
		if (memcpy(&(bhdr->parent), rid, sizeof(RECID)) == NULL){
			printf("Btr_setPtr failed: 'memcpy' for writing ptr of parent node\n");
			return AME_UNIX;
		}
	} else if ((nodeType == NODE_ROOT) || (nodeType == NODE_INT)){
		if ((idx > keyNum) || (idx < 0)){
			printf("Btr_setPtr failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(*pbuf + sizeof(BtrHdr) + idx * (sizeof(RECID) + attrLength), rid, sizeof(RECID)) == NULL){
			printf("Btr_setPtr failed: 'memcpy' for writing the pointer of internal(root) node\n");
			return AME_UNIX;
		}
		if (memcpy(&temp, *pbuf + sizeof(BtrHdr) + idx * (sizeof(RECID) + attrLength), sizeof(RECID)) == NULL){
			printf("Btr_setPtr failed: 'memcpy' for writing the pointer of internal(root) node\n");
			return AME_UNIX;
		}
		printf("Btr_setPtr: checking written rids: pagenum %d, recnum %d\n", temp.pagenum, temp.recnum);

	} else if (nodeType == NODE_LEAF){
		if ((idx > keyNum) || (idx < LEAFIDX_NEXT)){
			printf("Btr_setPtr failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (idx == LEAFIDX_PREV){
			if (memcpy(*pbuf + sizeof(BtrHdr), rid, sizeof(RECID)) == NULL){
				printf("Btr_setPtr failed: 'memcpy' for writing ptr of prev leaf node\n");
				return AME_UNIX;
			}
		} else if (idx == LEAFIDX_NEXT){
			if (memcpy(*pbuf + sizeof(BtrHdr) + sizeof(RECID) + keyNum * (sizeof(RECID) + attrLength), rid, sizeof(RECID)) == NULL){
				printf("Btr_setPtr failed: 'memcpy' for writing ptr of next leaf node\n");
				return AME_UNIX;
			}
		} else if (memcpy(*pbuf + sizeof(BtrHdr) + sizeof(RECID) + idx * (sizeof(RECID) + attrLength), rid, sizeof(RECID)) == NULL){
				printf("Btr_setPtr failed: 'memcpy' for writing the pointer of leaf node\n");
				return AME_UNIX;
		}
	} else {
		printf("Btr_setPtr failed: invalid 'nodeType'\n");
		return AME_INVALIDPARA;
	}
	return AME_OK;
}

/*
	sets the key at a specified position of B+ tree node with a given value

	*** parameters ***
	char ** pbuf - address to the B+ tree node
	char nodeType - type of the node (root, internal, leaf)
	int attrLength - attribute's length
	int idx - position of the key
	int keyNum - maximum number of keys a B+ tree node can contain
	char * value - points to the value to be written at the specified position

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_setKey(char ** pbuf, char nodeType, int attrLength, int idx, int keyNum, char * value){
	if ((nodeType == NODE_ROOT) || (nodeType == NODE_INT)){
		if ((idx >= keyNum) || (idx < 0)){
			printf("Btr_setKey failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(*pbuf + sizeof(BtrHdr) + idx * (sizeof(RECID) + attrLength) + sizeof(RECID), value, attrLength) == NULL){
			printf("Btr_setKey failed: 'memcpy' for reading the key of internal(root) node \n");
			return AME_UNIX;
		}
	} else if (nodeType == NODE_LEAF) {
		if ((idx >= keyNum) || (idx < 0)){
			printf("Btr_setKey failed: invalid 'idx' value: %d, keyNum: %d\n", idx, keyNum);
			return AME_INVALIDPARA;
		}
		if (memcpy(*pbuf + sizeof(BtrHdr) + sizeof(RECID) + idx * (sizeof(RECID) + attrLength) + sizeof(RECID), value, attrLength) == NULL){
			printf("Btr_setKey failed: 'memcpy' for reading the key of leaf node\n");
			return AME_UNIX;
		}
	} else {
		printf("Btr_setKey failed: invalid 'nodeType'\n");
		return AME_INVALIDPARA;
	}
	return AME_OK;
}

/*
	creates an index

	*** parameters ***
	char * fileName - points to the specified file's name
	int indexNo - index number for the specified file
	char attrType - attribute type of the index
	int attrLength - attribute length of the index
	bool_t isUnique - specifies whether this index must be unique, set to FALSE

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_CreateIndex(char *fileName, int indexNo, char attrType, int attrLength, bool_t isUnique){
	int err;
	int pfd;
	int keyNum;
	AMhdr_str amhdr;
	PFftab_ele *pfte;
	char iname[INAME_LEN];
	char cache[INAME_LEN];
	int pagenum[BTR_NODENUM_INIT];
	char * pbuf[BTR_NODENUM_INIT];
	RECID rid;
	RECID parent;
	RECID temp;

	/* initializing 'parent' */
	parent.pagenum = NODE_NULLPTR;
	parent.recnum = NODE_NULLPTR;

	strcpy(iname, fileName);
	sprintf(cache, "%d", indexNo);
	strcat(iname, cache);

	if (PF_CreateFile(iname) != PFE_OK){
		printf("AM_CreateIndex failed: PF_CreateFile with %s\n", iname);
		return AME_PF;
	}

	if ((pfd = PF_OpenFile(iname)) < PFE_OK) {
		printf("AM_CreateIndex failed: PF_OpenFile with %s\n", iname);
		return AME_PF;
	}

	pfte = &(pft[pfd]);

	if ((err = AM_validAttr(attrType, attrLength)) != AME_OK){
		printf("AM_CreateIndex failed: attribute type or length not valid\n");
		return err;
	}

	/* determine the number of pointers which can fit into each b+ tree node */
	keyNum = (PAGE_SIZE - sizeof(BtrHdr) - 2*sizeof(RECID)) / (sizeof(RECID) + attrLength);
	printf("RECID size: %d, attrLength: %d, num. of entries in each node: %d\n", (int)sizeof(RECID), attrLength, keyNum);

	amhdr.indexNo = indexNo;
	amhdr.attrType = attrType;
	amhdr.attrLength = attrLength;
	amhdr.maxKeys = keyNum;
	amhdr.numNodes = BTR_NODENUM_INIT; /* 3: one root node, two leaf nodes*/
	amhdr.numRecs = 0;
	amhdr.isUnique = FALSE;

	/* root node */
	if ((err = Btr_assignNode(pfd, NODE_ROOT, attrLength, keyNum, pagenum, &pbuf[0], parent)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_assignNode to NODE_ROOT\n");
		return err;
	}
	/* assigning root node's PF page number to AM header */
	amhdr.root.pagenum = pagenum[0];
	amhdr.root.recnum = NODE_NULLPTR;
	parent.pagenum = amhdr.root.pagenum;

	/* first child(leaf) node */
	if ((err = Btr_assignNode(pfd, NODE_LEAF, attrLength, keyNum, pagenum+1, &pbuf[1], parent)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_assignNode to first NODE_LEAF\n");
		return err;
	}
	rid.pagenum = pagenum[1];
	rid.recnum = NODE_INTNULL;
	if ((err = Btr_setPtr(&pbuf[0], NODE_ROOT, attrLength, 0, keyNum, &rid)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_setPtr at NODE_ROOT to first NODE_LEAF\n");
		return err;
	}
	if ((err = Btr_getPtr(&pbuf[0], NODE_ROOT, attrLength, 0, keyNum, &temp)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_setPtr at NODE_ROOT to second NODE_LEAF\n");
		return err;
	}
	/* second child(leaf) node */
	if ((err = Btr_assignNode(pfd, NODE_LEAF, attrLength, keyNum, pagenum+2, &pbuf[2], parent)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_assignNode to second NODE_LEAF\n");
		return err;
	}
	rid.pagenum = pagenum[2];
	if ((err = Btr_setPtr(&pbuf[0], NODE_ROOT, attrLength, 1, keyNum, &rid)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_setPtr at NODE_ROOT to second NODE_LEAF\n");
		return err;
	}
	if ((err = Btr_getPtr(&pbuf[0], NODE_ROOT, attrLength, 1, keyNum, &temp)) != AME_OK){
		printf("AM_CreateIndex failed: Btr_setPtr at NODE_ROOT to second NODE_LEAF\n");
		return err;
	}

	if((err = PF_UnpinPage(pfd, pagenum[0], TRUE)) != PFE_OK){
		printf("AM_CreateIndex failed: PF_UnpinPage of root node\n");
		return err;
	}

	/* links between leaf nodes */
	if ((err = Btr_getPtr(&pbuf[1], NODE_LEAF, attrLength, LEAFIDX_PREV, keyNum, &temp)) != AME_OK){
		printf("AM_CreateIndex failed: linking first leaf node to the second one\n");
		return err;
	}

	if ((err = Btr_getPtr(&pbuf[1], NODE_LEAF, attrLength, LEAFIDX_NEXT, keyNum, &temp)) != AME_OK){
		printf("AM_CreateIndex failed: linking first leaf node to the second one\n");
		return err;
	}

	rid.recnum = NODE_NULLPTR;
	if ((err = Btr_setPtr(&pbuf[1], NODE_LEAF, attrLength, LEAFIDX_NEXT, keyNum, &rid)) != AME_OK){
		printf("AM_CreateIndex failed: linking first leaf node to the second one\n");
		return err;
	}
	rid.pagenum = pagenum[1];
	if ((err = Btr_setPtr(&pbuf[2], NODE_LEAF, attrLength, LEAFIDX_PREV, keyNum, &rid)) != AME_OK){
		printf("AM_CreateIndex failed: linking second leaf node to the first one\n");
		return err;
	}
	if (memcpy(pfte->hdr.hdrrest, &amhdr, sizeof(AMhdr_str)) == NULL){
		printf("AM_CreateIndex failed: copying AMhdr_str to PF header\n");
		return AME_UNIX;
	}
	pfte->hdrchanged = TRUE;

	if((err = PF_UnpinPage(pfd, pagenum[1], TRUE)) != PFE_OK){
		printf("AM_CreateIndex failed: PF_UnpinPage of first leaf node\n");
		return err;
	}
	if((err = PF_UnpinPage(pfd, pagenum[2], TRUE)) != PFE_OK){
		printf("AM_CreateIndex failed: PF_UnpinPage of second node\n");
		return err;
	}

	return PF_CloseFile(pfd) == PFE_OK ? AME_OK : AME_PF;
}

/*
	destroys the specified index

	*** parameters ***
	char * fileName - points to the specified file's name
	int indexNo - index number for the specified file

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_DestroyIndex(char *fileName, int indexNo){
	char iname[INAME_LEN];
	char cache[INAME_LEN];
	strcpy(iname, fileName);
	sprintf(cache, "%d", indexNo);
	strcat(iname, cache);

	return PF_DestroyFile(iname) == PFE_OK ? AME_OK : AME_PF;
}

/*
	opens an AM index table entry

	*** parameters ***
	char * fileName - points to the specified file's name
	int indexNo - index number for the specified file

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_OpenIndex(char *fileName, int indexNo){
	int err;
	int pfd;
	int aid;
	char * pbuf;
	AMitab_ele * aite;
	RECID temp;

	char iname[INAME_LEN];
	char cache[INAME_LEN];

	strcpy(iname, fileName);
	sprintf(cache, "%d", indexNo);
	strcat(iname, cache);

	if ((pfd = PF_OpenFile(iname)) < PFE_OK) {
		printf("AM_OpenIndex failed: PF_OpenFile\n");
		return AME_PF;
	}

	for (aid = 0; aid < AM_ITAB_SIZE; aid++){
		aite = &(ait[aid]);
		if (aite->valid == FALSE){
			if (memcpy(&(aite->hdr), pft[pfd].hdr.hdrrest, sizeof(AMhdr_str)) == NULL || aite->hdr.maxKeys < 0 || aite->hdr.numNodes < 0 || aite->hdr.numRecs < 0 || aite->hdr.root.pagenum < 0) {
				PF_CloseFile(pfd);
				printf("AM_OpenIndex failed: copying AM header from the file to AM index table\n");
				return AME_PF;
			}
			aite->valid = TRUE;
			aite->fname = (char *)calloc(strlen(iname), sizeof(char));
			strcpy(aite->fname, iname);
			aite->pfd = pfd;

			return aid;
		}
	}

	PF_CloseFile(pfd);
	printf("AM_OpenIndex failed: AM index table full\n");
	return AME_FULLTABLE;
}


/*
	closes the specified AM index table entry

	*** parameters ***
	int AM_fd - file descriptor to the specified AM index table entry

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_CloseIndex(int AM_fd){
	int pfd = ait[AM_fd].pfd;
	PFftab_ele * pfte = &(pft[pfd]);
	pfte->hdrchanged = TRUE;

	if (memcpy(pfte->hdr.hdrrest, &(ait[AM_fd].hdr), sizeof(AMhdr_str)) == NULL){
		printf("AM_CloseIndex failed: copying AMhdr_str to PF header\n");
		return AME_UNIX;
	}

	if (PF_CloseFile(pfd) != PFE_OK){
		printf("AM_CloseIndex failed: PF_CloseFile\n");
		return AME_PF;
	}
	ait[AM_fd].valid = FALSE;
	ait[AM_fd].fname = NULL;
	ait[AM_fd].pfd = AMI_PFD_INVALID;
	ait[AM_fd].hdr.indexNo = AMIHDR_INVALID;
	ait[AM_fd].hdr.attrType = AMIHDR_INVALID;
	ait[AM_fd].hdr.attrLength = AMIHDR_INVALID;
	ait[AM_fd].hdr.maxKeys = AMIHDR_INVALID;
	ait[AM_fd].hdr.numNodes = AMIHDR_INVALID;
	ait[AM_fd].hdr.numRecs = AMIHDR_INVALID;
	ait[AM_fd].hdr.isUnique = FALSE;
	ait[AM_fd].hdrchanged = FALSE;
	ait[AM_fd].hdr.root.pagenum = NODE_NULLPTR;
	ait[AM_fd].hdr.root.recnum = NODE_INTNULL;

	return AME_OK;
}

/*
	tells if the specified B+ tree node is a leaf node

	*** parameters ***
	char * pbuf - pointer to a B+ tree node

	*** return values ***
	TRUE if specified B+ tree node is a leaf node
	FALSE otherwise
*/
bool_t Btr_isLeaf(char * pbuf){
	RECID res;
	memcpy(&res, pbuf + sizeof(BtrHdr), sizeof(RECID));
	if (res.recnum == NODE_NULLPTR){
		return TRUE;
	}
	return FALSE;
}

/*
	compares values, tell which one is bigger or smaller

	*** parameters ***
	char * a - points to a value to be compared
	char * b - points to another value to be compared
	char attrType - attribute type of a and b (must be the same)
	int attrLength - attribute length of a and b (must be the same)

	*** return values ***
	BTR_LT(-1) if a < b
	BTR_EQ(0) if a == b
	BTR_GT(1) if a > b
*/
int Btr_valComp(char * a, char * b, char attrType, int attrLength){
	int err;
	int result;

	if ((err = AM_validAttr(attrType, attrLength)) != AME_OK){
		printf("Btr_valComp failed: AM_validAttr\n");
		return err;
	}

	if (attrType == INT_TYPE){
		int temp1, temp2;
		memcpy(&temp1, a, sizeof(attrLength));
		memcpy(&temp2, b, sizeof(attrLength));
		if (temp1 < temp2) return BTR_LT;
		if (temp1 == temp2) return BTR_EQ;
		if (temp1 > temp2) return BTR_GT;
	} else if (attrType == REAL_TYPE){
		float temp1, temp2;
		memcpy(&temp1, a, sizeof(attrLength));
		memcpy(&temp2, b, sizeof(attrLength));
		if (temp1 < temp2) return BTR_LT;
		if (temp1 == temp2) return BTR_EQ;
		if (temp1 > temp2) return BTR_GT;
	} else if (attrType == STRING_TYPE){
		result = strncmp(a, b, attrLength);
		if (result < 0) return BTR_LT;
		if (result == 0) return BTR_EQ;
		if (result > 0) return BTR_GT;
	}
   return AME_UNIX;
}

/*
	performs split at specified node if necessary
	recursively called, copying values upward

	*** parameters ***
	int AM_fd - file descriptor of the AM index table
	char * value - points to the value to be inserted in the specified node
	RECID recId - record id of the value to be inserted
	RECID adr -	pointer to the B+ tree node to split when needed
	bool_t duplicate - tells whether a special split for duplicate values is needed

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_recSplit(int AM_fd, char * value, RECID recId, RECID adr, bool_t duplicate){

	int err;
	int entries;
	int entries2;
	int res;
	int i, j;
	char * pbuf;
	RECID parent;
	RECID tempRid_new;
	RECID tempRid_new2;
	RECID tempRid_nbr;
	RECID tempRid;
	RECID rid_empty;
	char * tempValue = (char *)calloc(ait[AM_fd].hdr.attrLength, sizeof(char));
	char * tempValue_par = (char *)calloc(ait[AM_fd].hdr.attrLength, sizeof(char));

	char * tempValue2 = (char *)calloc(sizeof(RECID) + ait[AM_fd].hdr.attrLength, sizeof(char));
	AMhdr_str * amhdr = &(ait[AM_fd].hdr);
	BtrHdr * bhdr;
	BtrHdr * bhdr_new;
	BtrHdr * bhdr_new2;

	char * value_empty = (char *)calloc(amhdr->attrLength, sizeof(char));
	int mid; /* values GE to this are moved to the new node */
	int new; /* idx where the new value is supposed to be at */
	int newNode; /* pagenum of the newly assigned node */
	int newNode2; /* pagenum of the newly assigned node */
	int newRoot; /* pagenum of the new root node, if newly assigned */

	char * pbuf_new; /* points to the created node */
	char * pbuf_new2; /* points to the created node */
	char * pbuf_par; /* points to the parent node */
	char * pbuf_nbr; /* points to the newly created neighboring node (on the right) */

	printf("Btr_recSplit at pagenum %d, recnum %d, duplicate %d, value %s\n", adr.pagenum, adr.recnum, duplicate, value);
	/* retrieving node information */
	if ((err = Btr_getNode(&pbuf, AM_fd, adr)) != AME_OK){
		printf("Btr_recSplit failed: Btr_getNode\n");
		return err;
	}

	/* retrieving number of entries, but we already know it is FULL at leaf nodes */
	bhdr = (BtrHdr *) pbuf;
	entries = bhdr->entries;

	printf("Btr_recSplit: currently contains %d entries\n", entries);

	/* at leaf node */
	if (Btr_isLeaf(pbuf) == TRUE){
		/* check if function was called for duplicate keys */
		if (duplicate == TRUE){

			/*
			   determine the position of the duplicate key
			   split nodes according to the position
			   distribute keys
			   adjust links between leaf nodes
			   copy up the key(s) - twice if the duplicate key is not at one of the ends
			*/

			/* find the position of the duplicate key */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: reading %d th value for finding duplicate value\n", i);
					return err;
				}
				if ((res = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* duplicate value not found */
					printf("Btr_recSplit failed: called for duplicate keys but could not find one?\n");
					return AME_KEYNOTFOUND;
				} else if (res == BTR_EQ) {
					/* found the duplicate value */
					break;
				} else if (res == BTR_GT) {
					/* scan further */
					continue;
				}
			}
			if (i == entries){
				printf("Btr_recSplit failed: called for duplicate keys but could not find one?\n");
				return AME_KEYNOTFOUND;
			}

			/* parent of the current node */
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &parent)) != AME_OK){
				printf("Btr_recSplit failed: retrieving the parent ptr of an leaf node\n");
				return err;
			}

			/* assign a new node */
			if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_LEAF, amhdr->attrLength, amhdr->maxKeys, &newNode, &pbuf_new, parent)) != AME_OK){
				printf("Btr_recSplit failed: assigning a new leaf node\n");
				return err;
			}
			bhdr_new = (BtrHdr *)pbuf_new;
			amhdr->numNodes++;
			ait[AM_fd].hdrchanged = TRUE;

			/* adjust links between leaf nodes */
			/* right neighbor of the current leaf node */

			adr.recnum = NODE_NULLPTR;
			if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &adr)) != AME_OK){
				printf("Btr_recSplit failed: setting PREV ptr of new node\n");
				return err;
			}

			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
				printf("Btr_recSplit failed: retrieving NEXT ptr of a leaf node\n");
				return err;
			}

			if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
				printf("Btr_recSplit failed: setting NEXT ptr of new node\n");
				return err;
			}

			tempRid_new.pagenum = newNode;
			tempRid_new.recnum = NODE_NULLPTR;

			if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_new)) != AME_OK){
				printf("Btr_recSplit failed: setting NEXT ptr of new leaf node\n");
				return err;
			}

			if (tempRid_nbr.pagenum != NODE_NULLPTR){
				if ((err = Btr_getNode(&pbuf_nbr, AM_fd, tempRid_nbr)) != AME_OK){
					printf("Btr_recSplit failed: Btr_getNode for neighboring leaf node\n");
					return err;
				}

				if ((err = Btr_setPtr(&pbuf_nbr, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_new)) != AME_OK){
					printf("Btr_recSplit failed: setting PREV ptr of neighboring leaf node\n");
					return err;
				}
			}

			/* duplicate value at the leftmost position */
			if (i == 0) {
				/* move values - all but the first value */
				for (j = 1; j < entries; j++){
					if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recSplit failed: retrieving %d th ptr of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, j-1, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", j);
						return err;
					}
					if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, j-1, amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", j);
						return err;
					}

					if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, &rid_empty)) != AME_OK){
						printf("Btr_recSplit failed: emptying %d th ptr of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, value_empty)) != AME_OK){
						printf("Btr_recSplit failed: emptying %d th value of current leaf node\n", j);
						return err;
					}
					bhdr->entries--;
					bhdr_new->entries++;
				}

				/* add duplicate value to the current node, set duplicate attribute to TRUE */
				if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, &recId)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of duplicate value at a leaf node\n", 1);
					return err;
				}
				if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, value)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of duplicate value at a leaf node\n", 1);
					return err;
				}
				bhdr->duplicate = TRUE;
				bhdr->entries++;
				amhdr->numRecs++;
				ait[AM_fd].hdrchanged = TRUE;

				/* copy up the second value, with new node's ptr */
				tempRid_new.recnum = NODE_INTNULL;
				if ((err = Btr_getKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving first value of new leaf node\n");
					return err;
				}
				if ((err = Btr_recSplit(AM_fd, tempValue, tempRid_new, parent, FALSE)) != AME_OK){
					printf("Btr_recSplit failed: copying up the new leaf node's first key value to parent from leaf\n");
					return err;
				}

				/* modify the key value of the parent node, whose right side ptr points to current node */
				if ((err = Btr_getNode(&pbuf_par, AM_fd, parent)) != AME_OK){
					printf("Btr_recSplit failed: Btr_getNode for the parent node\n");
					return err;
				}
				bhdr_new2 = (BtrHdr *)pbuf_par;
				entries2 = bhdr_new2->entries;

				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving first value of current node\n");
					return err;
				}

				for (j = 0; j < entries2; j++){
					if ((err = Btr_getPtr(&pbuf_par, NODE_INT, amhdr->attrLength, j, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recSplit failed: reading %d th ptr for finding key value to replace\n", j);
						return err;
					}
					if (tempRid.pagenum == adr.pagenum){
						/* found the key to be replaced */
						if (j == 0){
							/* when there is no key left to the found ptr */
							/* assign a new leaf node to the original ptr's place */
							/* assign a new node */
							if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_LEAF, amhdr->attrLength, amhdr->maxKeys, &newNode2, &pbuf_new2, parent)) != AME_OK){
								printf("Btr_recSplit failed: assigning a new leaf node\n");
								return err;
							}
							amhdr->numNodes++;
							ait[AM_fd].hdrchanged = TRUE;

							/* adjust the pointers */
							tempRid.pagenum = newNode2;
							tempRid.recnum = NODE_INTNULL;
							if ((err = Btr_setPtr(&pbuf_par, NODE_INT, amhdr->attrLength, j, amhdr->maxKeys, &tempRid)) != AME_OK){
								printf("Btr_recSplit failed: assigning ptr at a parent node to newly assigned node\n");
								return err;
							}

							adr.recnum = NODE_NULLPTR;
							if ((err = Btr_setPtr(&pbuf_new2, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &adr)) != AME_OK){
								printf("Btr_recSplit failed: assigning NEXT ptr of newly assigned leaf node \n");
								return err;
							}
							if(tempRid_nbr.pagenum != NODE_NULLPTR){
								if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
									printf("Btr_recSplit failed: PF_UnpinPage of NEXT neighbor\n");
									return err;
								}
							}
							if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
								printf("Btr_recSplit failed: retrieving PREV ptr of a leaf node\n");
								return err;
							}

							if ((err = Btr_setPtr(&pbuf_new2, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
								printf("Btr_recSplit failed: setting PREV ptr of new left leaf node\n");
								return err;
							}

							tempRid.recnum = NODE_NULLPTR;
							if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid)) != AME_OK){
								printf("Btr_recSplit failed: assigning PREV ptr of current leaf node \n");
								return err;
							}

							if (tempRid_nbr.pagenum != NODE_NULLPTR){
								if ((err = Btr_getNode(&pbuf_nbr, AM_fd, tempRid_nbr)) != AME_OK){
									printf("Btr_recSplit failed: Btr_getNode for neighboring leaf node\n");
									return err;
								}

								if ((err = Btr_setPtr(&pbuf_nbr, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid)) != AME_OK){
									printf("Btr_recSplit failed: setting NEXT ptr of neighboring leaf node\n");
									return err;
								}
							}

							/* reinsert the key and ptr to this node */
							adr.recnum = NODE_INTNULL;
							if ((err = Btr_recInsert(AM_fd, value, adr, parent)) != AME_OK){
								printf("Btr_recSplit failed: Btr_recInsert, inserting new empty node \n");
								return err;
							}
						} else {
							/* leave the other entries, just replace the key */
							if ((err = Btr_setKey(&pbuf_par, NODE_INT, amhdr->attrLength, j-1, amhdr->maxKeys, value)) != AME_OK){
								printf("Btr_recSplit failed: replacing the parent node's key for duplicate node\n");
								return err;
							}
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, parent.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recSplit failed: PF_UnpinPage of parent\n");
							return err;
						}
						if (tempRid_nbr.pagenum != NODE_NULLPTR){
							if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
								printf("Btr_recSplit failed: PF_UnpinPage of PREV neighbor\n");
								return err;
							}
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recSplit failed: PF_UnpinPage of newnode 1\n");
							return err;
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recSplit failed: PF_UnpinPage of newnode 2\n");
							return err;
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
							return err;
						}
						return AME_OK;
					} else {
						continue;
					}
				}

				printf("Btr_recSplit failed: rid for the current node not found on the parent node?\n");
				return AME_PF;


			} /* duplicate value at the rightmost position */
			else if (i == entries - 1){
				/* move values - just the last value */
				if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th ptr of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", 0);
					return err;
				}
				if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", 0);
					return err;
				}

				if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &rid_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th ptr of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, value_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th value of current leaf node\n", i);
					return err;
				}
				bhdr->entries--;
				bhdr_new->entries++;

				/* add duplicate value to the new node , set duplicate attribute to TRUE*/
				if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, &recId)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of duplicate value at a new leaf node\n", 1);
					return err;
				}
				if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, value)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of duplicate value at a new leaf node\n", 1);
					return err;
				}
				bhdr_new->duplicate = TRUE;
				bhdr_new->entries++;
				amhdr->numRecs++;
				ait[AM_fd].hdrchanged = TRUE;

				/* copy up the new value, with new node's ptr */
				tempRid_new.recnum = NODE_INTNULL;
				if ((err = Btr_getKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving first value of new leaf node\n");
					return err;
				}

				if (tempRid_nbr.pagenum != NODE_NULLPTR){
					if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
						printf("Btr_recSplit failed: PF_UnpinPage of NEXT neighbor\n");
						return err;
					}
				}
				if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of newnode 1\n");
					return err;
				}

				if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
					return err;
				}
				if ((err = Btr_recSplit(AM_fd, tempValue, tempRid_new, parent, FALSE)) != AME_OK){
					printf("Btr_recSplit failed: copying up the new leaf node's first key value to parent from leaf\n");
					return err;
				}
				return AME_OK;

			} /* otherwise: split twice */
			else {
				/* assign another node */
				if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_LEAF, amhdr->attrLength, amhdr->maxKeys, &newNode2, &pbuf_new2, parent)) != AME_OK){
					printf("Btr_recSplit failed: assigning a new leaf node\n");
					return err;
				}
				bhdr_new2 = (BtrHdr *)pbuf_new2;
				amhdr->numNodes++;
				ait[AM_fd].hdrchanged = TRUE;

				if ((err = Btr_setPtr(&pbuf_new2, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_new)) != AME_OK){
					printf("Btr_recSplit failed: setting PREV ptr of new node\n");
					return err;
				}
				if (tempRid_nbr.pagenum != NODE_NULLPTR){
					if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
						printf("Btr_recSplit failed: PF_UnpinPage of NEXT neighbor\n");
						return err;
					}
				}
				if ((err = Btr_getPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
					printf("Btr_recSplit failed: retrieving NEXT ptr of a leaf node\n");
					return err;
				}

				if ((err = Btr_setPtr(&pbuf_new2, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
					printf("Btr_recSplit failed: setting NEXT ptr of new node\n");
					return err;
				}

				tempRid_new2.pagenum = newNode2;
				tempRid_new2.recnum = NODE_NULLPTR;

				if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_new2)) != AME_OK){
					printf("Btr_recSplit failed: setting NEXT ptr of new leaf node\n");
					return err;
				}

				if (tempRid_nbr.pagenum != NODE_NULLPTR){
					if ((err = Btr_getNode(&pbuf_nbr, AM_fd, tempRid_nbr)) != AME_OK){
						printf("Btr_recSplit failed: Btr_getNode for neighboring leaf node\n");
						return err;
					}

					if ((err = Btr_setPtr(&pbuf_nbr, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_new2)) != AME_OK){
						printf("Btr_recSplit failed: setting PREV ptr of neighboring leaf node\n");
						return err;
					}
				}
				/* move values - values larger than duplicate one */
				for (j = i+1; j < entries; j++){
					if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recSplit failed: retrieving %d th ptr of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_setPtr(&pbuf_new2, NODE_LEAF, amhdr->attrLength, j-(i+1), amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", j);
						return err;
					}
					if ((err = Btr_setKey(&pbuf_new2, NODE_LEAF, amhdr->attrLength, j-(i+1), amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", j);
						return err;
					}

					if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, &rid_empty)) != AME_OK){
						printf("Btr_recSplit failed: emptying %d th ptr of current leaf node\n", j);
						return err;
					}
					if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, value_empty)) != AME_OK){
						printf("Btr_recSplit failed: emptying %d th value of current leaf node\n", j);
						return err;
					}
					bhdr->entries--;
					bhdr_new2->entries++;
				}
				/* move values - duplicate value into newly assigned node */
				if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th ptr of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", 0);
					return err;
				}
				if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", 0);
					return err;
				}

				if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &rid_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th ptr of current leaf node\n", i);
					return err;
				}
				if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, value_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th value of current leaf node\n", i);
					return err;
				}
				bhdr->entries--;
				bhdr_new->entries++;

				/* add duplicate value to the middle node , set duplicate attribute to TRUE*/
				if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, &recId)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", 1);
					return err;
				}
				if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 1, amhdr->maxKeys, value)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", 1);
					return err;
				}
				bhdr_new->entries++;
				amhdr->numRecs++;
				bhdr_new->duplicate = TRUE;

				/* copy up the values */
				if ((err = Btr_getKey(&pbuf_new2, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", 0);
					return err;
				}
				if (tempRid_nbr.pagenum != NODE_NULLPTR){
					if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
						printf("Btr_recSplit failed: PF_UnpinPage of NEXT neighbor\n");
						return err;
					}
				}
				if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of newnode 1\n");
					return err;
				}
				if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new2.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of newnode 2\n");
					return err;
				}

				if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
					return err;
				}
				tempRid_new.recnum = NODE_INTNULL;
				if ((err = Btr_recSplit(AM_fd, value, tempRid_new, parent, FALSE)) != AME_OK){
					printf("Btr_recSplit failed: copying up the new leaf node's first key value to parent from leaf\n");
					return err;
				}
				tempRid_new2.recnum = NODE_INTNULL;
				if ((err = Btr_recSplit(AM_fd, tempValue, tempRid_new2, parent, FALSE)) != AME_OK){
					printf("Btr_recSplit failed: copying up the new leaf node's first key value to parent from leaf\n");
					return err;
				}

				return AME_OK;
			}
			printf("Btr_recSplit failed: Why are you here?\n");
			return AME_PF;
		}

		/* Things to do:
			determine to which node the inserted key belongs - left or new
			determine mid key
			assign new node
				move keys - including the mid key
				remove moved keys from the left node
				adjust links
					links between leaf nodes
					parent to leaf - through copying up the key
			copy up the key
				at internal nodes, 'recId' becomes new nodes' link?
		*/

		/* checking whether this leaf node is full */
		if (entries != amhdr->maxKeys){
			printf("Btr_recSplit failed: entries %d, amhdr->maxKeys %d?\n", entries, amhdr->maxKeys);
			return AME_PF;
		}

		/* determine where 'mid' is */
		mid = amhdr->maxKeys / 2; /* if maxKeys = 4 or 5 then mid = 2 */

		/* parent of the current node */
		if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &parent)) != AME_OK){
			printf("Btr_recSplit failed: retrieving the parent ptr of an leaf node\n");
			return err;
		}

		/* determine where the new key is supposed to be at */
		if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, mid, amhdr->maxKeys, tempValue)) != AME_OK){
			printf("Btr_recSplit failed: retrieving the middle key of an leaf node\n");
			return err;
		}
		new = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength);

		/* assign a new node */
		if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_LEAF, amhdr->attrLength, amhdr->maxKeys, &newNode, &pbuf_new, parent)) != AME_OK){
			printf("Btr_recSplit failed: assigning a new leaf node\n");
			return err;
		}
		bhdr_new = (BtrHdr *)pbuf_new;

		/* adjust links between leaf nodes */
		/* right neighbor of the current leaf node */

		adr.recnum = NODE_NULLPTR;
		if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &adr)) != AME_OK){
			printf("Btr_recSplit failed: setting PREV ptr of new node\n");
			return err;
		}

		if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
			printf("Btr_recSplit failed: retrieving NEXT ptr of a leaf node\n");
			return err;
		}

		if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
			printf("Btr_recSplit failed: setting NEXT ptr of new node\n");
			return err;
		}

		tempRid_new.pagenum = newNode;
		tempRid_new.recnum = NODE_NULLPTR;

		if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_new)) != AME_OK){
			printf("Btr_recSplit failed: setting NEXT ptr of new leaf node\n");
			return err;
		}

		if (tempRid_nbr.pagenum != NODE_NULLPTR){
			if ((err = Btr_getNode(&pbuf_nbr, AM_fd, tempRid_nbr)) != AME_OK){
				printf("Btr_recSplit failed: Btr_getNode for neighboring leaf node\n");
				return err;
			}

			if ((err = Btr_setPtr(&pbuf_nbr, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_new)) != AME_OK){
				printf("Btr_recSplit failed: setting PREV ptr of neighboring leaf node\n");
				return err;
			}
		}

		/* moving keys, removing moved keys from the original node */
		rid_empty.pagenum = NODE_NULLPTR;
		rid_empty.recnum = NODE_NULLPTR;
		for (i = mid; i < entries; i++){
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_recSplit failed: retrieving %d th ptr of current leaf node\n", i);
				return err;
			}
			if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
				printf("Btr_recSplit failed: retrieving %d th value of current leaf node\n", i);
				return err;
			}
			if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, i-mid, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_recSplit failed: writing %d th ptr of current leaf node to new one\n", i);
				return err;
			}
			if ((err = Btr_setKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, i-mid, amhdr->maxKeys, tempValue)) != AME_OK){
				printf("Btr_recSplit failed: writing %d th value of current leaf node to new one\n", i);
				return err;
			}

			if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &rid_empty)) != AME_OK){
				printf("Btr_recSplit failed: emptying %d th ptr of current leaf node\n", i);
				return err;
			}
			if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, value_empty)) != AME_OK){
				printf("Btr_recSplit failed: emptying %d th value of current leaf node\n", i);
				return err;
			}
			bhdr->entries--;
			bhdr_new->entries++;
		}
		if (tempRid_nbr.pagenum != NODE_NULLPTR){
			if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recSplit failed: PF_UnpinPage of NEXT neighbor\n");
				return err;
			}
		}
		if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
			printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
			return err;
		}
		/* inserting the new value into one of the nodes */
		if (new == BTR_LT){
			if ((err = Btr_recInsert(AM_fd, value, recId, adr)) != AME_OK){
				printf("Btr_recSplit failed: inserting new value in current leaf node\n");
				return err;
			}
		} else {
			if ((err = Btr_recInsert(AM_fd, value, recId, tempRid_new)) != AME_OK){
				printf("Btr_recSplit failed: inserting new value in new leaf node\n");
				return err;
			}
		}
		amhdr->numNodes++;
		ait[AM_fd].hdrchanged = TRUE;

		/* copying up the mid value, with new node's ptr */
		tempRid_new.recnum = NODE_INTNULL;
		if ((err = Btr_getKey(&pbuf_new, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
			printf("Btr_recSplit failed: retrieving mid value of current leaf node\n");
			return err;
		}
		if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new.pagenum, TRUE)) != PFE_OK){
			printf("Btr_recSplit failed: PF_UnpinPage of newnode 1\n");
			return err;
		}
		if ((err = Btr_recSplit(AM_fd, tempValue, tempRid_new, parent, FALSE)) != AME_OK){
			printf("Btr_recSplit failed: copying up the mid key value to parent from leaf\n");
			return err;
		}
		printf("Btr_recSplit: value successfully copied up from a leaf node\n");
		return AME_OK;

	} /* at internal node */
	else {

		/* internal node not full */
		if (entries < amhdr->maxKeys){
			/* insert the key and ptr at an appropriate position */
			/* inserting the new value into one of the nodes */
			printf("Btr_recSplit: internal node not full\n");
			if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
				return err;
			}
			adr.recnum = NODE_INTNULL;
			if ((err = Btr_recInsert(AM_fd, value, recId, adr)) != AME_OK){
				printf("Btr_recSplit failed: inserting new value in current internal node(without splitting)\n");
				return err;
			}
			printf("Btr_recSplit: internal node not full. entry added\n");
			return AME_OK;
		} /* internal node full, split again */
		else {

			/* checking whether this internal node is full */
			if (entries != amhdr->maxKeys){
				printf("Btr_recSplit failed(internal node): entries %d, amhdr->maxKeys %d?\n", entries, amhdr->maxKeys);
				return AME_PF;
			}

			/* determine where 'mid' is */
			mid = amhdr->maxKeys / 2; /* if maxKeys = 4 or 5 then mid = 2 */

			/* parent of the current node */
			if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &parent)) != AME_OK){
				printf("Btr_recSplit failed: retrieving the parent ptr of an internal node\n");
				return err;
			}

			/* determine where the new key is supposed to be at */
			if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, mid, amhdr->maxKeys, tempValue)) != AME_OK){
				printf("Btr_recSplit failed: retrieving the middle key of an internal node\n");
				return err;
			}
			new = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength);

			/* assign a new node */
			if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_INT, amhdr->attrLength, amhdr->maxKeys, &newNode, &pbuf_new, parent)) != AME_OK){
				printf("Btr_recSplit failed: assigning a new internal node\n");
				return err;
			}
			bhdr_new = (BtrHdr *)pbuf_new;
			tempRid_new.pagenum = newNode;
			tempRid_new.recnum = NODE_INTNULL;

			/* no direct link between internal nodes of the same height */
			adr.recnum = NODE_INTNULL;

			/* if there is no parent node (current node is root), create new root and update */
			if (parent.pagenum == NODE_NULLPTR){
				printf("root node full, assigning a new root\n");
				if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_INT, amhdr->attrLength, amhdr->maxKeys, &newRoot, &pbuf_nbr, parent)) != AME_OK){
					printf("Btr_recSplit failed(internal): assigning a new root node\n");
					return err;
				}
				/* updating the pointers to the new root node */
				amhdr->root.pagenum = newRoot;
				parent.pagenum = newRoot;
				if ((err = Btr_setPtr(&pbuf, NODE_INT, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &parent)) != AME_OK){
					printf("Btr_recSplit failed: updating the current internal node's parent information after assigning a new root node\n");
					return err;
				}
				if ((err = Btr_setPtr(&pbuf_new, NODE_INT, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &parent)) != AME_OK){
					printf("Btr_recSplit failed: updating the new internal node's parent information after assigning a new root node\n");
					return err;
				}
				/* updating the pointers from the root to the child nodes */
				if ((err = Btr_setPtr(&pbuf_nbr, NODE_ROOT, amhdr->attrLength, 0, amhdr->maxKeys, &adr)) != AME_OK){
					printf("Btr_recSplit failed: updating the current internal node's parent information after assigning a new root node\n");
					return err;
				}
				if ((err = Btr_setPtr(&pbuf_nbr, NODE_ROOT, amhdr->attrLength, 1, amhdr->maxKeys, &tempRid_new)) != AME_OK){
					printf("Btr_recSplit failed: updating the new internal node's parent information after assigning a new root node\n");
					return err;
				}
				amhdr->numNodes++;
				ait[AM_fd].hdrchanged = TRUE;
				if ((err = PF_UnpinPage(ait[AM_fd].pfd, parent.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recSplit failed: PF_UnpinPage of new root node\n");
					return err;
				}
				printf("root node full, assigning a new root: ended\n");
			}

			/* moving keys, removing moved keys from the original node */
			rid_empty.pagenum = NODE_NULLPTR;
			rid_empty.recnum = NODE_NULLPTR;
			for (i = mid; i < entries; i++){
				if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, i+1, amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th ptr of current internal node\n", i+1);
					return err;
				}
				if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: retrieving %d th value of current internal node\n", i);
					return err;
				}
				if ((err = Btr_setPtr(&pbuf_new, NODE_INT, amhdr->attrLength, (i-mid+1), amhdr->maxKeys, &tempRid)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th ptr of current internal node to new one\n", i+1);
					return err;
				}
				if ((err = Btr_setKey(&pbuf_new, NODE_INT, amhdr->attrLength, i-mid, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recSplit failed: writing %d th value of current internal node to new one\n", i);
					return err;
				}

				if ((err = Btr_setPtr(&pbuf, NODE_INT, amhdr->attrLength, i+1, amhdr->maxKeys, &rid_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th ptr of current internal node\n", i+1);
					return err;
				}
				if ((err = Btr_setKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, value_empty)) != AME_OK){
					printf("Btr_recSplit failed: emptying %d th value of current internal node\n", i);
					return err;
				}
				bhdr->entries--;
				bhdr_new->entries++;
			}
			if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, mid, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_recSplit failed: retrieving %d th ptr of current internal node\n", mid);
				return err;
			}
			if ((err = Btr_setPtr(&pbuf_new, NODE_INT, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_recSplit failed: writing %d th ptr of current internal node to new one\n", 0);
				return err;
			}
			/* will not work if mid = 0(amhdr->maxKeys = 1)... */
			if ((err = Btr_setKey(&pbuf, NODE_INT, amhdr->attrLength, mid-1, amhdr->maxKeys, value_empty)) != AME_OK){
				printf("Btr_recSplit failed: emptying %d th value of current internal node\n", mid-1);
				return err;
			}
			bhdr->entries--;
			if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recSplit failed: PF_UnpinPage of adr\n");
				return err;
			}
			/* inserting the new value into one of the nodes */
			if (new == BTR_LT){
				adr.recnum = NODE_INTNULL;
				if ((err = Btr_recInsert(AM_fd, value, recId, adr)) != AME_OK){
					printf("Btr_recSplit failed: inserting new value in current internal node\n");
					return err;
				}
			} else {
				if ((err = Btr_recInsert(AM_fd, value, recId, tempRid_new)) != AME_OK){
					printf("Btr_recSplit failed: inserting new value in new internal node\n");
					return err;
				}
			}
			amhdr->numNodes++;
			ait[AM_fd].hdrchanged = TRUE;

			/* copying up the mid value, with new node's ptr */
			tempRid_new.recnum = NODE_INTNULL;
			if ((err = Btr_getKey(&pbuf_new, NODE_INT, amhdr->attrLength, 0, amhdr->maxKeys, tempValue)) != AME_OK){
				printf("Btr_recSplit failed: retrieving mid value of current internal node\n");
				return err;
			}
			if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_new.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recSplit failed: PF_UnpinPage of newnode 1\n");
				return err;
			}
			if ((err = Btr_recSplit(AM_fd, tempValue, tempRid_new, parent, FALSE)) != AME_OK){
				printf("Btr_recSplit failed: copying up the mid key value to parent from an internal node\n");
				return err;
			}
			return AME_OK;

		}
		printf("Btr_recSplit failed: not supposed to reach here?\n");
		return AME_PF;
	}
	printf("Btr_recSplit failed: not supposed to reach here?\n");
	return AME_PF;
}

/*
	inserts the specified value in the B+ tree
	recursively called

	*** parameters ***
	int AM_fd - file descriptor of the AM index table
	char * value - points to the value to be inserted in B+ tree
	RECID recId - record id of the value to be inserted
	RECID adr -	pointer to the B+ tree node to start searching

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_recInsert(int AM_fd, char * value, RECID recId, RECID adr){
	int err;
	int entries;
	int res;
	int i, j;
	char * pbuf;
	RECID tempRid;
	RECID tempRid_new;
	RECID tempRid_nbr;
	RECID tempRid_par;
	char * tempValue = (char *)calloc(ait[AM_fd].hdr.attrLength, sizeof(char));
	char * tempValue_par = (char *)calloc(ait[AM_fd].hdr.attrLength, sizeof(char));
	char * tempValue2 = (char *)calloc(sizeof(RECID) + ait[AM_fd].hdr.attrLength, sizeof(char));
	AMhdr_str * amhdr = &(ait[AM_fd].hdr);
	BtrHdr * bhdr;
	BtrHdr * bhdr_new;

	int newNode;
	char * pbuf_nbr;
	char * pbuf_new;
	char * pbuf_par;

	/* retrieving node information */
	if ((err = Btr_getNode(&pbuf, AM_fd, adr)) != AME_OK){
		printf("Btr_recInsert failed: Btr_getNode\n");
		return err;
	}

	printf("Btr_recInsert at page %d, with value %s\n", adr.pagenum, value);

	/* retrieving number of entries */
	bhdr = (BtrHdr *) pbuf;
	entries = bhdr->entries;

	/* at leaf node */
	if (Btr_isLeaf(pbuf) == TRUE){
		if (entries > amhdr->maxKeys){
			printf("Btr_recInsert failed: entries more than maxKeys??\n");
			return AME_PF;
		} /* when the node is full with entries */
		else if (entries == amhdr->maxKeys){
			/* if this leaf node contains duplicate values, raise error */
			if (bhdr->duplicate == TRUE){
				printf("Btr_recInsert failed: this node is full of duplicate values!!\n");
				return AME_TOOMANYRECSPERKEY;
			}
			if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
				return err;
			}

			/* split */
			if ((err = Btr_recSplit(AM_fd, value, recId, adr, FALSE)) != AME_OK){
				printf("Btr_recInsert failed: Btr_recSplit at node %d\n", adr.pagenum);
				return err;
			}
			printf("Btr_recInsert: successfully split at a leaf node\n");
			return AME_OK;
		} else {
			/* looking for a place to fit */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recInsert failed(looking for fitting place): retrieving %d th value of current leaf node\n", i);
					return err;
				}

				if ((res = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* found place to insert, get out of this for loop */
					break;
				} else if (res == BTR_EQ) {
					/* if recId is the same, raise error */
					if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recInsert failed(looking for fitting place): checking duplciate record ID \n");
						return err;
					}
					if ((tempRid.pagenum == recId.pagenum) && (tempRid.recnum == recId.recnum)){
						printf("Btr_recInsert failed(looking for fitting place): duplicate value with duplicate record ID \n");
						return AME_DUPLICATERECID;
					}

					/* if duplicate attribute is TRUE, continue */
					if (bhdr->duplicate == TRUE){
						continue;
					} else {
						/* otherwise, split for duplicate values */
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
							return err;
						}
						if ((err = Btr_recSplit(AM_fd, value, recId, adr, TRUE)) != AME_OK){
							printf("Btr_recInsert failed: Btr_recSplit for duplicate values at node %d\n", adr.pagenum);
							return err;
						}
						return AME_OK;
					}

				} else if (res == BTR_GT) {
					/* current node is for duplicate values but 'value' is bigger */
					if (bhdr->duplicate == TRUE){
						/* find current leaf node's NEXT node */
						if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
							printf("Btr_recInsert failed: obtaining NEXT ptr of current leaf node\n");
							return err;
						}

						/* assign a new node with this value inserted */
						if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, NODE_PARENT, amhdr->maxKeys, &tempRid_par)) != AME_OK){
							printf("Btr_recInsert failed: obtaining PARENT ptr of current leaf node\n");
							return err;
						}
						/* retrieving node information */
						if ((err = Btr_getNode(&pbuf_par, AM_fd, tempRid_par)) != AME_OK){
							printf("Btr_recInsert failed: Btr_getNode for parent node\n");
							return err;
						}

						/* assign a new node, adjust links */
						if ((err = Btr_assignNode(ait[AM_fd].pfd, NODE_LEAF, amhdr->attrLength, amhdr->maxKeys, &newNode, &pbuf_new, tempRid_par)) != AME_OK){
							printf("Btr_recInsert failed: assigning a new leaf node\n");
							return err;
						}
						bhdr_new = (BtrHdr *)pbuf_new;
						amhdr->numNodes++;
						ait[AM_fd].hdrchanged = TRUE;

						/* adjust links between leaf nodes */
						adr.recnum = NODE_NULLPTR;
						if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &adr)) != AME_OK){
							printf("Btr_recInsert failed: setting PREV ptr of new node\n");
							return err;
						}

						if ((err = Btr_setPtr(&pbuf_new, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_nbr)) != AME_OK){
							printf("Btr_recSplit failed: setting NEXT ptr of new node\n");
							return err;
						}

						tempRid_new.pagenum = newNode;
						tempRid_new.recnum = NODE_NULLPTR;

						if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid_new)) != AME_OK){
							printf("Btr_recSplit failed: setting NEXT ptr of new leaf node\n");
							return err;
						}

						if (tempRid_nbr.pagenum != NODE_NULLPTR){
							if ((err = Btr_getNode(&pbuf_nbr, AM_fd, tempRid_nbr)) != AME_OK){
								printf("Btr_recSplit failed: Btr_getNode for neighboring leaf node\n");
								return err;
							}

							if ((err = Btr_setPtr(&pbuf_nbr, NODE_LEAF, amhdr->attrLength, LEAFIDX_PREV, amhdr->maxKeys, &tempRid_new)) != AME_OK){
								printf("Btr_recSplit failed: setting PREV ptr of neighboring leaf node\n");
								return err;
							}
						}

						/* insert value in the new node */
						if ((err = Btr_recInsert(AM_fd, value, recId, tempRid_new)) != AME_OK){
							printf("Btr_recInsert failed: inserting new value in new leaf node\n");
							return err;
						}
						bhdr_new->entries++;
						amhdr->numRecs++;
						ait[AM_fd].hdrchanged = TRUE;

						/* copy up the value with key toward the new node */
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_par.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
							return err;
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid_nbr.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
							return err;
						}
						if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
							printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
							return err;
						}
						tempRid_new.recnum = NODE_INTNULL;
						if ((err = Btr_recSplit(AM_fd, value, tempRid_new, tempRid_par, FALSE)) != AME_OK){
							printf("Btr_recInsert failed: copying up the value with ptr to the new leaf node\n");
							return err;
						}
						return AME_OK;
					}

					/* scan further */
					continue;
				}
			}
			/* moving entries that are bigger than 'value' */
			if (i < entries){
				for (j = 0; j < entries - i; j++){
					/*
					   copy from (entries - (j+1)) th entry to (entries - j) th entry
					   j = 0 to (entries - i - 1) : (entries - 1)th to (entries - 0), ..., i th to i+1 th
					*/
					if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, entries - (j+1), amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recInsert failed: retrieving %d th ptr of current leaf node\n", entries - (j+1));
						return err;
					}
					if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, entries - (j+1), amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recInsert failed: retrieving %d th value of current leaf node\n", entries - (j+1));
						return err;
					}
					if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, entries - j, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recInsert failed: moving %d th ptr of current leaf node right\n", entries - (j+1));
						return err;
					}
					if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, entries - j, amhdr->maxKeys, tempValue)) != AME_OK){
						printf("Btr_recInsert failed: moving %d th value of current leaf node right\n", entries - (j+1));
						return err;
					}
				}
			}

			/* inserting value, with determined 'i' value */
			if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &recId)) != AME_OK){
				printf("Btr_recInsert failed: inserting new ptr at %d th entry\n", i);
				return err;
			}
			if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, value)) != AME_OK){
				printf("Btr_recInsert failed: inserting new key at %d th entry\n", i);
				return err;
			}
			printf("Btr_recInsert: new value %s inserted at %d of pagenum %d\n", value, i, adr.pagenum);

			/* updating 'entries' of the leaf node and header's 'numRecs' */
			entries++;
			amhdr->numRecs++;
			ait[AM_fd].hdrchanged = TRUE;
			bhdr->entries = entries;

			if ((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
				return err;
			}
			return AME_OK;
		}
	} /* at internal node */
	else {
		printf("Btr_recInsert: at internal\n");
		if (entries > amhdr->maxKeys){
			printf("Btr_recInsert failed: entries more than maxKeys(internal node)??\n");
			return AME_PF;
		} /* node is empty */
		else if (entries == 0) {
			/* fill the entry with this key, go to the right child */

			if ((err = Btr_setKey(&pbuf, NODE_INT, amhdr->attrLength, 0, amhdr->maxKeys, value)) != AME_OK){
				printf("Btr_recInsert failed: filling in an empty node with a key\n");
				return err;
			}

			entries++;
			bhdr->entries = entries;

			if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
				return err;
			}
			/* receiving pointer information */
			printf("Btr_recInsert: adr before: pagenum %d, recnum %d\n", adr.pagenum, adr.recnum);
			if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, 1, amhdr->maxKeys, &adr)) != AME_OK){
				printf("Btr_recInsert failed: Btr_getPtr of right child at an empty internal node\n");
				return err;
			}
			printf("Btr_recInsert: adr updated: pagenum %d, recnum %d\n", adr.pagenum, adr.recnum);

			return Btr_recInsert(AM_fd, value, recId, adr);
		} /* node is not empty, find the way */
		else {
			/* looking for a place to fit */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recInsert failed(looking for fitting place): retrieving %d th value of current internal node\n", i);
					return err;
				}

				if ((res = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* found way, get out of this for loop */
					break;
				} else if (res == BTR_EQ) {
					/* duplicate keys? - no need to worry, problems are dealt with at the leaf node level */
					continue;
				} else if (res == BTR_GT) {
					/* scan further */
					continue;
				}
			}

			/* checking whether the inserted value is from actual record or a key for internal nodes */
			if (recId.recnum == NODE_NULLPTR){
				printf("Btr_recInsert failed: recId.recnum < 0 and recnum == NODE_NULLPTR?\n");
				return AME_PF;
			}
			if (recId.recnum == NODE_INTNULL){
				printf("Btr_recInsert: adding an entry at INTERNAL NODE\n");
				/* moving entries that are bigger than 'value', with ptrs on the RIGHT, not left */
				if (i < entries){
					for (j = 0; j < entries - i; j++){
						if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, (entries - j), amhdr->maxKeys, &tempRid)) != AME_OK){
							printf("Btr_recInsert failed: retrieving %d th ptr of current internal node\n", entries - j);
							return err;
						}
						if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, (entries - (j+1)), amhdr->maxKeys, tempValue)) != AME_OK){
							printf("Btr_recInsert failed: retrieving %d th value of current internal node\n", entries - (j+1));
							return err;
						}
						if ((err = Btr_setPtr(&pbuf, NODE_INT, amhdr->attrLength, (entries - j + 1), amhdr->maxKeys, &tempRid)) != AME_OK){
							printf("Btr_recInsert failed: moving %d th ptr of current internal node right\n", entries - j);
							return err;
						}
						if ((err = Btr_setKey(&pbuf, NODE_INT, amhdr->attrLength, (entries - j), amhdr->maxKeys, tempValue)) != AME_OK){
							printf("Btr_recInsert failed: moving %d th value of current internal node right\n", entries - (j+1));
							return err;
						}
					}
				}
				printf("Btr_recInsert: adding an entry at INTERNAL NODE, about to add an entry\n");
				if ((err = Btr_setPtr(&pbuf, NODE_INT, amhdr->attrLength, (i+1), amhdr->maxKeys, &recId)) != AME_OK){
					printf("Btr_recInsert failed: inserting new ptr(on the right) at %d th entry\n", j);
					return err;
				}
				if ((err = Btr_setKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, value)) != AME_OK){
					printf("Btr_recInsert failed: inserting new key at %d th entry\n", j);
					return err;
				}
				entries++;
				bhdr->entries = entries;
				if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
					return err;
				}
				printf("Btr_recInsert: adding an entry at INTERNAL NODE ended\n");
				return AME_OK;
			} /* the inserted value is from actual record. proceed to child nodes */
			else {
				if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
					printf("Btr_recInsert failed: PF_UnpinPage of adr\n");
					return err;
				}
				/* receiving pointer information */
				if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, &adr)) != AME_OK){
					printf("Btr_recInsert failed: receiving pointer for a child at a nonempty internal node\n");
					return err;
				}
				printf("adr updated: pagenum %d, recnum %d\n", adr.pagenum, adr.recnum);

				return Btr_recInsert(AM_fd, value, recId, adr);
			}
		}
	}
	return AME_PF;
}


/*
	inserts the specified value in the B+ tree

	*** parameters ***
	int AM_fd - file descriptor of the AM index table
	char * value - points to the value to be inserted in B+ tree
	RECID recId - record id of the value to be inserted

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_InsertEntry(int AM_fd, char *value, RECID recId) {
	int err;
	
	if (ait[AM_fd].valid != TRUE) {
		printf("AM_InsertEntry failed: given AM_fd invalid\n");
		return AME_INVALIDPARA;
	}

	/* search begins at root node */
	if ((err = Btr_recInsert(AM_fd, value, recId, ait[AM_fd].hdr.root)) != AME_OK) {
		printf("AM_InsertEntry failed: Btr_recInsert\n");
		return AME_PF;
	}

	return AME_OK;
}

/*
	deletes the specified value in the B+ tree

	*** parameters ***
	int AM_fd - file descriptor of the AM index table
	char * value - points to the value to be deleted in B+ tree
	RECID recId - record id of the value to be deleted
	RECID adr - pointer to the B+ tree node for this function to start searching

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int Btr_recDelete(int AM_fd, char * value, RECID recId, RECID adr){
	int err;
	int entries;
	int res;
	int i, j;
	char * pbuf;
	RECID tempRid;
	RECID tempRid2;
	RECID rid_empty;
	char * tempValue = (char *)calloc(ait[AM_fd].hdr.attrLength, sizeof(char));
	char * tempValue2 = (char *)calloc(sizeof(RECID) + ait[AM_fd].hdr.attrLength, sizeof(char));
	AMhdr_str * amhdr = &(ait[AM_fd].hdr);
	BtrHdr * bhdr;

	char * value_empty = (char *)calloc(amhdr->attrLength, sizeof(char));

	rid_empty.pagenum = NODE_NULLPTR;
	rid_empty.recnum = NODE_NULLPTR;

	/* retrieving node information */
	if ((err = Btr_getNode(&pbuf, AM_fd, adr)) != AME_OK){
		printf("Btr_recDelete failed: Btr_getNode\n");
		return err;
	}

	/* retrieving number of entries */
	bhdr = (BtrHdr *) pbuf;
	entries = bhdr->entries;


	/* at leaf node */
	if (Btr_isLeaf(pbuf) == TRUE){
		if (entries > amhdr->maxKeys){
			printf("Btr_recDelete failed: entries more than maxKeys??\n");
			return AME_PF;
		} else {
			/* looking for the given entry */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recDelete failed(looking for value): retrieving %d th value of current leaf node\n", i);
					return err;
				}

				if ((res = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* failed to find the value?? */
					printf("Btr_recDelete failed: given value not found at a the leaf node?\n");
					return AME_KEYNOTFOUND;
				} else if (res == BTR_EQ) {
					/* check if recId is also the same */
					/* if recId is different (when not DUPLICATE), raise error */
					if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
						printf("Btr_recDelete failed(looking for value): retrieving record ID \n");
						return err;
					}
					if ((tempRid.pagenum == recId.pagenum) && (tempRid.recnum == recId.recnum)){
						/* If this node is the same as scan.currentNode and recId <= scan.currentNode, scan.currentNode.recnum--. */
						for (j = 0; j < MAXISCANS; j++) {
							if (ast[j].valid == FALSE) continue;

							printf("compare recnum %d, %d / %d, %d / %d, %d\n", tempRid.pagenum, tempRid.recnum, ast[j].current.pagenum, ast[j].current.recnum, ast[j].currentNode.pagenum, ast[j].currentNode.recnum);

							if (ast[j].fd == AM_fd && tempRid.pagenum == ast[j].current.pagenum && tempRid.recnum <= ast[j].current.recnum) {
								printf("minus recnum by 1\n");
								ast[j].current.recnum--;
								ast[j].currentNode.recnum--;

								if (ast[j].current.recnum < 0) ast[j].current.recnum = 0;
								if (ast[j].currentNode.recnum < 0) ast[j].currentNode.recnum = 0;
							}
						}

						/* delete this entry, pull latter entries one cell left */
						for (j = i; j < entries; j++){
							if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, (j+1), amhdr->maxKeys, &tempRid)) != AME_OK){
								printf("Btr_recDelete failed: retrieving %d th ptr of current leaf node\n", (j+1));
								return err;
							}
							if (j < entries - 1){
								if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, (j+1), amhdr->maxKeys, tempValue)) != AME_OK){
									printf("Btr_recDelete failed: retrieving %d th value of current leaf node\n", (j+1));
									return err;
								}
							}

							if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, &tempRid)) != AME_OK){
								printf("Btr_recDelete failed: moving %d th ptr of current leaf node left\n", j);
								return err;
							}
							if (j == (entries - 1)){
								if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, value_empty)) != AME_OK){
									printf("Btr_recDelete failed: deleting %d th value of current leaf node left\n", j);
									return err;
								}
								if ((err = Btr_setPtr(&pbuf, NODE_LEAF, amhdr->attrLength, (j+1), amhdr->maxKeys, &rid_empty)) != AME_OK){
									printf("Btr_recDelete failed: deleting %d th ptr of current leaf node left\n", j);
									return err;
								}
							} else {
								if ((err = Btr_setKey(&pbuf, NODE_LEAF, amhdr->attrLength, j, amhdr->maxKeys, tempValue)) != AME_OK){
									printf("Btr_recDelete failed: moving %d th value of current leaf node left\n", j);
									return err;
								}
							}
						}
						bhdr->entries--;
						amhdr->numRecs--;
						ait[AM_fd].hdrchanged = TRUE;
						if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
							printf("cBtr_recDelete failed: PF_UnpinPage of adr\n");
							return err;
						}
						return AME_OK;
					} else if (bhdr->duplicate == TRUE){
						continue;
					} else {
						printf("Btr_recDelete failed(looking for value): value found, recId not matching\n");
						if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
							printf("dBtr_recDelete failed: PF_UnpinPage of adr\n");
							return err;
						}
						return AME_RECNOTFOUND;
					}

				} else if (res == BTR_GT) {
					/* scan further */
					continue;
				}
			}

			/* value not found, return error */
			if ((res == BTR_EQ) && (bhdr->duplicate == TRUE)){
				printf("Btr_recDelete failed: value found(duplicate), recId not matching\n");
				return AME_RECNOTFOUND;
			} else {
				printf("Btr_recDelete failed: given value not found\n");
				return AME_KEYNOTFOUND;
			}
			if((err = PF_UnpinPage(ait[AM_fd].pfd, adr.pagenum, TRUE)) != PFE_OK){
				printf("aBtr_recDelete failed: PF_UnpinPage of adr\n");
				return err;
			}
			return AME_OK;
		}
	} /* at internal node */
	else {
		if (entries > amhdr->maxKeys){
			printf("Btr_recDelete failed: entries more than maxKeys(internal node)??\n");
			return AME_PF;
		} else {
			/* looking for a place to fit */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_recDelete failed(looking for fitting place): retrieving %d th value of current internal node\n", i);
					return err;
				}

				if ((res = Btr_valComp(value, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* found way, get out of this for loop */
					break;
				} else if (res == BTR_EQ) {
					/* scan further */
					continue;
				} else if (res == BTR_GT) {
					/* scan further */
					continue;
				}
			}
			/* proceed to child nodes */
			/* receiving pointer information */
			tempRid2.pagenum = adr.pagenum;

			if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, &adr)) != AME_OK){
				printf("Btr_recDelete failed: receiving pointer for a child at a nonempty internal node\n");
				return err;
			}
			printf("adr updated: pagenum %d, recnum %d\n", adr.pagenum, adr.recnum);
			if((err = PF_UnpinPage(ait[AM_fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
				printf("bBtr_recDelete failed: PF_UnpinPage of adr\n");
				return err;
			}

			return Btr_recDelete(AM_fd, value, recId, adr);
		}
	}
	return AME_PF;
}

/*
	deletes the specified entry in the B+ tree index

	*** parameters ***
	int AM_fd - file descriptor of the AM index table
	char * value - points to the value to be deleted in B+ tree
	RECID recId - record id of the value to be deleted

	*** return values ***
	AME_OK if successful
	error codes (< 0) if a problem occurred
*/
int AM_DeleteEntry(int AM_fd, char *value, RECID recId){
	int err;

	if (ait[AM_fd].valid != TRUE){
		printf("AM_DeleteEntry failed: given AM_fd invalid\n");
		return AME_INVALIDPARA;
	}

	/* search begins at root node */
	if ((err = Btr_recDelete(AM_fd, value, recId, ait[AM_fd].hdr.root)) != AME_OK){
		printf("AM_DeleteEntry failed: Btr_recDelete\n");
		return err;
	}

	return AME_OK;
}

/*
	retrieves the first valid value in the B+ tree

	*** parameters ***
	int fd - file descriptor of the AM index table
	char ** record - address of the space where value will be copied once found
	RECID * nodeAdr - points to the RECID of the value's position

	*** return values ***
	RECID of the given value if successful
	RECID containing NODE_NULLPTR(-1) if a problem occurred
*/
RECID Btr_getFirstValue(int fd, char ** record, RECID * nodeAdr){
	int err;

	RECID tempRid;
	RECID tempRid2;
	RECID res;
	char * tempValue = (char *)calloc(ait[fd].hdr.attrLength, sizeof(char));

	char * pbuf;
	AMhdr_str * amhdr = &(ait[fd].hdr);
	BtrHdr * bhdr;

	res.pagenum = NODE_NULLPTR;
	res.recnum = NODE_NULLPTR;
	/* retrieving root node information */
	if ((err = Btr_getNode(&pbuf, fd, amhdr->root)) != AME_OK){
		printf("Btr_getFirstValue failed: Btr_getNode\n");
		return res;
	}
	bhdr = (BtrHdr *) pbuf;

	if((err = PF_UnpinPage(ait[fd].pfd, amhdr->root.pagenum, TRUE)) != PFE_OK){
		printf("Btr_getFirstValue failed: PF_UnpinPage of root\n");
		return res;
	}
	tempRid.pagenum = NODE_NULLPTR;
	while(Btr_isLeaf(pbuf) != TRUE){
		if (tempRid.pagenum != NODE_NULLPTR){
			if((err = PF_UnpinPage(ait[fd].pfd, tempRid.pagenum, TRUE)) != PFE_OK){
				printf("Btr_getFirstValue failed: PF_UnpinPage of root\n");
				return res;
			}
		}
		/* proceed to leftmost child nodes */
		if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
			printf("Btr_getFirstValue failed: receiving pointer to a leftmost child node\n");
			return res;
		}

		if ((err = Btr_getNode(&pbuf, fd, tempRid)) != AME_OK){
			printf("Btr_getFirstValue failed: Btr_getNode to a leftmost child node\n");
			return res;
		}
	}
	while(TRUE){
		/* at leaf node, check if this node has any valid entries */
		bhdr = (BtrHdr *)pbuf;
		if (bhdr->entries > 0){
			nodeAdr->pagenum = tempRid.pagenum;
			nodeAdr->recnum = 0;
			/* retrieve the first value and pointer, since it is the smallest and first record */
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_getFirstValue failed: receiving pointer of leaf node\n");
				return res;
			}
			if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, *record)) != AME_OK){
				printf("Btr_getFirstValue failed: receiving key of leaf node\n");
				return res;
			}
			if((err = PF_UnpinPage(ait[fd].pfd, nodeAdr->pagenum, TRUE)) != PFE_OK){
				printf("Btr_getFirstValue failed: PF_UnpinPage of root\n");
				return res;
			}

			return tempRid;
		} else {
			tempRid2.pagenum = tempRid.pagenum;
			tempRid2.recnum = tempRid.recnum;
			if((err = PF_UnpinPage(ait[fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
				printf("Btr_getFirstValue failed: PF_UnpinPage of leaf\n");
				return res;
			}
			/* retrieve pointer to the NEXT leaf node, check validity */
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_getFirstValue failed: receiving pointer to NEXT leaf node\n");
				return res;
			}
			if (tempRid.pagenum == NODE_NULLPTR){
				/* current leaf node is the last one, failed finding the first value */
				printf("Btr_getFirstValue failed: entire B+ tree is empty\n");
				return res;
			} else {
				/* move to the NEXT leaf node */
				if ((err = Btr_getNode(&pbuf, fd, tempRid)) != AME_OK){
					printf("Btr_getFirstValue failed: Btr_getNode to NEXT leaf node\n");
					return res;
				}
			}
		}
	}
}

/*
	finds the record id of the specified value

	*** parameters ***
	int fd - file descriptor for the AM index table
	RECID recId - RECID of the given value to find
	char * record_in - points to the record to be found in B+ tree

	*** return values ***
	RECID of the specified value if successful
	RECID containing NODE_NULLPTR(-1) if a problem occurred
*/
RECID Btr_getThisValue(int fd, RECID recId, char * record_in){
	int err, result, i, j, entries;

	RECID tempRid;
	RECID tempRid2;
	RECID tempRid_nbr;
	RECID tempRec;
	RECID res;
	char * tempValue = (char *)calloc(ait[fd].hdr.attrLength, sizeof(char));

	char * pbuf;
	AMhdr_str * amhdr = &(ait[fd].hdr);
	BtrHdr * bhdr;

	res.pagenum = NODE_NULLPTR;
	res.recnum = NODE_NULLPTR;

	/* retrieving root node information */
	if ((err = Btr_getNode(&pbuf, fd, amhdr->root)) != AME_OK){
		printf("Btr_getThisValue failed: Btr_getNode\n");
		return res;
	}
	bhdr = (BtrHdr *) pbuf;

	while(Btr_isLeaf(pbuf) != TRUE){

		bhdr = (BtrHdr *) pbuf;
		entries = bhdr->entries;
		/* check where to go */
		if (entries > amhdr->maxKeys){
			printf("Btr_getThisValue failed: entries more than maxKeys(internal node)??\n");
			return res;
		} else {
			/* looking for a place to fit */
			for (i = 0; i < entries; i++){
				if ((err = Btr_getKey(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
					printf("Btr_getThisValue failed(looking for fitting place): retrieving %d th value of current internal node\n", i);
					return res;
				}

				if ((result = Btr_valComp(record_in, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
					/* found way, get out of this for loop */
					break;
				} else if (result == BTR_EQ) {
					/* scan further */
					continue;
				} else if (result == BTR_GT) {
					/* scan further */
					continue;
				}
			}
			/* proceed to child nodes */
			/* receiving pointer information */
			tempRid2.pagenum = tempRid.pagenum;
			tempRid2.recnum = tempRid.recnum;

			if((err = PF_UnpinPage(ait[fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
				printf("Btr_getThisValue failed: PF_UnpinPage of leaf\n");
				return res;
			}
			if ((err = Btr_getPtr(&pbuf, NODE_INT, amhdr->attrLength, i, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_getThisValue failed: receiving pointer for a child node\n");
				return res;
			}
			if ((err = Btr_getNode(&pbuf, fd, tempRid)) != AME_OK){
				printf("Btr_getThisValue failed: Btr_getNode to a child node\n");
				return res;
			}
		}
	}
	/* leaf node found, look for the value */
	for (i = 0; i < entries; i++){
		if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, tempValue)) != AME_OK){
			printf("Btr_getThisValue failed(looking for fitting place): retrieving %d th value of current internal node\n", i);
			return res;
		}

		if ((result = Btr_valComp(record_in, tempValue, amhdr->attrType, amhdr->attrLength)) == BTR_LT){
			/* value not found, exit */
			break;
		} else if (result == BTR_EQ) {
			/* check if recId also coincides */
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, i, amhdr->maxKeys, &tempRec)) != AME_OK){
				printf("Btr_getThisValue failed: receiving pointer for a record\n");
				return res;
			}
			if ((tempRec.pagenum == recId.pagenum) && (tempRec.recnum == recId.recnum)){
				tempRid2.pagenum = tempRid.pagenum;
				tempRid2.recnum = tempRid.recnum;

				if((err = PF_UnpinPage(ait[fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
					printf("Btr_getThisValue failed: PF_UnpinPage of leaf\n");
					return res;
				}
				res.pagenum = tempRid.pagenum;
				res.recnum = i;
				return res;
			} else {
				continue;
			}

		} else if (result == BTR_GT) {
			/* scan further */
			continue;
		}
	}

	printf("Btr_getThisValue failed: value not found in the leaf node\n");
	return res;
}

/*
	finds the next value in the B+ tree

	*** parameters ***
	int fd - file descriptor for the AM index table
	char ** record_out - address of the space where next value will be written at if found
	RECID * nodeAdr - position of the current record in the B+ tree, updated when a next value is found

	*** return value ***
	RECID of the next value if successful
	RECID containing NODE_NULLPTR(-1) if a problem occurred
*/
RECID Btr_getNextValue(int fd, char ** record_out, RECID * nodeAdr){
	int err, i, j, entries;

	RECID tempRid;
	RECID tempRid2;
	RECID tempRec;
	RECID res;
	char * tempValue = (char *)calloc(ait[fd].hdr.attrLength, sizeof(char));

	char * pbuf;
	AMhdr_str * amhdr = &(ait[fd].hdr);
	BtrHdr * bhdr;

	res.pagenum = NODE_NULLPTR;
	res.recnum = NODE_NULLPTR;

	tempRid.pagenum = nodeAdr->pagenum;
	tempRid.recnum = nodeAdr->recnum;
	/* retrieving leaf node information */
	if ((err = Btr_getNode(&pbuf, fd, tempRid)) != AME_OK){
		printf("Btr_getNextValue failed: Btr_getNode\n");
		return res;
	}
	bhdr = (BtrHdr *) pbuf;

	/* another value in the same leaf node */
	if (tempRid.recnum + 1 < bhdr->entries){
		if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, tempRid.recnum + 1, amhdr->maxKeys, *record_out)) != AME_OK){
			printf("Btr_getNextValue failed: retrieving %d th value of current leaf node\n", tempRid.recnum + 1);
			return res;
		}
		if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, tempRid.recnum + 1, amhdr->maxKeys, &tempRec)) != AME_OK){
			printf("Btr_getNextValue failed: receiving pointer for a record\n");
			return res;
		}
		nodeAdr->recnum++;
		tempRid2.pagenum = tempRid.pagenum;
		tempRid2.recnum = tempRid.recnum;

		if((err = PF_UnpinPage(ait[fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
			printf("aBtr_getNextValue failed: PF_UnpinPage of leaf\n");
			return res;
		}
		return tempRec;
	}


	while(TRUE){
		tempRid2.pagenum = tempRid.pagenum;
		tempRid2.recnum = tempRid.recnum;

		if((err = PF_UnpinPage(ait[fd].pfd, tempRid2.pagenum, TRUE)) != PFE_OK){
			printf("bBtr_getNextValue failed: PF_UnpinPage of leaf\n");
			return res;
		}
		/* retrieve pointer to the NEXT leaf node, check validity */
		if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, LEAFIDX_NEXT, amhdr->maxKeys, &tempRid)) != AME_OK){
			printf("Btr_getNextValue failed: receiving pointer to NEXT leaf node\n");
			return res;
		}
		if (tempRid.pagenum == NODE_NULLPTR){
			/* current leaf node is the last one, failed finding the first value */
			printf("Btr_getNextValue failed: remaining B+ tree nodes are empty\n");
			return res;
		} else {

			/* move to the NEXT leaf node */
			if ((err = Btr_getNode(&pbuf, fd, tempRid)) != AME_OK){
				printf("Btr_getNextValue failed: Btr_getNode to NEXT leaf node\n");
				return res;
			}
		}
		/* at leaf node, check if this node has any valid entries */
		bhdr = (BtrHdr *)pbuf;
		if (bhdr->entries > 0){
			nodeAdr->pagenum = tempRid.pagenum;
			nodeAdr->recnum = 0;
			/* retrieve the first value and pointer, since it is the smallest and first record */
			if ((err = Btr_getPtr(&pbuf, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, &tempRid)) != AME_OK){
				printf("Btr_getNextValue failed: receiving pointer of leaf node\n");
				return res;
			}
			if ((err = Btr_getKey(&pbuf, NODE_LEAF, amhdr->attrLength, 0, amhdr->maxKeys, *record_out)) != AME_OK){
				printf("Btr_getNextValue failed: receiving key of leaf node\n");
				return res;
			}
			if((err = PF_UnpinPage(ait[fd].pfd, nodeAdr->pagenum, TRUE)) != PFE_OK){
				printf("cBtr_getNextValue failed: PF_UnpinPage of leaf\n");
				return res;
			}
			return tempRid;
		}
	}
	return res;
}


/*
	opens an AM scan table entry for a scan

	*** parameters ***
	int AM_fd - file descriptor for the AM index table
	int op - comparison operator
	char * value - value for comparison

	*** return values ***
	index of the AM scan table entry if successful
	AME_SCANTABLEFULL if AM scan table is already full
*/
int AM_OpenIndexScan(int AM_fd, int op, char *value){
	int asd;

	for (asd = 0; asd < MAXISCANS; asd++) {
		if (ast[asd].valid == FALSE) {
			ast[asd].valid = TRUE;
			ast[asd].fd = AM_fd;
			ast[asd].attrType = ait[AM_fd].hdr.attrType;
			ast[asd].attrLength = ait[AM_fd].hdr.attrLength;
			ast[asd].op = op;
			ast[asd].value = value;
			ast[asd].current.pagenum = AME_SCANOPEN;
			ast[asd].current.recnum = AME_SCANOPEN;
			return asd;
		}
	}
	return AME_SCANTABLEFULL;
}

/*
	finds the an entry of the index next to the entry of specified scan descriptor

	*** parameters ***
	int scanDesc - scan descriptor of the index to find the next entry

	*** return values ***
	RECID of the next entry if successful
	RECID containing NODE_NULLPTR(-1) if a problem occurred
*/
RECID AM_FindNextEntry(int scanDesc){
	RECID recid = ast[scanDesc].current;
	int op = ast[scanDesc].op;
	char* value = ast[scanDesc].value;
	char* record = (char *)calloc(ast[scanDesc].attrLength, sizeof(char));
	char* record_temp = (char *)calloc(ast[scanDesc].attrLength, sizeof(char));
	int match = 0;

	RECID rec_err;
	RECID nodeAdr = ast[scanDesc].currentNode;
	rec_err.pagenum = NODE_NULLPTR;
	rec_err.recnum = NODE_NULLPTR;

	while(!match) {
		printf("getNextValue: %d, %d / %d, %d\n", recid.pagenum, recid.recnum, nodeAdr.pagenum, nodeAdr.recnum);

		if (recid.pagenum == AME_SCANOPEN && recid.recnum == AME_SCANOPEN) {
			recid = Btr_getFirstValue(ast[scanDesc].fd, &record, &nodeAdr);
		}
		else {
			/* copy from record to record_temp */
			if (memcpy(record_temp, record, ast[scanDesc].attrLength) == NULL){
				printf("AM_FindNextEntry failed: memcpy from record to record_temp\n");
				return rec_err;
			}
			/* reset record */
			if (memset(record, 0, ast[scanDesc].attrLength) == NULL){
				printf("AM_FindNextEntry failed: memset to 'record'\n");
				return rec_err;
			}
			recid = Btr_getNextValue(ast[scanDesc].fd, &record, &nodeAdr);
		}

		if (recid.pagenum == -1) {
			AMerrno = AME_EOF;
			return recid;
		}

		if(value == NULL) match = 1;
		else if (ast[scanDesc].attrType == 'c'){
			int result = strncmp(record, value, ast[scanDesc].attrLength);

			if (op == 1) match = result == 0;
			else if (op == 2) match = result < 0;
			else if (op == 3) match = result > 0;
			else if (op == 4) match = result <= 0;
			else if (op == 5) match = result >= 0;
			else if (op == 6) match = result != 0;
			else return rec_err;
		} else if (ast[scanDesc].attrType == 'i'){

			int src, dst;
			if (memcpy(&src, record, ast[scanDesc].attrLength) == NULL || memcpy(&dst, value, ast[scanDesc].attrLength) == NULL) {
                return rec_err;
            }

			if (op == 1) match = src == dst;
			else if (op == 2) match = src < dst;
            else if (op == 3) match = src > dst;
            else if (op == 4) match = src <= dst;
            else if (op == 5) match = src >= dst;
            else if (op == 6) match = src != dst;
            else return rec_err;
		} else if (ast[scanDesc].attrType == 'f'){
			float src, dst;
			if (memcpy(&src, record, ast[scanDesc].attrLength) == NULL || memcpy(&dst, value, ast[scanDesc].attrLength) == NULL) {
                return rec_err;
            }

			if (op == 1) match = src == dst;
            else if (op == 2) match = src < dst;
            else if (op == 3) match = src > dst;
            else if (op == 4) match = src <= dst;
            else if (op == 5) match = src >= dst;
            else if (op == 6) match = src != dst;
            else return rec_err;
		} else return rec_err;
	}

	ast[scanDesc].current = recid;
	ast[scanDesc].currentNode = nodeAdr;
	return recid;
}

/*
	closes the specified scan

	*** parameters ***
	int scanDesc - scan descriptor of the index to be closed

	*** return values ***
	AME_OK when closing is finished
*/
int AM_CloseIndexScan(int scanDesc){
	ast[scanDesc].valid = FALSE;
	return AME_OK;
}

/*
	prints the given error message

	*** parameters ***
	char * errString - pointer to the given error message

	*** return values ***
	void
*/
void AM_PrintError(char *errString){
	fprintf(stderr, "%s\n", errString);
}
