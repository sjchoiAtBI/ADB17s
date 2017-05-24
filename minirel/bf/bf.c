#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "minirel.h"
#include "bf.h"
/* #include "pf.h" */
#include "custom.h"

#define BF_INVALID NULL
#define CHAR_INVALID NULL
#define PFHDR_SIZE PAGE_SIZE
#define BF_MINIMUM 0

BFpage *LRU_head = BF_INVALID;
BFpage *LRU_tail = BF_INVALID;
BFpage *Fr_head = BF_INVALID;
int BF_cnt = BF_MINIMUM;

/* BF_Init is initiate to use BF layer.
   First, initiate Hash Table, and secondly, make a Free List and LRU List
   When initiate, no page is existed in buffer pool, so all of BFpage are linked in Free List
   And finally, set a counter(BF_cnt) to 0 which indicate how many pages in buffer */
void BF_Init(void) {
	BFpage *buf[BF_MAX_BUFS];
	int i;

    BF_InitHash();
	/* set free list. Fr_head points first page directly */
	buf[0] = (BFpage*)malloc(sizeof(BFpage));
	Fr_head = buf[0];
	for (i = 0; i < BF_MAX_BUFS - 1; i++) {
		buf[i + 1] = (BFpage*)malloc(sizeof(BFpage));
		buf[i]->nextpage = buf[i + 1];
	}

	/* set LRU list. LRU_head,tail point each other */
	LRU_head = (BFpage*)malloc(sizeof(BFpage));
	LRU_tail = (BFpage*)malloc(sizeof(BFpage));
	LRU_head->count = 1;
	LRU_tail->count = 1;
	LRU_head->nextpage = LRU_tail;
	LRU_tail->prevpage = LRU_head;
	BF_cnt = 0;
}

/* BF_AllocBuf makes a new BFpage in Buffer Pool and point the page by fpage
   If same page already exist in Buffer Pool, return error message.
   If not, make a new BFpage and add it on Buffer Pool
    If adding is failed because of pinned pages in Buffer Pool, return error message.
    If all of these operation completed without error, return BFE_OK message

    params: bq = the property of required page, fpage = pointer which point the targeted page when return
    return: BFE_OK = complete, BFE_NOBUF = BF is full, BFE_PAGEINBUF = page already in Buffer Pool */

int BF_AllocBuf(BFreq bq, PFpage **fpage) {
	BFpage* new_page;
	/* if the page doesn't exist in Buffer Pool, make a new page */
	if (BF_SearchHash(bq.fd, bq.pagenum, NULL) != BFE_OK) {

		/* if page fulled, find victim and delete */
		if (BF_cnt >= BF_MAX_BUFS) {
			if (del_victim() != BFE_OK) {
				return BFE_NOBUF; /* ERROR: Buffer Pool is fully accupied by pinned pages! */
			}
		}

		/* make new page file */
		new_page = Fr_head;
		Fr_head = Fr_head->nextpage;

		new_page->dirty = FALSE;
		new_page->count = 1;
		new_page->unixfd = bq.unixfd;
		new_page->fd = bq.fd;
		new_page->pageNum = bq.pagenum;

		/* insert the "new_page" */
		if (insert_in_LRU(new_page) == BFE_OK){

			if (fpage) {
				*fpage = &(new_page->fpage);
			}

			return BFE_OK;
		}
		return BFE_NOBUF; /* ERROR: Buffer Pool is fully accupied by pinned pages! */
	}
	return BFE_PAGEINBUF; /* ERROR: already exist in Buffer Pool! */
}


/* find the requested page and point the page by 'fpage'
 	 if the requested page exists in Buffer Pool, increase its pin point and  point it by fpage
	 else if the requested page isn't in Buffer Pool, call it from disk and alloc it on Buffer Pool
	 if cannot be inserted in Buffer Pool because of pinned page in Buffer Pool, return error message

	 params: bq = the property of required page, fpage = pointer which point the targeted page when return
	 return: BFE_OK = complete, BFE_NOBUF = BF is full */

int BF_GetBuf(BFreq bq, PFpage **fpage) {

	BFhash_entry* hash_page = NULL;
	BFpage* new_page;
	char temp[PAGE_SIZE];

	/* if already exist in Buffer Pool, return */
	if (BF_SearchHash(bq.fd, bq.pagenum, &hash_page) == BFE_OK) {

		BFpage* get_page = hash_page->bpage;
		get_page->count++;

		if (fpage){
			*fpage = &(get_page->fpage);
		}
		return BFE_OK;
	}

	/* if Buffer Pool has no space, make free space */
	if (BF_cnt >= BF_MAX_BUFS) {
		if (del_victim() != BFE_OK){
			return BFE_NOBUF; /* ERROR: Buffer Pool is fully accupied by pinned pages! */
		}
	}

	new_page = Fr_head;

	/* if the requested page is in disk, alloc it on Buffer Pool, and return */
	/* make 'new_page' and copy 'bfpage' which is found in disk */
	Fr_head = Fr_head->nextpage;

	if (pread(bq.unixfd, new_page->fpage.pagebuf, PAGE_SIZE, PFHDR_SIZE + PAGE_SIZE * bq.pagenum) != PAGE_SIZE) {
		return BFE_UNIX;
	}
	new_page->dirty = FALSE;
	new_page->count = 1;
	new_page->unixfd = bq.unixfd;
	new_page->fd = bq.fd;
	new_page->pageNum = bq.pagenum;

	/* insert 'new_page' into buffer Pool, and point PFpage of 'new_page' by fpage */
	if (insert_in_LRU(new_page) == BFE_OK) {
		*fpage = &(new_page->fpage);
		return BFE_OK;
	}

	return BFE_NOBUF; /* ERROR: Buffer Pool is fully accupied by pinned pages! */
}


/* BF_UnpinBuf decreases pin count of targeted page in Buffer Pool
 	 If targeted page isn't exist or already unpinned, return error message

 	 params: bq = the property of required page
 	 return: bFE_OK = complete, BFE_PAGEUNFIXED = already unpinned, BFE_PAGENOTINBUF = page not in BF */

int BF_UnpinBuf(BFreq bq) {
	BFhash_entry* hash_page = NULL;

	if (BF_SearchHash(bq.fd, bq.pagenum, &hash_page) == BFE_OK) {
		if (hash_page->bpage->count > 0) {
			hash_page->bpage->count--;
			/* printf("%d, %d new count: %d\n", bq.fd, bq.pagenum,  hash_page->bpage->count); */
			return BFE_OK;
		}
		return BFE_PAGEUNFIXED; /* ERROR: page is already unpinned! */
	}
	return BFE_PAGENOTINBUF; /* ERROR: page is not in Buffer Pool! */
}


/* BF_TouchBuf makes the targeted page dirty, and moves the page on MRU area
   if targeted page is unpinned or isn't exist, return error message

   params: bq = the property of required page
   return: BFE_OK = complete, BFE_PAGEUNFIXED = page unpinned, BFE_PAGENOTINBUF = target is not in BF */

int BF_TouchBuf(BFreq bq) {

	BFhash_entry* hash_page = NULL;

	/* find the page which we want to make dirty, and named it 'dir_page' */
	if (BF_SearchHash(bq.fd, bq.pagenum, &hash_page) == BFE_OK) {
		BFpage* dir_page = hash_page->bpage;

		/* if 'dir_page' is unpinned, return error */
		if (dir_page->count == 0){
			return BFE_PAGEUNFIXED; /* ERROR: page is not pinned! */
		}

		dir_page->dirty = TRUE;

	/* relocate the touched page to MRU */
		dir_page->prevpage->nextpage = dir_page->nextpage;
		dir_page->nextpage->prevpage = dir_page->prevpage;

		dir_page->prevpage = LRU_head;
		dir_page->nextpage = LRU_head->nextpage;
		LRU_head->nextpage->prevpage = dir_page;
		LRU_head->nextpage = dir_page;

		return BFE_OK;
	}
    return BFE_PAGENOTINBUF; /* ERROR: no requested page in Buffer Pool! */
}


/* BF_FlushBuf flushs out all of pages whose file descriptor is as same as request
 	 It scan all of pages in LRU List, and if file descriptor of the page is 'fd', flush it out
	 If flushed page is dirty, save the contents of page in the disk
	 If the page is pinned, return error message

	 params: fd = file descriptor
	 return: BFE_OK = complete, BFE_PAGEFIXED = target is pinned, BFE_UNIX = UNIX write error */

int BF_FlushBuf(int fd) {

	BFpage* Flushed_page = LRU_head;
	BFpage* temp = Flushed_page->nextpage;

	while (1) {

		Flushed_page = temp;

		/* if 'Flushed_page' points end of the page, break the loop */
		if (Flushed_page == LRU_tail){
			break;
		}
		else {
			temp = temp->nextpage;
		}

		if (Flushed_page->fd == fd) {

			/* if 'Flushed_page' is pinned, return error message */
			if (Flushed_page->count != 0) {
				/* printf("BFE_PAGEFIXED %d\n", Flushed_page->count); */
				printf("allocbuf: pagefixed");
				return BFE_PAGEFIXED;
			}

			/* if 'Flushed_page' is dirty, write PFpage of it on the disk */
			if (Flushed_page->dirty == TRUE) {
				if (pwrite(Flushed_page->unixfd, Flushed_page->fpage.pagebuf, PAGE_SIZE, PFHDR_SIZE + PAGE_SIZE * Flushed_page->pageNum) != PAGE_SIZE) {
					printf("allocbuf: pwrite");
					return BFE_UNIX;
				}
			}

			memset(Flushed_page->fpage.pagebuf, 0, PAGE_SIZE);

			/* delete from LRU List */
			Flushed_page->prevpage->nextpage = Flushed_page->nextpage;
			Flushed_page->nextpage->prevpage = Flushed_page->prevpage;
			BF_DeleteHash(fd, Flushed_page->pageNum, NULL);
			BF_cnt--;

			/* insert in Free List */
			Flushed_page->nextpage = Fr_head;
			Fr_head = Flushed_page;
		}

	}

    return BFE_OK;
}


/* BF_ShowBuf shows the status of Buffer Pool */

void BF_ShowBuf(void) {
	BFpage* cur_page = LRU_head->nextpage;
	int i;

	printf ("The buffer pool content:\n");

	if (BF_cnt == 0) {
		printf("empty\n");
		return;
	}

	printf ("pageNum\tfd\tunixfd\tcount\tdirty\n");

	for (i = 0; i < BF_cnt; i++){
		printf("%d\t%d\t%d\t%d\t%d\n", cur_page->pageNum, cur_page->fd, cur_page->unixfd, cur_page->count, cur_page->dirty);
		cur_page = cur_page->nextpage;
	}
}

/*
* additional functions.
*/


/* insert 'bfpage' in LRU list with highest priority (MRU area)

    params: bfpage = page which we want to insert in LRU
    return: BFE_OK = complete */

int insert_in_LRU(BFpage* bfpage) {

	/* link fpage on LRU list */
	bfpage->nextpage = LRU_head->nextpage;
	bfpage->prevpage = LRU_head;
	LRU_head->nextpage->prevpage = bfpage;
	LRU_head->nextpage = bfpage;

	/* insert bfpage in hashtable */
	BF_InsertHash(bfpage);

	BF_cnt++;
	return BFE_OK;
}


/* delete victim to make free space in LRU list
	 find the unpinned page, called victim, while scan LRU to MRU
	 if victim is dirty, save its contents on disk
	 and if deleting a victim completed, BF_cnt decrease by one, and return success message

	 return: BFE_OK = complete, BFE_NOBUF = BF is full, BFE_HASHNOTFOUND = cannot find victim in hashtable */

int del_victim(void) {

	BFpage* Unpinned = LRU_tail->prevpage;

	/* find unpinned page and point it by 'Unpinned' */
	while (Unpinned->count != 0) {

		/* if there is not unpinned page, return error */
		if (Unpinned == LRU_head) {
			return BFE_NOBUF; /* ERROR: full of pinned page! */
		}
		Unpinned = Unpinned->prevpage;
	}

	/* unlink the page, pointed by 'Unpinned', in LRU */
	Unpinned->prevpage->nextpage = Unpinned->nextpage;
	Unpinned->nextpage->prevpage = Unpinned->prevpage;

	/* write the page content to the file, the page is dirty */
	if (Unpinned->dirty == TRUE) {
		pwrite(Unpinned->unixfd, Unpinned->fpage.pagebuf, PAGE_SIZE, PFHDR_SIZE + PAGE_SIZE * Unpinned->pageNum);
	}

	/* delete 'Unpinned' in Hashtable */
	if (BF_DeleteHash(Unpinned->fd, Unpinned->pageNum, NULL) != BFE_OK){
		return BFE_HASHNOTFOUND;
	}

	/* insert Unpinned in Free List */
	Unpinned->nextpage = Fr_head;
	Fr_head = Unpinned;
	BF_cnt--;
	return BFE_OK;
}

/* Entry for Hash table. */
typedef struct BFhash_key {
    int fd;                             /* file descriptor                 */
    int pageNum;                        /* page number                     */
} BFhash_key;

/* Hash table pointer and hash key length. */
BFhash_entry *hash = NULL;
unsigned keylen;

/* Initialize hash table. */
void BF_InitHash() {
	/* Reference: http://troydhanson.github.io/uthash/ */
    /* Init keylen for future use. */
    BFhash_key * lookup_key;
    int tempPageNum = 0;

    /* calculate the key length including padding, using formula */
    keylen =   offsetof(BFhash_entry, pageNum)       /* offset of last key field */
             + sizeof(tempPageNum)             /* size of last key field */
             - offsetof(BFhash_entry, fd);  /* offset of first key field */
}

/* Insert a bpage to hash table.
	- bpage		: pointer to the page to insert.

	return value: status code defined in BF layer.
*/
int BF_InsertHash(BFpage *bpage) {
    BFhash_entry *tmp = NULL, *new = NULL;

	/* Find hash with given key. */
    HASH_FIND(hh, hash, &bpage->fd, keylen, tmp);

	/* Init new hash engry. */
    new = (BFhash_entry *) malloc(sizeof(BFhash_entry));
    new->nextentry = NULL;
    new->preventry = NULL;
    new->fd = bpage->fd;
    new->pageNum = bpage->pageNum;
    new->bpage = bpage;

	/* Check collision. */
    if (tmp) {
        /* Collision. Add to the chain. */
        while (tmp->nextentry) {
            tmp = tmp->nextentry;
        }

        tmp->nextentry = new;
        new->preventry = tmp;
    } else {
		/* No collision. Insert to hash table. */
        HASH_ADD(hh, hash, fd, keylen, new);
    }

    return BFE_OK;
}

/* Search hash table with gien fd and pageNum.
	- fd		: PF layer's file descripter to search.
	- pageNum	: index of the page to search.
	- hash_entry: pointer of pointer where the address of found hash entry will be assigned.

	return value: status code defined in BF layer.
*/
int BF_SearchHash(int fd, int pageNum, BFhash_entry **hash_entry) {
    BFhash_key key;
    BFhash_entry *tmp = NULL;

    key.fd = fd;
    key.pageNum = pageNum;

	/* Find hash table. */
    HASH_FIND(hh, hash, &key.fd, keylen, tmp);

	/* Follow hash entry and it's chain until meet corresponding entry. */
    while (tmp) {
        if (tmp->fd == fd && tmp->pageNum == pageNum) {
            if (hash_entry) {
                *hash_entry = tmp;
            }
            break;
        }

        tmp = tmp->nextentry;
    }

	/* Return BFE signal accordingly. */
    return tmp ? BFE_OK : BFE_HASHNOTFOUND;
}

/* Delete a hash entry with given fd and pageNum.
	- fd		: PF layer's file descripter to delete.
	- pageNum	: index of the page to delete.
	- hash_entry: pointer of pointer where the address of deleteed hash entry will be assigned.

	return value: status code defined in BF layer.
*/
int BF_DeleteHash(int fd, int pageNum, BFpage **bpage) {
    BFhash_key key;
    BFhash_entry *tmp = NULL;

    key.fd = fd;
    key.pageNum = pageNum;

	/* Search hash table. */
    if (BF_SearchHash (fd, pageNum, &tmp) == BFE_OK && tmp) {
        if (tmp->preventry == NULL) {
            /* The first element of this hash chain. */
            if (tmp->nextentry == NULL) {
                /* And the last element. */
                HASH_DELETE(hh, hash, tmp);
            } else {
				/* If the chain is longer than 1, replace the first entry with the second one. */
                HASH_REPLACE(hh, hash, fd, keylen, tmp->nextentry, tmp);
                tmp->nextentry->preventry = NULL;
            }
        } else {
			/* If this entry is in the middle of the chain, do pointer job. */
            tmp->preventry->nextentry = tmp->nextentry;
        }

		/* Return bpage which was tagged, if possible. */
        if (bpage) {
            *bpage = tmp->bpage;
        }

        free(tmp);

        return BFE_OK;
    }

    return BFE_HASHNOTFOUND;
}
