#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "minirel.h"
#include "bf.h"
#include "pf.h"
#include "custom.h"

#define FOPEN_NOFILE (-1)
#define PFHDR_PNUM_INIT 0
#define CLOSE_SUCCESS 0
#define STAT_SUCCESS 0
#define REMOVE_SUCCESS 0
#define READ_FAILURE (-1)
#define INODE_INVALID (-1)
#define FNAME_INVALID NULL
#define	UNIXFD_INVALID (-1)
#define PFHDR_PNUM_INVALID (-1)
#define PFHDR_HDRC_INVALID (-1)
#define PAGENUM_MINIMUM 0
#define FILE_BEGINNING 0
#define SAME_STRING 0


/*
   Check the validity of file table entry or the page number value.

   *** parameters ***
   int fd - PF file descriptor of the checked page
   int pagenum - page number of the checked page

   *** return value ***
   PFE_FD - when the given PF file descriptor is invalid (file not open)
   PFE_EOF - if the given page number equals the value of 'numpages' of PF header,
   			indicating that EOF was reached
   PFE_INVALIDPAGE - if the value of given page number itself is inappropriate
   PFE_OK - when the given file and its page number are both valid
 */
int PF_IsValidPage(int fd, int pagenum){
	if (pft[fd].valid == FALSE){ /* checking the validity of the file table entry itself */
		return PFE_FD;
	} else if (pft[fd].hdr.numpages == pagenum){ /* checking EOF */
		return PFE_EOF;
	} else if (pft[fd].hdr.numpages < pagenum || pagenum < PAGENUM_MINIMUM) { /* checking the validity of pagenum value */
		return PFE_INVALIDPAGE;
	}
	/* Validity of the page verified */
	return PFE_OK;
}


/*
   if the given PF file descriptor is valid, return the number of pages in *pagenum

   *** parameters ***
   int fd - PF file descriptor
   int * pagenum - address of the int variable where number of pages of the given file will be returned

   *** return value ***
   PFE_FD - when the given file table entry itself is invalid
   PFE_INVALIDPAGE - when the page number of the given file table entry is invalid
   PFE_OK - number of pages of the given file successfully saved at *pagenum
 */
int PF_GetNumPages(int fd, int* pagenum){
	/* Check validity of the file table entry and its saved page number*/
	if(pft[fd].valid == FALSE){
		return PFE_FD;
	} else if (pft[fd].hdr.numpages < PFHDR_PNUM_INIT) {
		return PFE_INVALIDPAGE;
	}

	/* save number of pages at *pagenum if fd is valid */
	*pagenum = pft[fd].hdr.numpages;
	return PFE_OK;
}


/*
	initialize the PF layer - invoke BF_Init() & initialize the file table
*/
void PF_Init		(void) {
	int i;
	BF_Init(); /* initialize the BF layer */
	pft = (PFftab_ele *)calloc(PF_FTAB_SIZE, sizeof(PFftab_ele)); /* initialize the file table */

	/* initialize each file table entry */
	for (i = 0; i < PF_FTAB_SIZE; i++){
		pft[i].valid = FALSE;
		pft[i].inode = INODE_INVALID;
		pft[i].fname = FNAME_INVALID;
		pft[i].unixfd = UNIXFD_INVALID;
		pft[i].hdr.numpages = PFHDR_PNUM_INVALID;
		pft[i].hdrchanged = PFHDR_HDRC_INVALID;
	}

	return;
}

/*
    creates a file named 'filename', which SHOULD NOT have already existed
	use UNIX system call open() to create the file
	PF file header initialized & written to the file
	file closed using the 'close()' UNIX system call

    *** parameters ***
	char * filename - name of the file to be created

	*** return values ***
	PFE_FILEOPEN - when the file with the given filename already exists
	PFE_HDRWRITE - when an error has occurred while writing header to the file
	PFE_UNIX - when an error has occurred during the system call close()
	PFE_OK - file successfully created and closed
*/
int  PF_CreateFile	(char *filename) {

    int file_fd; /* UNIX file descriptor returned from the system call open() */
	PFhdr_str file_hdr; /* content of the header to be written on the created file */

	/* Checking whether the file already exists */
	file_fd = open(filename, O_RDONLY);
	if(file_fd != FOPEN_NOFILE){
		close(file_fd);
		return PFE_FILEOPEN; /* file already exists */
	}

	/* Creating file, writing the header */
	file_fd = open(filename, O_WRONLY|O_CREAT);
	file_hdr.numpages = PFHDR_PNUM_INIT;
	if (write(file_fd, &file_hdr, sizeof(PFhdr_str)) != sizeof(PFhdr_str)){
		return PFE_HDRWRITE;
	}

	/* Closing the file */
	if (close(file_fd) == CLOSE_SUCCESS) {
		return PFE_OK;
	}

	/* when an error has occurred during the system call close() */
	return PFE_UNIX;
}


/*
	destroys the file 'filename', which SHOULD have existed and SHOULD NOT already be opened
	uses UNIX system call remove()

	*** parameters ***
	char * filename - name of the file to be destroyed

	*** return values ***
	PFE_FILEOPEN - when the specified file exists and is open
	PFE_UNIX - when an error has occurred during the system call remove() or open()
	PFE_OK - when the file was successfully destroyed
*/
int  PF_DestroyFile	(char *filename) {

	int file_fd;
	int i;
	struct stat stat_file;

 	/* Checking whether the file actually exists */
	file_fd = open(filename, O_RDONLY);
	if(file_fd == FOPEN_NOFILE){ printf("pf1\n");
		return PFE_UNIX;
	}
	close(file_fd);

	/* checking whether there is an open file with the same filename */
	for(i = 0; i < PF_FTAB_SIZE; i++){
		if ((pft[i].fname != FNAME_INVALID) && (strcmp(pft[i].fname, filename) == SAME_STRING) && (pft[i].valid == TRUE)){printf("pf2\n");
			return PFE_FILEOPEN;
		}
	}

	/* destroying the file using the system call remove() */
	if (remove(filename) != REMOVE_SUCCESS){printf("pf3\n");
		return PFE_UNIX; /* when remove() fails and an error code is returned */
	}

	/* the specified file successfully destroyed */
    return PFE_OK;
}

/*
	opens the file 'filename' using the system call open()
	reads in the file header
	fields in the file table entry filled accordingly

	*** parameters ***
	char * filename - name of the file to be opened

	*** return values ***
	PF file descriptor(index of the file table) - when the file was successfully opened
	PFE_FILENOTOPEN - when the system call open() failed to open the specified file
	PFE_FILEOPEN - if the specified file was already opened
	PFE_HDRREAD - when system call read() failed to read in the information of the file header
	PFE_UNIX - when system call stat() failed
	PFE_FTABFULL - when the file table was full and failed to allocate an entry for the specified file
*/
int  PF_OpenFile	(char *filename) {
	int file_fd, i;
	int pft_idx;
	struct stat stat_file;

	/* checking whether there is an open file with the same filename */
	for(i = 0; i < PF_FTAB_SIZE; i++) {
		if ((pft[i].fname != FNAME_INVALID) && (strcmp(pft[i].fname, filename) == SAME_STRING) && (pft[i].valid == TRUE)) {
			return PFE_FILEOPEN;
		}
	}

	/* Opens the file if exists */
	file_fd = open(filename, O_RDWR);
	if (file_fd == FOPEN_NOFILE) {
		return PFE_FILENOTOPEN;
	}

	/* read in the file header, filling in the file table entry */
	for (pft_idx = 0; pft_idx < PF_FTAB_SIZE; pft_idx++){
		if (pft[pft_idx].valid == FALSE){
			if(read(file_fd, &pft[pft_idx].hdr, sizeof(PFhdr_str)) != sizeof(PFhdr_str)){
				close(file_fd);
				return PFE_HDRREAD; /* when read() failed and an error code was returned */
			}

		    /* use system call stat() to retrieve UNIX file information */
			if (stat(filename, &stat_file)!= STAT_SUCCESS){
				return PFE_UNIX; /* when stat() fails */
			}
			pft[pft_idx].valid = TRUE;
			pft[pft_idx].inode = stat_file.st_ino;
			pft[pft_idx].fname = (char *)calloc(strlen(filename), sizeof(char));
			strcpy(pft[pft_idx].fname, filename);
			pft[pft_idx].unixfd = file_fd;
			pft[pft_idx].hdrchanged = FALSE;

			/* when successfull, return the index of the PF file table allocated for the opened file */
			return pft_idx;
		}
	}

	/* when the file table was full, use system call close() and return error code */
	close(file_fd);
	return PFE_FTABFULL;
}

/*
	closes the file associated with the given PF file descriptor
	release all the buffer pages belonging to the file from LRU list to the free list
		BF_FlushBuf() is used
		dirty pages written back to the file if any
		all the buffer pages of a file must have been UNPINNED in order to be closed
	if the file header has changed, written back to the file
	file closed by using the system call close()
	the file table entry corresponding to the file is INVALIDATED

	*** parameters ***
	int fd - PF file descriptor of the file to be closed

	*** return values ***
	PFE_FILENOTOPEN - when the specified filed was not opened, so cannot be closed
	PFE_PAGEFREE - when an error has occurred during BF_FlushBuf()
	PFE_HDRWRITE - when an error has occurred while writing the header information back to the file
	PFE_UNIX - when an error has occurred during the system call close()
	PFE_OK - when the specified file was successfully closed
*/
int  PF_CloseFile	(int fd) {

	/* Check if the file was ever opened */
	if (pft[fd].valid == FALSE){
		printf("filenotopen\n");
		return PFE_FILENOTOPEN;
	}

	/* using BF_FlushBuf() to release all the buffer pages, writing dirty pages */
	if (BF_FlushBuf(fd) != BFE_OK) {
		printf("pagefree\n");
		return PFE_PAGEFREE;
	}

	/* Write the file header back to file if ever changed */
	if (pft[fd].hdrchanged == TRUE){
		if (pwrite(pft[fd].unixfd, &pft[fd].hdr, sizeof(PFhdr_str), FILE_BEGINNING)!= sizeof(PFhdr_str)){
		printf("hdrwrite\n");
			return PFE_HDRWRITE;
		}
	}

	/* close the file using close(), freeing the file table entry */
	if (close(pft[fd].unixfd) != CLOSE_SUCCESS){
		printf("unix");
		return PFE_UNIX; /*  when close() fails and an error code is returned */
	}
	pft[fd].valid = FALSE;

	/* the file was closed successfully */
    return PFE_OK;
}

/*
	new page appended to the end of the specified file
	allocates a buffer entry corresponding to the new page using BF_AllocBuf()
	pageNum for the page being allocated determined from the information stored in the file header
	page allocated PINNED and marked DIRTY using PF_DirtyPage()
	file header updated accordingly

	*** parameters ***
	int fd - PF file descriptor of the file where a new page is allocated
	int * pagenum - address of the int variable where page number of the allocated page is saved
	char ** pagebuf - pointer to the newly allocated PFpage content

	*** return values ***
	PFE_FILENOTOPEN - when the file with the given PF file descriptor was not opened in advance
	PFE_INVALIDPAGE - when an error has occurred during BF_AllocBuf() or PF_DirtyPage()
	PFE_OK - when page allocation was successful
*/
int  PF_AllocPage	(int fd, int *pagenum, char **pagebuf) {
	BFreq bq;
	PFpage * fpage = NULL;

	/* Check if the file was ever opened, valid for allocating a new page */
	if (pft[fd].valid == FALSE){
		return PFE_FILENOTOPEN;
	}

	/* determine pageNum by using the information in the file header */
	/* Allocate a buffer entry corresponding to the new page by using BF_AllocBuf */
	bq.fd = fd;
	bq.unixfd = pft[fd].unixfd;
	bq.pagenum = pft[fd].hdr.numpages;
	bq.dirty = TRUE;
	/* fpage = (PFpage *)malloc(sizeof(PFpage)); */

	if (BF_AllocBuf(bq, &fpage) != BFE_OK){
		return PFE_INVALIDPAGE;
	}

	/* if successful, update the file header */
	*pagenum = pft[fd].hdr.numpages; /* copy the index of allocated page to *pagenum */
	*pagebuf = fpage->pagebuf; /* assign the address of page content to given pointer */
	pft[fd].hdr.numpages++;
	pft[fd].hdrchanged = TRUE;

	/* PIN the page and mark DIRTY by using PF_DirtyPage */
	/* the page is already pinned if BF_AllocBuf() was successful */
	if (PF_DirtyPage(fd, bq.pagenum) != PFE_OK){
		return PFE_INVALIDPAGE;
	}

	/* check if the pagenum and pagebuf got approrpiate addresses */
	if (pagenum != NULL && pagebuf != NULL){
    	return PFE_OK;
	}

	return PFE_INVALIDPAGE;
}

/* Get first page of given fd.
	- fd		: PF layer's file descripter to find.
	- pagenum	: not being used. Will be assigned to zero after this function returns.
	- pagebuf	: pointer of pointer where the the address of found page will be assigned.

	return value: status code defined in PF layer. The address of found page will be assigned to pagebuf.
*/
int  PF_GetFirstPage	(int fd, int *pagenum, char **pagebuf) {
    * pagenum = -1;

	/* implemented using PF_GetNextPage. */
    return PF_GetNextPage (fd, pagenum, pagebuf);
}

/* Get next page of given fd and pagenum.
	- fd		: PF layer's file descripter to find.
	- pagenum	: index of the current page. It's next page will be found and it's pagenumber will be assigned to it.
	- pagebuf	: pointer of pointer where the the address of found page will be assigned.

	return value: status code defined in PF layer. The address of found page will be assigned to pagebuf.
*/
int  PF_GetNextPage	(int fd, int *pagenum, char **pagebuf) {
	int error;
	/* Check next page exists. */
	if ((error = PF_IsValidPage(fd, *pagenum + 1)) != PFE_OK) {
		return error;
    }

	/* If exists, call PF_GetThisPage with increased pagenum. */
    *pagenum = *pagenum + 1;
    return PF_GetThisPage (fd, *pagenum, pagebuf);
}

/* Get a page with given fd and pagenum.
	- fd		: PF layer's file descripter to find.
	- pagenum	: index of the page to find.
	- pagebuf	: pointer of pointer where the the address of found page will be assigned.

	return value: status code defined in PF layer. The address of found page will be assigned to pagebuf.
*/
int  PF_GetThisPage	(int fd, int pagenum, char **pagebuf) {
	BFreq bq;
	PFpage *fpage;

	/* Check such page exists. */
	if (PF_IsValidPage(fd, pagenum) != PFE_OK) {
		return PFE_INVALIDPAGE;
	}

	/* Init BFreq. */
	bq.fd = fd;
	bq.unixfd = pft[fd].unixfd;
	bq.pagenum = pagenum;

	/* Call BF_GetBuf to get the page. */
	if (BF_GetBuf(bq, &fpage) == BFE_OK) {
		*pagebuf = fpage->pagebuf;
		return PFE_OK;
	} else {
		return PFE_NOUSERS;
	}
}

/* Make a page dirty with given fd and pagenum.
	- fd		: PF layer's file descripter to make dirty.
	- pagenum	: index of the page to make dirty.

	return value: status code defined in PF layer.
*/
int  PF_DirtyPage	(int fd, int pagenum) {
    BFreq bq;

	/* Check such page exists. */
    if (PF_IsValidPage(fd, pagenum) != PFE_OK) {
        return PFE_INVALIDPAGE;
    }

	/* Init BFreq. */
    bq.fd = fd;
    bq.pagenum = pagenum;

	/* Call BF_TouchBuf to touch the page. */
    if (BF_TouchBuf(bq) == BFE_OK) {
        return PFE_OK;
    } else {
        return PFE_NOUSERS;
    }
}

/* Unpin a page dirty with given fd and pagenum.
	- fd		: PF layer's file descripter to unpin.
	- pagenum	: index of the page to unpin.
	- dirty		: set if also want to make it dirty.

	return value: status code defined in PF layer.
*/
int  PF_UnpinPage	(int fd, int pagenum, int dirty) {
    BFreq bq;

	/* Check such page exists. */
    if (PF_IsValidPage(fd, pagenum) != PFE_OK) {
        return PFE_INVALIDPAGE;
    }

	/* Init BFreq. */
    bq.fd = fd;
    bq.pagenum = pagenum;

	if (dirty) {
		if (BF_TouchBuf(bq) != BFE_OK) {
			return PFE_INVALIDPAGE;
		}
	}

	/* Call BF_UnpinBuf to unpin the page. */
    if (BF_UnpinBuf(bq) == BFE_OK) {
        return PFE_OK;
    } else {
        return PFE_INVALIDPAGE;
    }
}
