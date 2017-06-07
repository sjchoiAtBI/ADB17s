#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "minirel.h"
#include "bf.h"
#include "pf.h"
#include "hf.h"
#include "custom.h"

int HFerrno;

/* HF file table element. */
typedef struct  HFftab_ele {
    bool_t valid;
    HFHeader hfheader;
    int pfd;
} HFftab_ele;

/* HF scan table element. */
typedef struct  HFstab_ele {
    bool_t valid;
    int hfd;
    char attrType;
    int attrLength;
    int attrOffset;
    int op;
    char *value;
    RECID current;
} HFstab_ele;

HFftab_ele *hft = NULL;
HFstab_ele *hst = NULL;

int setFirstRec = 0;

/* Write HF header to header page.
    - pfd: PF layer fd.
    - hfheader: header page.

    return value: satus code.
*/
int write_header(int pfd, HFHeader *hfheader) {
    PFftab_ele *pfte = &(pft[pfd]);
    pfte->hdrchanged = TRUE;
    return memcpy(pfte->hdr.hdrrest, hfheader, sizeof(HFHeader)) != NULL ? HFE_OK : HFE_INTERNAL;
}

/* Init HF layer variables. */
void HF_Init() {
    int i;

    PF_Init();

    hft = (HFftab_ele *)calloc(HF_FTAB_SIZE, sizeof(HFftab_ele));
    for (i = 0; i < HF_FTAB_SIZE; i++) {
        hft[i].valid = FALSE;
        hft[i].hfheader.RecSize = 0;
        hft[i].hfheader.RecPage = 0;
        hft[i].hfheader.NumPg = 0;
    }

    hst = (HFstab_ele *)calloc(MAXSCANS, sizeof(HFstab_ele));
    for (i = 0; i < MAXSCANS; i++) {
        hst[i].valid = FALSE;
    }
}

/* Create new file with PF layer functions.
    - fileName: name of the file.
    - recSize: size of each record.

    return value: satus code.
*/
int HF_CreateFile(char *fileName, int recSize) {
    int pfd;
    double _recSize;
    PFftab_ele *pfte = NULL;
    HFHeader hfheader;

    if (PF_CreateFile(fileName) != PFE_OK) {
        return HFE_PF;
    }

    if ((pfd = PF_OpenFile(fileName)) < 0) {
        return HFE_PF;
    }

    hfheader.RecSize = recSize;
    _recSize = ((double) recSize) + 0.125;
    hfheader.RecPage = ((double)PAGE_SIZE) / _recSize ;
    /* printf("HF_CreateFile: %s, %d / (%d + 0.125) = %d\n", fileName,  PAGE_SIZE, recSize, hfheader.RecPage); */
    hfheader.NumPg = 0;

    if (write_header(pfd, &hfheader) != HFE_OK) {
        PF_CloseFile(pfd);
        return HFE_PF;
    }

    return PF_CloseFile(pfd) == PFE_OK ? HFE_OK : HFE_PF;
}

/* Destroy a file.
    - fileName: name of the file.

    return value: satus code.
*/
int HF_DestroyFile(char *fileName) {
    return PF_DestroyFile(fileName) == PFE_OK ? HFE_OK : HFE_PF;
}

/* Open a existing file.
    - fileName: name of the file.

    return value: HF layer fd.
*/
int HF_OpenFile(char *fileName) {
    int hfd;
    int pfd;

    if ((pfd = PF_OpenFile(fileName)) < 0) {
        return HFE_PF;
    }

    for (hfd = 0; hfd < HF_FTAB_SIZE; hfd++) {
        HFftab_ele *hfte = &(hft[hfd]);
        if (hfte->valid == FALSE) {
            if (memcpy(&(hfte->hfheader), pft[pfd].hdr.hdrrest, sizeof(HFHeader)) == NULL || hfte->hfheader.RecSize <= 0 || hfte->hfheader.RecPage <= 0) {
                PF_CloseFile(pfd);
                return HFE_PF;
            }

            hfte->valid = TRUE;
            hfte->pfd = pfd;

            return hfd;
        }
    }

    PF_CloseFile(pfd);
    return HFE_FTABFULL;
}

/* Close a open file.
    - HFfd: HF layer fd.

    return value: satus code.
*/
int HF_CloseFile(int HFfd) {
    int pfd = hft[HFfd].pfd;

    if (write_header(pfd, &(hft[HFfd].hfheader)) != HFE_OK) {
        /* printf("here\n"); */
        return HFE_PF;
    }

    if (PF_CloseFile(pfd) != PFE_OK) {
        /* printf("here2 %d\n", PF_CloseFile(pfd)); */
        return HFE_PF;
    }

    hft[HFfd].valid = FALSE;
    hft[HFfd].hfheader.RecSize = 0;
    hft[HFfd].hfheader.RecPage = 0;
    hft[HFfd].hfheader.NumPg = 0;

    return HFE_OK;
}

/* Insert a record to HF fd.
    - HFfd: fd of HF layer.
    - record: pointer to record content.

    return value: record id of inserted position.
*/
RECID HF_InsertRec(int HFfd, char *record) {
    HFftab_ele *hfte = &(hft[HFfd]);
    int recSize = hfte->hfheader.RecSize;
    RECID recid;
    int pagenum, recnum;
    char *pagebuf;
    int byte, bit;
    char map;

    recid.pagenum = -1;
    recid.recnum = HFE_PF;

    pagenum = -1;
    while (1) {
        int err = PF_GetNextPage(hfte->pfd, &pagenum, &pagebuf);

        if (err == PFE_EOF) {
            if (PF_AllocPage (hfte->pfd, &pagenum, &pagebuf) != PFE_OK || PF_UnpinPage(hfte->pfd, pagenum, 1) != PFE_OK) {
                return recid;
            }

            /* printf("allocating page %d\n", hfte->hfheader.NumPg); */
            hfte->hfheader.NumPg++;
            pagenum--;
            continue;
        } else if (err != PFE_OK) {
            return recid;
        }

        for (recnum = 0; recnum < hfte->hfheader.RecPage; recnum++) {
            byte = recnum / 8;
            bit = recnum % 8;

            map = pagebuf[recSize * hfte->hfheader.RecPage + byte];

            if (((map >> bit) & 0x01) == 0) {
                if (memcpy(pagebuf + recSize * recnum, record, recSize) == NULL) {
                    PF_UnpinPage(hfte->pfd, pagenum, 0);
                    return recid;
                }

                /* printf("insert %s at %d, %d. map changes from %x, to %x\n", record, pagenum, recnum, map & 0xFF, map | (0x01 << bit)); */

                pagebuf[recSize * hfte->hfheader.RecPage + byte] = map | (0x01 << bit);

                if (PF_UnpinPage(hfte->pfd, pagenum, 1) == PFE_OK) {
                    recid.pagenum = pagenum;
                    recid.recnum = recnum;
                }

                return recid;
            }
        }

        if (PF_UnpinPage(hfte->pfd, pagenum, 0) != PFE_OK) {
            return recid;
        }
    }
}

/* Delete a record.
    - HFfd: fd of HF layer.
    - recId: record id of which will be deleted.

    return value: status code.
*/
int HF_DeleteRec(int HFfd, RECID recId) {
    HFftab_ele *hfte = &(hft[HFfd]);
    int recSize = hfte->hfheader.RecSize;
    char *pagebuf;
    int byte, bit;
    char map;

    if (PF_GetThisPage(hfte->pfd, recId.pagenum, &pagebuf) != PFE_OK) {
        return HFE_PF;
    }

    byte = recId.recnum / 8;
    bit = recId.recnum % 8;

    map = pagebuf[recSize * hfte->hfheader.RecPage + byte];
    pagebuf[recSize * hfte->hfheader.RecPage + byte] = map & (0xFF - (0x01 << bit));

    /* printf("delete %d, %d. map changes from %x, to %x\n", recId.pagenum, recId.recnum, map & 0xFF, map & (0xFF - (0x01 << bit))); */

    return PF_UnpinPage(hfte->pfd, recId.pagenum, 1) == PFE_OK ? HFE_OK : HFE_PF;
}

/* Get the first record of a file.
    - HFfd: fd of HF layer.
    - record: pointer where read content will be copied.

    return value: record id of which is found.
*/
RECID HF_GetFirstRec(int HFfd, char *record) {
    RECID recid;
    recid.pagenum = 0;
    recid.recnum = 0;

    setFirstRec = 1;
    return HF_GetNextRec (HFfd, recid, record);
}

/* Get a record which is next to given position.
    - HFfd: fd of HF layer.
    - recId: record position where search start from. The record at this position is excluded from search.
    - record: pointer where read content will be cpoied.

    return value: position of next found record.
*/
RECID HF_GetNextRec(int HFfd, RECID recId, char *record) {
    HFftab_ele *hfte = &(hft[HFfd]);
    int recSize = hfte->hfheader.RecSize;
    RECID recid;
    int pagenum, recnum;
    char *pagebuf;
    int byte, bit;
    char map;

    recid.pagenum = -1;
    recid.recnum = HFE_PF;

    if (HF_ValidRecId(HFfd, recId) != TRUE) {
        recid.recnum = HFE_INVALIDRECORD;
        return recid;
    }

    pagenum = recId.pagenum - 1;
    while (1) {
        int err = PF_GetNextPage(hfte->pfd, &pagenum, &pagebuf);

        if (err == PFE_EOF) {
            /* printf("here %d, %d\n", pagenum, recId.pagenum); */
            recid.recnum = HFE_EOF;
            return recid;
        } else if (err != PFE_OK) {
            return recid;
        }

        if (pagenum == recId.pagenum) {
            recnum = recId.recnum + 1;
        } else {
            recnum = 0;
        }

        if (setFirstRec) {
            setFirstRec = 0;
            recnum = 0;
        }

        for (; recnum < hfte->hfheader.RecPage; recnum++) {
            byte = recnum / 8;
            bit = recnum % 8;

            map = pagebuf[recSize * hfte->hfheader.RecPage + byte];

            if (((map >> bit) & 0x01) == 1) {
                if (memcpy(record, pagebuf + recSize * recnum, recSize) == NULL) {
                    PF_UnpinPage(hfte->pfd, pagenum, 0);
                    return recid;
                }

                if (PF_UnpinPage(hfte->pfd, pagenum, 0) == PFE_OK) {
                    recid.pagenum = pagenum;
                    recid.recnum = recnum;
                }

                return recid;
            }
        }

        if (PF_UnpinPage(hfte->pfd, pagenum, 0) != PFE_OK) {
            return recid;
        }
    }
}

/* Get the record at given position.
    - HFfd: fd of HF layer.
    - recId: record position to read.
    - record: pointer where read content will be written

    return value: status code.
*/
int HF_GetThisRec(int HFfd, RECID recId, char *record) {
    HFftab_ele *hfte = &(hft[HFfd]);
    int recSize = hfte->hfheader.RecSize;
    char *pagebuf;
    int byte, bit;
    char map;

    if (HF_ValidRecId(HFfd, recId) != TRUE) {
        return HFE_INVALIDRECORD;
    }

    if (PF_GetThisPage(hfte->pfd, recId.pagenum, &pagebuf) != PFE_OK) {
        return HFE_PF;
    }

    byte = recId.recnum / 8;
    bit = recId.recnum % 8;

    map = pagebuf[recSize * hfte->hfheader.RecPage + byte];
    if (((map >> bit) & 0x01) == 1) {
        if (memcpy(record, pagebuf + recSize * recId.recnum, recSize) == NULL) {
            PF_UnpinPage(hfte->pfd, recId.pagenum, 0);
            return HFE_PF;
        }

        return PF_UnpinPage(hfte->pfd, recId.pagenum, 0) == PFE_OK ? HFE_OK : HFE_PF;
    } else {
        PF_UnpinPage(hfte->pfd, recId.pagenum, 0);
        return HFE_EOF;
    }
}

/* Open new file scan.
    - HFfd: fd of HF layer.
    - attrType: type of attribute to search.
    - attrLength: lenth of attribute to search.
    - attrOffset: offset of attribute to search.
    - op: operation code like eq, le, so on.
    - value: pointer to the value which will be compared to.

    return value: scan descriptor.
*/
int HF_OpenFileScan(int HFfd, char attrType, int attrLength, int attrOffset, int op, char *value) {
    int hsd;

    for (hsd = 0; hsd < MAXSCANS; hsd++) {
        if (hst[hsd].valid == FALSE) {
            hst[hsd].valid = TRUE;
            hst[hsd].hfd = HFfd;
            hst[hsd].attrType = attrType;
            hst[hsd].attrLength = attrLength;
            hst[hsd].attrOffset = attrOffset;
            hst[hsd].op = op;
            hst[hsd].value = value;
            hst[hsd].current.pagenum = -1;
            hst[hsd].current.recnum = 0;
            return hsd;
        }
    }

    return HFE_STABFULL;
}

/* Get next record satisfies comparision expression.
    - HFsd: scan descriptor.
    - record: pointer where read content will be written.

    return value: record position of which is found.
*/
RECID HF_FindNextRec(int HFsd, char *record) {
    RECID recid = hst[HFsd].current;
    int op = hst[HFsd].op;
    char *value = hst[HFsd].value;
    int match = 0;
    RECID rec_err;

    rec_err.pagenum = -1;
    rec_err.recnum = HFE_INTERNAL;

    if (recid.pagenum < 0) {
        recid.pagenum = 0;
        setFirstRec = 1;
    }

    while (!match) {
        recid = HF_GetNextRec(hst[HFsd].hfd, recid, record);

        if (recid.pagenum == -1) return recid;

        if (value == NULL) match = 1;
        else if (hst[HFsd].attrType == STRING_TYPE) {
            int result = strncmp(record + hst[HFsd].attrOffset, value, hst[HFsd].attrLength);

            if (op == 1) match = result == 0;
            else if (op == 2) match = result < 0;
            else if (op == 3) match = result > 0;
            else if (op == 4) match = result <= 0;
            else if (op == 5) match = result >= 0;
            else if (op == 6) match = result != 0;
            else return rec_err;
        } else if (hst[HFsd].attrType == INT_TYPE) {
            int src, dst;
            if (memcpy(&src, record + hst[HFsd].attrOffset, hst[HFsd].attrLength) == NULL || memcpy(&dst, value, hst[HFsd].attrLength) == NULL) {
                return rec_err;
            }

            if (op == 1) match = src == dst;
            else if (op == 2) match = src < dst;
            else if (op == 3) match = src > dst;
            else if (op == 4) match = src <= dst;
            else if (op == 5) match = src >= dst;
            else if (op == 6) match = src != dst;
            else return rec_err;
        } else if (hst[HFsd].attrType == REAL_TYPE) {
            float src, dst;
            if (memcpy(&src, record + hst[HFsd].attrOffset, hst[HFsd].attrLength) == NULL || memcpy(&dst, value, hst[HFsd].attrLength) == NULL) {
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

    hst[HFsd].current = recid;
    return recid;
}

/* Close an open file scan.
    - HFsd: sd of HF layer.

    return value: status code.
*/
int HF_CloseFileScan(int HFsd) {
    hst[HFsd].valid = FALSE;
    return HFE_OK;
}

void HF_PrintError(char *errString) {
    printf("HF_PrintError: %s\n", errString);
}

/* Test where record position is valid.
    - HFfd: fd of HF layer.
    - recid: record position to test.

    return value: TRUE if valid.
*/
bool_t HF_ValidRecId(int HFfd, RECID recid) {
    HFHeader h = hft[HFfd].hfheader;

    return recid.pagenum >= 0 && recid.recnum >= 0 && recid.pagenum < h.NumPg && recid.recnum < h.RecPage ? TRUE : FALSE;
}
