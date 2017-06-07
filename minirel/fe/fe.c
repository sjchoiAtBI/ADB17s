#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "minirel.h"
#include "hf.h"
#include "am.h"
#include "fe.h"
#include "custom.h"
#include "catalog.h"

int FEerrno;

bool_t initialized = FALSE;

/* File descriptor and scan descriptors of relcat and attrcat. */
int rfd, afd;
char *db;

void _dbcreate_unix(void *pointer, int fd) {
    printf("_dbcreate_unix\n");

    if (pointer) {
        free(pointer);
    }

    if (fd > 0) {
        HF_CloseFile(fd);
    }

    /*DBdestroy(dbname);*/
    FEerrno = FEE_UNIX;
}

void DBcreate(char *dbname) {
    size_t length;
    char *filename;
    int fd, i;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId;

    /* Type: 0 for char, 1 for int, 2 for float. */
    char *rel_name[5] = {"relname", "relwid", "attrcnt", "indexcnt", "primattr"};
    int rel_offset[5] = {offsetof(RELDESCTYPE, relname),
    offsetof(RELDESCTYPE, relwid),
    offsetof(RELDESCTYPE, attrcnt),
    offsetof(RELDESCTYPE, indexcnt),
    offsetof(RELDESCTYPE, primattr)};
    int rel_len[5] = {sizeof(char) * MAXNAME, sizeof(int), sizeof(int), sizeof(int), sizeof(char) * MAXNAME};
    int rel_type[5] = {STRING_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, STRING_TYPE};

    char *attr_name[7] = {"relname", "attrname", "offset", "attrlen", "attrtype", "indexed", "attrno"};
    int attr_offset[7] = {offsetof(ATTRDESCTYPE, relname),
    offsetof(ATTRDESCTYPE, attrname),
    offsetof(ATTRDESCTYPE, offset),
    offsetof(ATTRDESCTYPE, attrlen),
    offsetof(ATTRDESCTYPE, attrtype),
    offsetof(ATTRDESCTYPE, indexed),
    offsetof(ATTRDESCTYPE, attrno)};
    int attr_len[7] = {sizeof(char) * MAXNAME, sizeof(char) * MAXNAME, sizeof(int), sizeof(int), sizeof(int), sizeof(bool_t), sizeof(int)};
    int attr_type[7] = {STRING_TYPE, STRING_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, INT_TYPE};

    if (!initialized) FE_Init();

    /* Create directory. */
    if (mkdir (dbname, S_IRWXU) != 0) {
        FEerrno = FEE_UNIX;
        printf("FEerrno = %d\n", FEerrno);
        return;
    };

    /* Create relcat. */
    length = strlen(dbname) + strlen(RELCATNAME) + 2;
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, RELCATNAME);

    if (HF_CreateFile(filename, RELDESCSIZE) != HFE_OK) { _dbcreate_unix(filename, -1); return; }

    if ((fd = HF_OpenFile(filename)) < 0) { _dbcreate_unix(filename, -1); return; }
    free(filename);

    sprintf(rel.relname, RELCATNAME);
    rel.relwid = RELDESCSIZE;
    rel.attrcnt = 5;
    rel.indexcnt = 0;

    recId = HF_InsertRec(fd, (char *) &rel);
    if (!HF_ValidRecId(fd, recId)) { _dbcreate_unix(NULL, fd); return; }

    sprintf(rel.relname, ATTRCATNAME);
    rel.relwid = ATTRDESCSIZE;
    rel.attrcnt = 7;
    rel.indexcnt = 0;

    recId = HF_InsertRec(fd, (char *) &rel);
    if (!HF_ValidRecId(fd, recId)) { _dbcreate_unix(NULL, fd); return; }

    if (HF_CloseFile(fd) != HFE_OK) { _dbcreate_unix(NULL, -1); return; }

    /* Create attrcat. */
    length = strlen(dbname) + strlen(ATTRCATNAME) + 2;
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, ATTRCATNAME);

    if (HF_CreateFile(filename, ATTRDESCSIZE) != HFE_OK) { _dbcreate_unix(filename, -1); return; }

    if ((fd = HF_OpenFile(filename)) < 0) { _dbcreate_unix(filename, -1); return; }
    free(filename);

    sprintf(attr.relname, RELCATNAME);
    attr.indexed = FALSE;

    for (i = 0; i < 5; i++) {
        sprintf(attr.attrname, "%s", rel_name[i]);
        attr.offset = rel_offset[i];
        attr.attrlen = rel_len[i];
        attr.attrtype = rel_type[i];
        attr.attrno = i;

        if (!HF_ValidRecId(fd, HF_InsertRec(fd, (char *) &attr))) { _dbcreate_unix(NULL, fd); return; }
    }

    sprintf(attr.relname, ATTRCATNAME);
    attr.indexed = FALSE;

    for (i = 0; i < 7; i++) {
        sprintf(attr.attrname, "%s", attr_name[i]);
        attr.offset = attr_offset[i];
        attr.attrlen = attr_len[i];
        attr.attrtype = attr_type[i];
        attr.attrno = i;

        if (!HF_ValidRecId(fd, HF_InsertRec(fd, (char *) &attr))) { _dbcreate_unix(NULL, fd); return; }
    }

    if (HF_CloseFile(fd) != HFE_OK) { _dbcreate_unix(NULL, -1); return; }
}

void DBdestroy(char *dbname) {
    if (rmdir(dbname)) {
        FEerrno = FEE_UNIX;
    }
}

void _connect_hf(void *pointer, int fd) {
    if (pointer) {
        free(pointer);
    }

    if (fd > 0) {
        HF_CloseFile(fd);
    }

    FEerrno = FEE_HF;
}

void DBconnect(char *dbname) {
    char *filename;
    int length;

    /* Save dbname. */
    db = (char *) malloc (sizeof(char) * strlen(dbname) + 1);
    /*strcpy(db, dbname);*/
    sprintf(db, "%s", dbname);

    /* Open relcat. */
    length = strlen(dbname) + strlen(RELCATNAME) + 2;
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, RELCATNAME);

    if ((rfd = HF_OpenFile(filename)) < 0) { _connect_hf(filename, -1); return; }
    free(filename);

    /* Open attrcat. */
    length = strlen(dbname) + strlen(ATTRCATNAME) + 2;
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, ATTRCATNAME);

    if ((afd = HF_OpenFile(filename)) < 0) { _connect_hf(filename, rfd); return; }
    free(filename);

}

void DBclose(char *dbname) {
    free(db);

    if (HF_CloseFile(rfd) != HFE_OK || HF_CloseFile(afd) != HFE_OK) {
        FEerrno = FEE_HF;
    }
}

int  CreateTable(char *relName, int numAttrs, ATTR_DESCR attrs[], char *primAttrName) {
    char *filename;
    int i, j, len, attrErr;
    RECID recId[numAttrs + 1];
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;

    len = 0;
    for (i = 0; i < numAttrs; i++) {
        if (strlen(attrs[i].attrName) > MAXNAME) {
            return FEE_ATTRNAMETOOLONG;
        }

        len += attrs[i].attrLen;

        for (j = i + 1; j < numAttrs; j++) {
            if (strcmp(attrs[i].attrName, attrs[j].attrName) == 0) {
                return FEE_DUPLATTR;
            }
        }
    }

    filename = (char *) malloc (sizeof(char) * (strlen(db) + 1 + strlen(relName)));
    sprintf(filename, "%s/%s", db, relName);
    if (HF_CreateFile(filename, len) != HFE_OK) {
        free(filename);
        return FEE_HF;
    }
    free(filename);

    sprintf(rel.relname, "%s", relName);
    rel.relwid = len;
    rel.attrcnt = numAttrs;
    rel.indexcnt = 0;

    recId[0] = HF_InsertRec(rfd, (char *) &rel);
    if (!HF_ValidRecId(rfd, recId[0])) {
        HF_DestroyFile(relName);
        return FEE_HF;
    }

    sprintf(attr.relname, "%s", relName);
    attr.indexed = FALSE;

    len = 0;
    attrErr = -1;
    for (i = 0; i < numAttrs; i++) {
        sprintf(attr.attrname, "%s", attrs[i].attrName);
        attr.offset = len;
        attr.attrlen = attrs[i].attrLen;
        attr.attrtype = attrs[i].attrType;
        attr.attrno = i;

        recId[i + 1] = HF_InsertRec(afd, (char *) &attr);
        if (!HF_ValidRecId(afd, recId[i + 1])) {
            attrErr = i;
            break;
        }

        len += attrs[i].attrLen;
    }

    if (attrErr > -1) {
        HF_DestroyFile(relName);
        HF_DeleteRec(rfd, recId[0]);

        for (i = 0; i < attrErr; i++) {
            HF_DeleteRec(afd, recId[i + 1]);
        }

        return FEE_HF;
    }

    return FEE_OK;
}

int  DestroyTable(char *relName) {
    char *filename;
    int sd;
    RECID recId;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;


    filename = (char *) malloc (sizeof(char) * (strlen(db) + strlen(relName) + 2));
    sprintf(filename, "%s/%s", db, relName);

    if (HF_DestroyFile(filename) != HFE_OK) { printf("hey\n");
        return FEE_HF;
    }

    /* Delete relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, (char *) &rel);
    if (!HF_ValidRecId(rfd, recId) || HF_DeleteRec(rfd, recId) != HFE_OK) {
        HF_CloseFileScan(sd); printf("yo 1\n"); exit (-1);
        return FEE_HF;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    /* Delete attrcat. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, (char *) &attr);
    while (HF_ValidRecId(afd, recId)) {
        if (HF_DeleteRec(afd, recId) != HFE_OK) {
            HF_CloseFileScan(sd); printf("yo 2\n"); exit (-1);
            return FEE_HF;
        }

        recId = HF_FindNextRec(sd, (char *) &attr);
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    return FEE_OK;
}

int HF_ReplaceRec (int fd, RECID recId, char *record, RECID *newRecId) {
    RECID temp;

    temp = HF_InsertRec(fd, record);
    if (!HF_ValidRecId(fd, temp)) {
        return HFE_INTERNAL;
    }

    if (HF_DeleteRec(fd, recId) != HFE_OK) {
        HF_DeleteRec(fd, temp);
        return HFE_INTERNAL;
    }

    if (newRecId) {
        *newRecId = temp;
    }

    return HFE_OK;
}

int  BuildIndex(char *relName, char *attrName) {
    char *filename, *record, *value;
    int fd, sd, found, attrIndex, ifd;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId, relRecId, attrRecId;

    /* Update attrcat. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        printf("here\n"); exit(-1);
        return FEE_HF;
    }

    found = 0;
    attrIndex = 0;
    recId = HF_FindNextRec(sd, (char *) &attr);
    while (HF_ValidRecId(afd, recId)) {
        if (strcmp(attr.attrname, attrName) == 0) {
            if (attr.indexed == TRUE) {
                HF_CloseFileScan(sd);printf("here2\n"); exit(-1);
                return FEE_ALREADYINDEXED;
            }

            attr.indexed = TRUE;

            if (HF_ReplaceRec(afd, recId, (char *) &attr, &attrRecId) != HFE_OK) {
                HF_CloseFileScan(sd);printf("here3\n"); exit(-1);
                return FEE_HF;
            }

            found = 1;
            break;
        }

        recId = HF_FindNextRec(sd, (char *) &attr);
        attrIndex++;
    }

    if (!found) {
        HF_CloseFileScan(sd);printf("here4\n"); exit(-1);
        return FEE_NOSUCHATTR;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        attr.indexed = FALSE;printf("here5\n"); exit(-1);
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        return FEE_HF;
    }

    /* Update relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        attr.indexed = FALSE;printf("here6\n"); exit(-1);
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, (char *) &rel);
    if (!HF_ValidRecId(rfd, recId)) {printf("here7\n"); exit(-1);
        /*attr.indexed = FALSE;
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        HF_CloseFileScan(sd);*/
        return FEE_HF;
    }

    rel.indexcnt++;
    if (HF_ReplaceRec(rfd, recId, (char *) &rel, &relRecId) != HFE_OK) {
        /*attr.indexed = FALSE;printf("here8\n"); exit(-1);
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        HF_CloseFileScan(sd);*/
        return FEE_HF;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {printf("here9\n"); exit(-1);
        /*attr.indexed = FALSE;
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        rel.indexcnt--;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);*/
        return FEE_HF;
    }

    /* Build am index. */
    filename = (char *) malloc (sizeof(char) * (strlen(db) + strlen(relName) + 2));
    sprintf(filename, "%s/%s", db, relName);

    if ((fd = HF_OpenFile(filename)) < 0) {
        attr.indexed = FALSE;printf("here11\n"); exit(-1);
        /*HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        rel.indexcnt--;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        free(filename);*/
        return FEE_HF;
    }

    if ((sd = HF_OpenFileScan(fd, attr.attrtype, attr.attrlen, attr.offset, EQ_OP, NULL)) < 0) {
        attr.indexed = FALSE;printf("here12\n"); exit(-1);
        /*HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        rel.indexcnt--;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        HF_CloseFile(fd);
        free(filename);*/
        return FEE_HF;
    }

    if (AM_CreateIndex(filename, attr.attrno, (char) attr.attrtype, attr.attrlen, FALSE) != AME_OK) {
        attr.indexed = FALSE;printf("here13\n"); exit(-1);
        /*HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        rel.indexcnt--;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        HF_CloseFileScan(sd);
        HF_CloseFile(fd);
        free(filename);*/
        return FEE_AM;
    }

    if ((ifd = AM_OpenIndex(filename, attr.attrno)) < 0) {printf("here15\n"); exit(-1);
        /*attr.indexed = FALSE;
        HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
        rel.indexcnt--;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        AM_DestroyIndex(filename, attrIndex);
        HF_CloseFileScan(sd);
        HF_CloseFile(fd);
        free(filename);*/
        return FEE_AM;
    }

    record = (char *) malloc(sizeof(char) * rel.relwid);
    value = (char *) malloc (sizeof(char) * attr.attrlen);

    recId = HF_FindNextRec(sd, record);
    while (HF_ValidRecId(fd, recId)) {
        if (memcpy (value, record + attr.offset, attr.attrlen) == NULL) return FEE_UNIX;

        if (AM_InsertEntry(ifd, value, recId) != AME_OK) {
            attr.indexed = FALSE; printf("here16\n"); exit(-1);
            /*HF_ReplaceRec(afd, attrRecId, (char *) &attr, NULL);
            rel.indexcnt--;
            HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
            AM_CloseIndex(ifd);
            AM_DestroyIndex(filename, attrIndex);
            HF_CloseFileScan(sd);
            HF_CloseFile(fd);
            free(filename);*/
            return FEE_AM;
        }

        recId = HF_FindNextRec(sd, record);
    }

    free(filename);
    free(record);
    free(value);

    if (AM_CloseIndex(ifd) != AME_OK || HF_CloseFileScan(sd) != HFE_OK || HF_CloseFile(fd) != HFE_OK) {printf("here17\n"); exit(-1);
        return FEE_AM;
    }

    return FEE_OK;
}

void recoverIndex(char *relname) {
    char *filename;
    int i, fd, sd;
    ATTRDESCTYPE attr;
    RECID recId;

    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relname)) < 0) {
        return ;
    }

    filename = (char *) malloc (sizeof(char) * (strlen(db) + 1 + strlen(relname)));
    sprintf(filename, "%s/%s", db, relname);

    recId = HF_FindNextRec(sd, (char *) &attr);
    while (HF_ValidRecId(afd, recId)) {
        if ((fd = AM_OpenIndex(filename, i)) >= 0) {
            attr.indexed = TRUE;
            HF_ReplaceRec(afd, recId, (char *) &attr, NULL);
            AM_CloseIndex(fd);
        }

        recId = HF_FindNextRec(sd, (char *) &attr);
    }

    free(filename);
    HF_CloseFileScan(sd);
}

int  DropIndex(char *relname, char *attrName) {
    char *filename, record;
    int fd, sd, i, attrIndex, ifd, prevIndexcnt;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId, relRecId, attrRecId;

    /* Update attrcat. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relname)) < 0) {
        return FEE_HF;printf("hello\n"); exit(-1);
    }

    attrIndex = 0;
    recId = HF_FindNextRec(sd, (char *) &attr);
    while (HF_ValidRecId(afd, recId)) {
        if (attrName == NULL) {
            if (attr.indexed == TRUE) {
                attr.indexed = FALSE;
                if (HF_ReplaceRec(afd, recId, (char *) &attr, &attrRecId) != HFE_OK) {
                    HF_CloseFileScan(sd);printf("hello1\n"); exit(-1);
                    recoverIndex(relname);
                    return FEE_NOTINDEXED;
                }
            }
        } else if (strcmp(attr.attrname, attrName) == 0) {
            if (attr.indexed == FALSE) {
                HF_CloseFileScan(sd);printf("hello2\n"); exit(-1);
                return FEE_NOTINDEXED;
            }

            attr.indexed = FALSE;

            if (HF_ReplaceRec(afd, recId, (char *) &attr, &attrRecId) != HFE_OK) {
                HF_CloseFileScan(sd);printf("hello3\n"); exit(-1);
                return FEE_HF;
            }

            break;
        }

        recId = HF_FindNextRec(sd, (char *) &attr);
        attrIndex++;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        recoverIndex(relname);printf("hello4\n"); exit(-1);
        return FEE_HF;
    }

    /* Update relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relname)) < 0) {
        recoverIndex(relname);printf("hello5\n"); exit(-1);
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, (char *) &rel);
    if (!HF_ValidRecId(rfd, recId)) {printf("hello6\n"); exit(-1);
        recoverIndex(relname);
        HF_CloseFileScan(sd);
        return FEE_HF;
    }

    prevIndexcnt = rel.indexcnt;
    if (attrName) {
        rel.indexcnt--;
    } else {
        rel.indexcnt = 0;
    }

    if (HF_ReplaceRec(rfd, recId, (char *) &rel, &relRecId) != HFE_OK) {
        recoverIndex(relname);
        HF_CloseFileScan(sd);printf("hello7\n"); exit(-1);
        return FEE_HF;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        recoverIndex(relname);printf("hello8\n"); exit(-1);
        rel.indexcnt = prevIndexcnt;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        return FEE_HF;
    }

    /* Build am index. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relname)) < 0) {
        recoverIndex(relname);printf("hello9\n"); exit(-1);
        rel.indexcnt = prevIndexcnt;
        HF_ReplaceRec(rfd, relRecId, (char *) &rel, NULL);
        return FEE_HF;
    }

    filename = (char *) malloc (sizeof(char) * (strlen(db) + strlen(relname) + 2));
    sprintf(filename, "%s/%s", db, relname);

    recId = HF_FindNextRec(sd, (char *) &attr);
    while (HF_ValidRecId(afd, recId)) {

        if (attrName == NULL) {
            /*printf("deleting index %s, %d\n", filename, attr.attrno);*/
            if (AM_DestroyIndex (filename, attr.attrno) != AME_OK) {printf("hello10\n"); exit(-1);
                return FEE_AM;
            }
        } else if (strcmp(attr.attrname, attrName) == 0) {
            /*printf("deleting index %s, %d\n", filename, attr.attrno);*/
            if (AM_DestroyIndex (filename, attr.attrno) != AME_OK) {printf("hello11\n"); exit(-1);
                return FEE_AM;
            }
            break;
        }

        recId = HF_FindNextRec(sd, (char *) &attr);
    }

    free(filename);
    if (HF_CloseFileScan(sd) != HFE_OK) {printf("hello12\n"); exit(-1);
        return FEE_HF;
    }

    return FEE_OK;
}

int findRel (char *relName, char *record) {
    int sd;
    RELDESCTYPE rel;
    RECID recId;

    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

    recId = HF_FindNextRec(sd, record);
    if (!HF_ValidRecId(rfd, recId)) return FEE_HF;

    if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;

    return FEE_OK;
}

int  LoadTable(char *relName, char *fileName) {
    size_t length;
    char *filename, *record, *value;
    int fd, sd, id;
    FILE *fp = fopen(fileName, "r");
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId;

    /* check load file. */
    if (fp == NULL) return FEE_UNIX;

    /* Find relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

    recId = HF_FindNextRec(sd, (char *) &rel);
    if (!HF_ValidRecId(rfd, recId)) return FEE_HF;

    if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;

    /* Open HF file. */
    length = strlen(db) + strlen(relName) + 2;
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", db, relName);
    if ((fd = HF_OpenFile(filename)) < 0) return FEE_HF;

    /* Check there is an index. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

    recId = HF_FindNextRec(sd, (char *) &attr);
    if (HF_ValidRecId(afd, recId) != TRUE) return FEE_HF;

    if (attr.indexed) {
        if ((id = AM_OpenIndex (filename, attr.attrno)) < 0) return FEE_AM;
        value = (char *) malloc (sizeof(char) * attr.attrlen);
    }

    /* Insert records. */
    record = (char *) malloc (sizeof(char) * rel.relwid);
    while (fread(record, rel.relwid, 1, fp) == 1) {
        /* Insert to HF file. */
        recId = HF_InsertRec(fd, record);
        if (!HF_ValidRecId(fd, recId)) return FEE_HF;

        /* Insert to AM file. */
        if (attr.indexed) {
            if (memcpy (value, record + attr.offset, attr.attrlen) == NULL) return FEE_UNIX;
            if (attr.indexed && AM_InsertEntry(id, value, recId) != AME_OK) return FEE_AM;
        }
    }

    if (HF_CloseFile(fd) != HFE_OK) return FEE_HF;

    if (attr.indexed && AM_CloseIndex(id) != AME_OK) return FEE_AM;

    if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;

    if (attr.indexed) free (value);
    free(record);
    free(filename);
    fclose(fp);

    return FEE_OK;
}

int  HelpTable(char *relName) {
    int sd, sda;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId;

    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

    recId = HF_FindNextRec(sd, (char *) &rel);
    while (HF_ValidRecId(rfd, recId)) {
        printf ("Relation\t%s\n", rel.relname);
        printf ("	Prim Attr:  relname	No of Attrs:  %d	Tuple width:  %d	No of Indices:  %d\nAttributes:\n+--------------+--------------+--------------+--------------+--------------+--------------+\n| attrname     | offset       | length       | type         | indexed      | attrno       |\n+--------------+--------------+--------------+--------------+--------------+--------------+\n", rel.attrcnt, rel.relwid, rel.indexcnt);

        if ((sda = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;
        recId = HF_FindNextRec(sda, (char *) &attr);
        while (HF_ValidRecId(afd, recId)) {
            printf ("| %s\t| %d\t| %d\t| %c\t| %s\t| %d\t|\n", attr.attrname, attr.offset, attr.attrlen, attr.attrtype, attr.indexed ? "yes" : "no", attr.attrno);
            recId = HF_FindNextRec(sda, (char *) &attr);
        }
        printf ("+--------------+--------------+--------------+--------------+--------------+--------------+\n");

        if (HF_CloseFileScan(sda) != HFE_OK) return FEE_HF;

        recId = HF_FindNextRec(sd, (char *) &rel);
    }

    if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;

    return FEE_OK;
}

int  PrintTable(char *relName) {
    char *filename, *record;
    int fd, sd;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;
    RECID recId;

    /* Find rel wid */
    if (findRel (relName, (char *) &rel) != FEE_OK) return FEE_HF;

    filename = (char *) malloc (sizeof(char) * (strlen(db) + strlen(relName) + 2));
    sprintf(filename, "%s/%s", db, relName);
    printf("file: %s\n", filename);

    record = (char *) malloc (sizeof(char) * rel.relwid);

    /* Print attr names. */
    printf ("Relation %s\n------------------------------------------------------------------------------------\n|", relName);
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

    while (HF_ValidRecId(afd, HF_FindNextRec(sd, (char *) &attr))) {
        printf (" %s\t|", attr.attrname);
    }

    if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;
    printf ("\n------------------------------------------------------------------------------------\n");


    /* Print attr values. */
    if (strcmp(relName, RELCATNAME) == 0) fd = rfd;
    else if (strcmp(relName, ATTRCATNAME) == 0) fd = afd;
    else if ((fd = HF_OpenFile(filename)) < 0) return FEE_HF;

    recId = HF_GetFirstRec(fd, record);
    while (HF_ValidRecId(fd, recId)) {
        printf ("|");

        if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) return FEE_HF;

        while (HF_ValidRecId(afd, HF_FindNextRec(sd, (char *) &attr))) {
            if (attr.attrtype == INT_TYPE) {
                printf(" %d\t|", *((int *) (record + attr.offset)));
            } else if (attr.attrtype == REAL_TYPE) {
                printf(" %f\t|", *((float *) (record + attr.offset)));
            } else if (attr.attrtype == STRING_TYPE) {
                printf(" %s\t|", record + attr.offset);
            }
        }

        if (HF_CloseFileScan(sd) != HFE_OK) return FEE_HF;
        printf ("\n");

        recId = HF_GetNextRec(fd, recId, record);
    }
     printf("------------------------------------------------------------------------------------\n");

    if (fd != rfd && fd != afd && HF_CloseFile(fd) != HFE_OK) return FEE_HF;

    /*if (record) free(record);
    if (filename) free(filename);*/

    return FEE_OK;
}

int  Select(char *srcRelName, char *selAttr, int op, int valType, int valLength, void *value, int numProjAttrs,	char *projAttrs[], char *resRelName) {
    return FEE_INTERNAL;
}

int  Join(REL_ATTR *joinAttr1, int op, REL_ATTR *joinAttr2, int numProjAttrs, REL_ATTR projAttrs[], char *resRelName) {
    return FEE_INTERNAL;
}

int  Insert(char *relName, int numAttrs, ATTR_VAL values[]) {
    return FEE_INTERNAL;
}

int  Delete(char *relName, char *selAttr, int op, int valType, int valLength, void *value) {
    return FEE_INTERNAL;
}

void FE_PrintError(char *errmsg) {
    fprintf(stderr, "%s\n", errmsg);
}

void FE_Init(void) {
    AM_Init();
    initialized = TRUE;
}
