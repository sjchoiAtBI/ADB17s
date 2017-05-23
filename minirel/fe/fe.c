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

/* File descriptor and scan descriptors of relcat and attrcat. */
int rfd, afd;
char *db;

void _dbcreate_unix(void *pointer, int fd) {
    if (pointer) {
        free(pointer);
    }

    if (fd > 0) {
        HF_CloseFile(fd);
    }

    DBdestroy(dbname);
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
    char rel_name[5] = ["relname", "relwid", "attrcnt", "indexcnt", "primattr"];
    int rel_offset[5] = [offsetof(RELDESCTYPE, relname),
    offsetof(RELDESCTYPE, relwid),
    offsetof(RELDESCTYPE, attrcnt),
    offsetof(RELDESCTYPE, indexcnt),
    offsetof(RELDESCTYPE, primattr)];
    int rel_len[5] = [sizeof(char) * MAXNAME, sizeof(int), sizeof(int), sizeof(int), sizeof(char) * MAXNAME];
    int rel_type[5] = [STRING_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, STRING_TYPE];

    char attr_name[7] = ["relname", "attrname", "offset", "attrlen", "attrtype", "indexed", "attrno"];
    int attr_offset[7] = [offsetof(ATTRDESCTYPE, relname),
    offsetof(ATTRDESCTYPE, attrname),
    offsetof(ATTRDESCTYPE, offset),
    offsetof(ATTRDESCTYPE, attrlen),
    offsetof(ATTRDESCTYPE, attrtype),
    offsetof(ATTRDESCTYPE, indexed),
    offsetof(ATTRDESCTYPE, attrno)];
    int attr_len[7] = [sizeof(char) * MAXNAME, sizeof(char) * MAXNAME, sizeof(int), sizeof(int), sizeof(int), sizeof(bool_t), sizeof(int)];
    int attr_type[7] = [STRING_TYPE, STRING_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, INT_TYPE];

    /* Create directory. */
    if (mkdir (dbname, S_IRWXU) != 0) {
        FEerrno = FEE_UNIX;
        return;
    };


    /* Create relcat. */
    length = strlen(dbname) + strlen(RELCATNAME);
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, RELCATNAME);

    if (HF_CreateFile(filename, RELDESCSIZE) != HFE_OK) return _dbcreate_unix(filename, -1);

    if ((fd = HF_OpenFile(filename)) < 0) return _dbcreate_unix(filename, -1);
    free(filename);

    sprintf(rel.relname, "relcat");
    rel.relwid = RELDESCSIZE;
    rel.attrcnt = 5;
    rel.indexcnt = 0;

    recId = HF_InsertRec(fd, &rel);
    if (!HF_ValidRecId(fd, recId)) return _dbcreate_unix(NULL, fd);

    sprintf(rel.relname, "attrcat");
    rel.relwid = ATTRDESCSIZE;
    rel.attrcnt = 7;
    rel.indexcnt = 0;

    recId = HF_InsertRec(fd, &rel);
    if (!HF_ValidRecId(fd, recId)) return _dbcreate_unix(NULL, fd);

    if (HF_CloseFile(fd) != HFE_OK) _dbcreate_unix(NULL, -1);

    /* Create attrcat. */
    length = strlen(dbname) + strlen(ATTRCATNAME);
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, ATTRCATNAME);

    if (HF_CreateFile(filename, ATTRDESCSIZE) != HFE_OK) return _dbcreate_unix(filename, -1);

    if ((fd = HF_OpenFile(filename)) < 0) return _dbcreate_unix(filename, -1);
    free(filename);

    sprintf(attr.relname, "relcat");
    attr.indexed = FALSE;

    for (i = 0; i < 5; i++) {
        sprintf(attr.attrname, rel_name[i]);
        attr.offset = rel_offset[i];
        attr.attrlen = rel_len[i];
        attr.attrtype = rel_type[i];
        attr.attrno = i;

        if (!HF_ValidRecId(fd, HF_InsertRec(fd, &attr))) return _dbcreate_unix(NULL, fd);
    }

    sprintf(attr.relname, "attrcat");
    attr.indexed = FALSE;

    for (i = 0; i < 5; i++) {
        sprintf(attr.attrname, attr_name[i]);
        attr.offset = attr_offset[i];
        attr.attrlen = attr_len[i];
        attr.attrtype = attr_type[i];
        attr.attrno = i;

        if (!HF_ValidRecId(fd, HF_InsertRec(fd, &attr))) return _dbcreate_unix(NULL, fd);
    }

    if (HF_CloseFile(fd) != HFE_OK) _dbcreate_unix(NULL, -1);
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

    FEerrno = HFE_HF;
}

void DBconnect(char *dbname) {
    char *filename;
    int length;

    /* Save dbname. */
    db = (char *) malloc (sizeof(char) * strlen(dbname));
    sprintf(db, "%s", dbname);

    /* Open relcat. */
    length = strlen(dbname) + strlen(RELCATNAME);
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, RELCATNAME);

    if ((rfd = HF_OpenFile(filename)) < 0) return _connect_hf(filename, -1);
    free(filename);

    /* Open attrcat. */
    length = strlen(dbname) + strlen(ATTRCATNAME);
    filename = (char *) malloc (sizeof(char) * length);
    sprintf(filename, "%s/%s", dbname, ATTRCATNAME);

    if ((afd = HF_OpenFile(filename)) < 0) return _connect_hf(filename, rfd);
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

    filename = (char *) malloc (sizeof(char) * (strlen(db) + 1 + strlen(relName));
    sprintf(filename, "%s/%s", db, relName);
    if (HF_CreateFile(filename, len) != HFE_OK) {
        free(filename);
        return FEE_HF;
    }
    free(filename);

    sprintf(rel.relname, relName);
    rel.relwid = len;
    rel.attrcnt = numAttrs;
    rel.indexcnt = 0;

    recId[0] = HF_InsertRec(rfd, &rel);
    if (!HF_ValidRecId(rfd, recId[0])) {
        HF_DestroyFile(relName);
        return FEE_HF;
    }

    sprintf(attr.relname, "attrcat");
    attr.indexed = FALSE;

    len = 0;
    attrErr = -1;
    for (i = 0; i < numAttrs; i++) {
        sprintf(attr.attrname, attrs[i].attrName);
        attr.offset = len;
        attr.attrlen = attrs[i].attrLen;
        attr.attrType = attrs[i].attrType;
        attr.attrno = i;

        recId[i + 1] = HF_InsertRec(afd, &attr);
        if (!HF_ValidRecId(afd, recId[i + 1])) {
            attrErr = i;
            break;
        }

        len += attrs[i].attrLen;
    }

    if (attrErr > -1) {
        HF_DestroyFile(relName);
        HF_DeleteRec(rfd, recId);

        for (i = 0; i < attrErr; i++) {
            HF_DeleteRec(afd, recId[i + 1]);
        }

        return FEE_HF;
    }

    return FEE_OK;
}

int  DestroyTable(char *relName) {
    int sd;
    RECID recId;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;

    if (HF_DestroyFile(relName) != HFE_OK) {
        return FEE_HF;
    }

    /* Delete relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, &rel);
    if (!HF_ValidRecId(rfd, recId) || HF_DeleteRec(rfd, recId) != HFE_OK) {
        HF_CloseFileScan(sd);
        return FEE_HF;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    /* Delete attrcat. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, &attr);
    while (HF_ValidRecId(afd, recId)) {
        if (HF_DeleteRec(afd, recId) != HFE_OK) {
            HF_CloseFileScan(sd);
            return FEE_HF;
        }

        recId = HF_FindNextRec(sd, &attr);
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    return FEE_OK;
}

int  BuildIndex(char *relName, char *attrName) {
    char *filename;
    int fd, sd, found;
    RELDESCTYPE rel;
    ATTRDESCTYPE attr;

    /* Update attrcat. */
    if ((sd = HF_OpenFileScan(afd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    found = 0;
    recId = HF_FindNextRec(sd, &attr);
    while (HF_ValidRecId(afd, recId)) {
        if (strcmp(attr.attrname, attrName) == 0) {
            if (attr.indexed == TRUE) {
                HF_CloseFileScan(sd);
                return FEE_ALREADYINDEXED;
            }

            attr.indexed = TRUE;

            if (HF_DeleteRec(afd, recId) != HFE_OK || HF_InsertRec(afd, &attr) != HFE_OK) {
                HF_CloseFileScan(sd);
                return FEE_HF;
            }

            found = 1;
            break;
        }

        recId = HF_FindNextRec(sd, &attr);
    }

    if (!found) {
        HF_CloseFileScan(sd);
        return FEE_NOSUCHATTR;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    /* Update relcat. */
    if ((sd = HF_OpenFileScan(rfd, STRING_TYPE, MAXNAME, 0, EQ_OP, relName)) < 0) {
        return FEE_HF;
    }

    recId = HF_FindNextRec(sd, &rel);
    if (!HF_ValidRecId(rfd, recId)) {
        HF_CloseFileScan(sd);
        return FEE_HF;
    }

    rel.indexcnt++;
    if (HF_DeleteRec(rfd, recId) != HFE_OK || HF_InsertRec(rfd, &rel) != HFE_OK) {
        HF_CloseFileScan(sd);
        return FEE_HF;
    }

    if (HF_CloseFileScan(sd) != HFE_OK) {
        return FEE_HF;
    }

    /* Build am index. */
    filename = (char *) malloc (sizeof(char) * (strlen(db) + 1 + strlen(relName));
    sprintf(filename, "%s/%s", db, relName);
    if (HF_CreateFile(filename, len) != HFE_OK) {
        free(filename);
        return FEE_HF;
    }

    if ((fd = HF_OpenFile(filename)) < 0) {
        free(filename);
        return FEE_HF;
    }

    if ((sd = HF_OpenFileScan(fd, attr.attrtype, attr.attrlen, attr.offset, EQ_OP, NULL)) < 0) {
        HF_CloseFile(fd);
        return FEE_HF;
    }

    if (AM_CreateIndex(filename, rel.indexcnt - 1, (char) attr.attrtype, attr.attrlen, FALSE) != AME_OK) {
        free(filename);
        return FEE_AM;
    }



    free(filename);
}

int  DropIndex(char *relname, char *attrName) {

}

int  LoadTable(char *relName, char *fileName) {

}

int  HelpTable(char *relName) {

}

int  PrintTable(char *relName) {

}

int  Select(char *srcRelName, char *selAttr, int op, int valType, int valLength, void *value, int numProjAttrs,	char *projAttrs[], char *resRelName) {

}

int  Join(REL_ATTR *joinAttr1, int op, REL_ATTR *joinAttr2, int numProjAttrs, REL_ATTR projAttrs[], char *resRelName) {

}

int  Insert(char *relName, int numAttrs, ATTR_VAL values[]) {

}

int  Delete(char *relName, char *selAttr, int op, int valType, int valLength, void *value) {

}

void FE_PrintError(char *errmsg) {

}

void FE_Init(void) {
    AM_Init();
}
