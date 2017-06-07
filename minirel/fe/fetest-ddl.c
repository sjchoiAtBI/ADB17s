/*
 * fetest-ddl.c : tests Database utilities and DDL functions.
 *
 *	Bongki Moon (bkmoon@snu.ac.kr), May/07/2017
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "minirel.h"
#include "fe.h"
#include "am.h"
#include "hf.h"
#include "catalog.h"

#define TESTDB		"testdb"
#define	STUDREL		"student.sid"
#define	PROFREL		"prof.pid"

#include "studprofdb-schema.h"
#define STUD_LOADFILE	"./data.student.sid"
#define PROF_LOADFILE	"./data.professor.pid"

extern int relcatFd, attrcatFd;


/******************************************** */
/* Testing the PrintTable() and HelpTable()   */
/* by printing the relcat and attrcat         */
/* catalogs of TESTDB.                        */
/**********************************************/

void show_catalogs()
{
   /* HelpTable part */
   printf("HelpTable with relcat\n");
   HelpTable("relcat");
   printf("HelpTable with attrcat\n");
   HelpTable("attrcat");

   /* PrintTable part */
   printf("Print the relation catalog\n");
   PrintTable("relcat");
   printf("Print the attribute catalog\n");
   PrintTable("attrcat");
}

/***********************************/
/* testing CreateTable by creating */
/* relations                       */
/***********************************/

void make_attrDesc(ATTR_DESCR *attr, char *name, char type, int len)
{
  attr->attrName = (char *) malloc(MAXNAME);
  strcpy(attr->attrName, name);
  attr->attrType = type;
  attr->attrLen = len;
}

void create_student()
{
   ATTR_DESCR in_attrs[STUD_NUM_ATTRS];

   printf("Start create_student ...\n");

   /* specifying the attributes of the relation to be created */
   /* "sid", "sname", "gpa", "age", "advisor" attributes */
   make_attrDesc(&in_attrs[0],"sid",INT_TYPE,sizeof(int));
   make_attrDesc(&in_attrs[1],"sname",STRING_TYPE,MAXSTRLEN);
   make_attrDesc(&in_attrs[2],"gpa",REAL_TYPE,sizeof(float));
   make_attrDesc(&in_attrs[3],"age",INT_TYPE,sizeof(int));
   make_attrDesc(&in_attrs[4],"advisor",INT_TYPE,sizeof(int));

   if (CreateTable(STUDREL, STUD_NUM_ATTRS, in_attrs, NULL) != FEE_OK)
      FE_PrintError("Relation creation failed in create_student");

   printf("End create_student ...\n");
}

void create_professor()
{
   ATTR_DESCR in_attrs[PROF_NUM_ATTRS];

   printf("Start create_professor ...  \n");

   /* specifying the attributes of the relation to be created */
   /* "pid", "pname", "office" attributes */
   make_attrDesc(&in_attrs[0],"pid",INT_TYPE,sizeof(int));
   make_attrDesc(&in_attrs[1],"pname",STRING_TYPE,MAXSTRLEN);
   make_attrDesc(&in_attrs[2],"office",INT_TYPE,sizeof(int));

   if (CreateTable(PROFREL, PROF_NUM_ATTRS, in_attrs, NULL) != FEE_OK)
      FE_PrintError("Relation creation failed in create_professor");

   printf("End create_professor ...  \n");
}

/*******************************************************/
/* show_student:                                       */
/* Printing the student relation, and some info about  */
/* it by using PrintTable and HelpTable.               */
/*******************************************************/

void show_table(char *relname)
{
   printf("Start show_table(%s) ... \n",relname);

   if (HelpTable(relname) != FEE_OK)
      FE_PrintError("Error: while executing Help relation\n");

   if (PrintTable(relname) != FEE_OK)
      FE_PrintError("Error: while printing relation\n");

   printf("End show_table(%s) ... \n",relname);
}

/**************************************/
/* fetest6:                           */
/* Testing BuildIndex                 */
/* by creating indeces on student     */
/**************************************/

void index_student()
{
  printf("Start index_student ... \n");

  if (BuildIndex(STUDREL, "sname") != FEE_OK)
    FE_PrintError("Problem building the studname index\n");

  if (BuildIndex(STUDREL, "age") != FEE_OK)
    FE_PrintError("Problem building the age index\n");

  printf("End index_student ... \n");
}

void index_professor()
{
  printf("Start index_professor ... \n");

  if (BuildIndex(PROFREL, "pid") != FEE_OK)
    FE_PrintError("Problem building the sid index\n");

  printf("End index_professor ... \n");
}

/***************************************/
/* fetest7: testing LoadTable          */
/* Loading student with some tuples    */
/***************************************/

void load_student()
{
  printf("Start loading student from %s ...\n",STUD_LOADFILE);

  if (LoadTable(STUDREL, STUD_LOADFILE) != FEE_OK) {
     FE_PrintError("Problem loading the student data file.\n");
     printf("FEerrno==%d, AMerrno=%d, HFerrno=%d\n",FEerrno,AMerrno,HFerrno);exit(-1);
  }

  printf("End of loading student from %s ...\n",STUD_LOADFILE);
}

void load_professor()
{
  printf("Start loading professor from %s ...\n",PROF_LOADFILE);

  if (LoadTable(PROFREL, PROF_LOADFILE) != FEE_OK) {
     FE_PrintError("Problem loading the professor data file.\n");
     printf("FEerrno==%d, AMerrno=%d, HFerrno=%d\n",FEerrno,AMerrno,HFerrno);
  }

  printf("End of loading professor from %s ...\n",PROF_LOADFILE);
}

/**************************************/
/* dropindex_student:                  */
/* testing dropping indices           */
/**************************************/
void dropindex_student()
{
   printf("Start dropping student index ...\n");

   printf("... dropping student index on sname *\n");
   if (DropIndex(STUDREL, "sname") != FEE_OK)
     FE_PrintError("Problem dropping the index\n");

   printf("... dropping student index on age *\n");
   if (DropIndex(STUDREL, "age") != FEE_OK)
     FE_PrintError("Problem dropping the age index\n");

   printf("End of dropping student index ...\n");
}

void dropindex_professor()
{
   printf("Start dropping professor index ...\n");

   printf("... dropping professor index on pid *\n");
   if (DropIndex(PROFREL, "pid") != FEE_OK)
     FE_PrintError("Problem dropping the index\n");

   printf("End of dropping professor index ...\n");
}

/**************************************/
/* destroy_table:                     */
/* testing DestroyTable               */
/* by destroying the student relation */
/**************************************/

void destroy_table(char *relname)
{
   printf("Start destroying %s table ...\n",relname);

   if (DestroyTable(relname) != FEE_OK)
      FE_PrintError("Destruction of relation failed");

   printf("End of destroying %s table ...\n",relname);
}

/***********************************************/
/* cleanup:                                    */
/* Gets rid of files and directories generated */
/* by the tests                                */
/***********************************************/
void cleanup(void)
{
   fflush(NULL);
   DBdestroy(TESTDB);
   printf("cleanup done!\n");
}

int main(int argc, char *argv[])
{
  int testnum, i;

  cleanup();
  DBcreate(TESTDB);
  printf(">>> Database %s has been created\n", TESTDB);

  DBconnect(TESTDB);
  show_catalogs();

  printf(">>> Creating tables for students and professors ...\n");
  create_student();
  create_professor();
  show_catalogs();

  printf(">>> Indexing and loading students ...\n");
  index_student();
  load_student();
  show_table(STUDREL);
  DBclose(TESTDB);

  DBconnect(TESTDB);
  show_catalogs();

  printf(">>> Loading and indexing professors ...\n");
  load_professor();
  index_professor();
  show_table(PROFREL);

  printf(">>> Dropping indexes of students and professors ...\n");
  dropindex_student();
  dropindex_professor();
  show_catalogs();

  printf(">>> Destroying tables of students and professors ...\n");
  destroy_table(STUDREL);
  destroy_table(PROFREL);
  show_catalogs();

  DBclose(TESTDB);
}
