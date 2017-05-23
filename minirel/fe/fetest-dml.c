/*
 * fetest-dml.c : tests DML and Query functions.
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
#define STUDREL		"student.sid"
#define PROFREL		"prof.pid"

#include "studprofdb-schema.h"

extern int relcatFd, attrcatFd;


/******************************************** */
/* show_catalogs:                             */
/* Testing the PrintTable() and HelpTable()   */
/* by printing the relcat and attrcat         */
/* catalogs of TESTDB.                        */
/**********************************************/

void show_catalogs()
{
   DBconnect(TESTDB);

   /* PrintTable part */
   printf("Print the relation catalog\n");
   PrintTable("relcat");
   printf("Print the attribute catalog\n");
   PrintTable("attrcat");

   /* HelpTable part */
   printf("HelpTable with relcat\n");
   HelpTable("relcat");
   printf("HelpTable with attrcat\n");
   HelpTable("attrcat");

   DBclose(TESTDB);
}

/***********************************/
/* create_student:                  */
/* testing CreateTable by creating */
/* a relation called student       */
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

   printf("Start create_student ...  \n");

   /* specifying the attributes of the relation to be created */
   /* "sid", "sname", "gpa", "age", "advisor" attributes */
   make_attrDesc(&in_attrs[0],"sid",INT_TYPE,sizeof(int));
   make_attrDesc(&in_attrs[1],"sname",STRING_TYPE,MAXSTRLEN);
   make_attrDesc(&in_attrs[2],"gpa",REAL_TYPE,sizeof(float));
   make_attrDesc(&in_attrs[3],"age",INT_TYPE,sizeof(int));
   make_attrDesc(&in_attrs[4],"advisor",INT_TYPE,sizeof(int));

   DBconnect(TESTDB);

   if (CreateTable(STUDREL, STUD_NUM_ATTRS, in_attrs, NULL) != FEE_OK)
      FE_PrintError("Relation creation failed in create_student");

   DBclose(TESTDB);
   printf("End create_student ...  \n");
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

   DBconnect(TESTDB);

   if (CreateTable(PROFREL, PROF_NUM_ATTRS, in_attrs, NULL) != FEE_OK)
      FE_PrintError("Relation creation failed in create_professor");

   DBclose(TESTDB);
   printf("End create_professor ...  \n");
}

/****** insert_student:   ***********/
/* inserting some tuples in student */
/* assumes the relation has been    */
/* created and it is under directory*/
/* TESTDB                           */
/************************************/

void make_attrVal(ATTR_VAL *aval, char *name, char type, int len)
{
  aval->attrName = (char*)malloc(MAXNAME);
  strcpy(aval->attrName,name);
  aval->valType = type;
  aval->valLength = (type==STRING_TYPE)? MAXSTRLEN : sizeof(int);
  aval->value = (char*)malloc(aval->valLength);
}

void insert_student()
{
  ATTR_VAL values[STUD_NUM_ATTRS];
  int	i;

  printf("Start insert_student ...  \n");
  DBconnect(TESTDB);

  make_attrVal(&values[0],"sid",INT_TYPE,sizeof(int));
  make_attrVal(&values[1],"sname",STRING_TYPE,MAXSTRLEN);
  make_attrVal(&values[2],"gpa",REAL_TYPE,sizeof(float));
  make_attrVal(&values[3],"age",INT_TYPE,sizeof(int));
  make_attrVal(&values[4],"advisor",INT_TYPE,sizeof(int));

  for(i=0; i < STUD_NUM ;i++) {
    *(int*)(values[0].value) = i;
    sprintf(values[1].value,"student%d",i);
    *(float*)(values[2].value) = 99.0 * ((float)rand()/RAND_MAX);
    *(int*)(values[3].value) = i + 10;
    *(int*)(values[4].value) = i % PROF_NUM;

    if (Insert(STUDREL,STUD_NUM_ATTRS,values) != FEE_OK)
	FE_PrintError("Insert student faild.");
  }

   DBclose(TESTDB);
   printf("End insert_student ...  \n");
}

void insert_professor()
{
  ATTR_VAL values[PROF_NUM_ATTRS];
  int i;

  printf("Start insert_professor ...  \n");
  DBconnect(TESTDB);

  make_attrVal(&values[0],"pid",INT_TYPE,sizeof(int));
  make_attrVal(&values[1],"pname",STRING_TYPE,MAXSTRLEN);
  make_attrVal(&values[2],"office",INT_TYPE,sizeof(int));

  for(i=0; i < PROF_NUM ;i++) {
    *(int*)(values[0].value) = i;
    sprintf(values[1].value,"professor%d",i);
    *(int*)(values[2].value) = i + 400;

    if (Insert(PROFREL,PROF_NUM_ATTRS,values) != FEE_OK)
	FE_PrintError("Insert professor faild.");
  }

   DBclose(TESTDB);
   printf("End insert_professor ...  \n");
}

/*******************************************************/
/* show_student:                                       */
/* Printing the student relation, and some info about  */
/* it by using PrintTable and HelpTable.               */
/*******************************************************/

void show_table(char *relname)
{
   printf("Start show_table(%s) ... \n",relname);
   DBconnect(TESTDB);

   if (HelpTable(relname) != FEE_OK)
      FE_PrintError("Error: while executing Help relation\n");

   if (PrintTable(relname) != FEE_OK)
      FE_PrintError("Error: while printing relation\n");

   DBclose(TESTDB);
   printf("End show_table(%s) ... \n",relname);
}


/**************************************/
/* index_student:                     */
/* Testing BuildIndex                 */
/* by creating indeces on student     */
/**************************************/

void index_student()
{
  printf("Start index_student ... \n");
  DBconnect(TESTDB);

  if (BuildIndex(STUDREL, "sname") != FEE_OK)
    FE_PrintError("Problem building the studname index\n");

  if (BuildIndex(STUDREL, "age") != FEE_OK)
    FE_PrintError("Problem building the age index\n");

  DBclose(TESTDB);
  printf("End index_student ... \n");
}

void index_professor()
{
  printf("Start index_professor ... \n");
  DBconnect(TESTDB);

  if (BuildIndex(PROFREL, "pid") != FEE_OK)
    FE_PrintError("Problem building the sid index\n");

  DBclose(TESTDB);
  printf("End index_professor ... \n");
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

void delete_student()
{
  float gpaval = 40.;

  printf("Start deleting students ... \n");
  DBconnect(TESTDB);

  if (Delete(STUDREL,"gpa",LT_OP,REAL_TYPE,
			sizeof(float),&gpaval) != FEE_OK)
	FE_PrintError("Delete student faild.");

  DBclose(TESTDB);
  printf("End deleting students ... \n");
}

void delete_professor()
{
  int offval = 401;

  printf("Start deleting professors ... \n");
  DBconnect(TESTDB);

  if (Delete(PROFREL,"office",LE_OP,INT_TYPE,sizeof(int),&offval) != FEE_OK)
	FE_PrintError("Delete professor faild.");

  DBclose(TESTDB);
  printf("End deleting professors ... \n");
}

void select_student()
{

  int numProjs;
  float gpaval;
  char *projAttrs[STUD_NUM_ATTRS];

  DBconnect(TESTDB);

  gpaval = 75.;
  numProjs = 4;
  projAttrs[0] = "sname";
  projAttrs[1] = "advisor";
  projAttrs[2] = "gpa";
  projAttrs[3] = "age";

  if (Select(STUDREL,"gpa",GT_OP,REAL_TYPE,sizeof(float),&gpaval,
		numProjs,projAttrs,NULL) != FEE_OK)
	FE_PrintError("Select student.sid faild.");

  DBclose(TESTDB);
}

void select_professor()
{

  int numProjs, pidval = 7;
  char *projAttrs[PROF_NUM_ATTRS];

  DBconnect(TESTDB);

  numProjs = 2;
  projAttrs[0] = "pname";
  projAttrs[1] = "office";

  if (Select(PROFREL,"pid",LT_OP,INT_TYPE,sizeof(int),&pidval,
		numProjs,projAttrs,NULL) != FEE_OK)
	FE_PrintError("Select professor.sid faild.");

  DBclose(TESTDB);
}

void make_relAttr(REL_ATTR *ra, char *relname, char *attrname)
{
  ra->relName = (char *) malloc(MAXNAME);
  ra->attrName = (char *) malloc(MAXNAME);
  strcpy(ra->relName, relname);
  strcpy(ra->attrName, attrname);
}

void join_student_prof(char *resRel)
{
  REL_ATTR relAttrs[2];
  REL_ATTR projAttrs[8];
  int	numProjs;

  printf("Start joining students and professors ... \n");

  make_relAttr(&relAttrs[0],STUDREL,"advisor");
  make_relAttr(&relAttrs[1],PROFREL,"pid");

  numProjs = 5;
  make_relAttr(&projAttrs[0],STUDREL,"sid");
  make_relAttr(&projAttrs[1],STUDREL,"sname");
  make_relAttr(&projAttrs[2],STUDREL,"advisor");
  make_relAttr(&projAttrs[3],PROFREL,"pid");
  make_relAttr(&projAttrs[4],PROFREL,"pname");

  DBconnect(TESTDB);

  if (Join(&relAttrs[0],EQ_OP,&relAttrs[1],
		numProjs,projAttrs,resRel) != FEE_OK)
	FE_PrintError("Join student.sid and prof.pid faild.");

  DBclose(TESTDB);
  printf("End joining students and professors ... \n");
}

int main(int argc, char *argv[])
{
  char *progname = argv[0];	/* since we will be changing argv */
  int testnum, i;

  cleanup();
  DBcreate(TESTDB);
  printf(">>> Database %s has been created\n", TESTDB);

  show_catalogs();

  printf(">>> Creating tables for students and professors ...\n");
  create_student();
  create_professor();
  show_catalogs();

  printf(">>> Inserting students and professors ...\n");
  insert_student();
  show_table(STUDREL);
  insert_professor();
  show_table(PROFREL);

  printf(">>> Selecting / Joining students and professors ...\n");
  select_student();
  select_professor();
  join_student_prof(NULL);

  printf(">>> Deleting students and professors ...\n");
  delete_student();
  delete_professor();
  show_table(STUDREL);
  show_table(PROFREL);

  printf(">>> Indexing / Joining students and professors ...\n");
  index_student();
  index_professor();
  join_student_prof("stud_prof");
  show_table("stud_prof");
  show_catalogs();
}
