#ifndef __FE_H__
#define __FE_H__

/****************************************************************************
 * fe.h: External interface for the FE (UT and QU) layers
 ****************************************************************************/

/*
 * maximum length of an identifier (relation or attribute name)
 */
#define MAXNAME		12

/*
 * ATTR_DESCR: attribute descriptor used in CreateTable
 */
typedef struct{
    char *attrName;	/* relation name	*/
    int attrType;	/* type of attribute	*/
    int attrLen;	/* length of attribute	*/
} ATTR_DESCR;

/*
 * REL_ATTR: describes a qualified attribute (relName.attrName)
 */
typedef struct{
    char *relName;	/* relation name	*/
    char *attrName;	/* attribute name	*/
} REL_ATTR;

/*
 * ATTR_VAL: <attribute, value> pair
 */
typedef struct{
    char *attrName;	/* attribute name	*/
    int valType;	/* type of value	*/
    int valLength;	/* length if type = STRING_TYPE */
    void *value;	/* value for attribute	*/
} ATTR_VAL;


/*
 * Prototypes for FE layer functions
 * They start with UT because FE layer functions are
 * divided in utilities (UT) and query (QU) functions.
 */

int  CreateTable(char *relName,		/* name	of relation to create	*/
		int numAttrs,		/* number of attributes		*/
		ATTR_DESCR attrs[],	/* attribute descriptors	*/
		char *primAttrName);	/* primary index attribute	*/

int  DestroyTable(char *relName);	/* name of relation to destroy	*/

int  BuildIndex(char *relName,		/* relation name		*/
		char *attrName);	/* name of attr to be indexed	*/

int  DropIndex(char *relname,		/* relation name		*/
		char *attrName);	/* name of indexed attribute	*/

int  PrintTable(char *relName);		/* name of relation to print	*/

int  LoadTable(char *relName,		/* name of target relation	*/
		char *fileName);	/* file containing tuples	*/

int  HelpTable(char *relName);		/* name of relation		*/

/*
 * Prototypes for QU layer functions
 */

int  Select(char *srcRelName,		/* source relation name         */
		char *selAttr,		/* name of selected attribute   */
		int op,			/* comparison operator          */
		int valType,		/* type of comparison value     */
		int valLength,		/* length if type = STRING_TYPE */
		void *value,		/* comparison value             */
		int numProjAttrs,	/* number of attrs to print     */
		char *projAttrs[],	/* names of attrs of print      */
		char *resRelName);       /* result relation name         */

int  Join(REL_ATTR *joinAttr1,		/* join attribute #1            */
		int op,			/* comparison operator          */
		REL_ATTR *joinAttr2,	/* join attribute #2            */
		int numProjAttrs,	/* number of attrs to print     */
		REL_ATTR projAttrs[],	/* names of attrs to print      */
		char *resRelName);	/* result relation name         */

int  Insert(char *relName,		/* target relation name         */
	int numAttrs,			/* number of attribute values   */
	ATTR_VAL values[]);		/* attribute values             */

int  Delete(char *relName,		/* target relation name         */
		char *selAttr,		/* name of selection attribute  */
		int op,			/* comparison operator          */
		int valType,		/* type of comparison value     */
		int valLength,		/* length if type = STRING_TYPE */
		void *value);		/* comparison value             */


void FE_PrintError(char *errmsg);	/* error message		*/
void FE_Init(void);			/* FE initialization		*/



/*
 * FE layer error codes
 */

#define FEE_OK			0
#define FEE_ALREADYINDEXED	(-40)
#define FEE_ATTRNAMETOOLONG	(-41)
#define FEE_DUPLATTR		(-42)
#define FEE_INCOMPATJOINTYPES	(-43)
#define FEE_INCORRECTNATTRS	(-44)
#define FEE_INTERNAL		(-45)
#define FEE_INVATTRTYPE		(-46)
#define FEE_INVNBUCKETS		(-47)
#define FEE_NOTFROMJOINREL	(-48)
#define FEE_NOSUCHATTR		(-49)
#define FEE_NOSUCHDB		(-50)
#define FEE_NOSUCHREL		(-51)
#define FEE_NOTINDEXED		(-52)
#define FEE_PARTIAL		(-53)
#define FEE_PRIMARYINDEX	(-54)
#define FEE_RELCAT		(-55)
#define FEE_RELEXISTS		(-56)
#define FEE_RELNAMETOOLONG	(-57)
#define FEE_SAMEJOINEDREL	(-58)
#define FEE_SELINTOSRC		(-59)
#define FEE_THISATTRTWICE	(-60)
#define FEE_UNIONCOMPAT		(-61)
#define FEE_WRONGVALTYPE	(-62)
#define FEE_RELNOTSAME          (-63)
#define FEE_NOMEM               (-64)
#define FEE_EOF                 (-65)
#define FEE_CATALOGCHANGE       (-66)
#define FEE_STRTOOLONG          (-67)
#define FEE_SCANTABLEFULL       (-68)
#define FEE_INVALIDSCAN         (-69)
#define FEE_INVALIDSCANDESC     (-70)
#define FEE_INVALIDOP           (-71)

#define FE_NERRORS              (71)

/*************/
#define FEE_UNIX		(-100)
#define FEE_HF			(-101)
#define FEE_AM                  (-102)
#define FEE_PF                  (-103)


#define INT_SIZE  4
#define REAL_SIZE 4

/*
 * global variables for the catalogs. Must be defined in the FE layer.
 */
extern int relcatFd, attrcatFd;

/*
 * global FE layer error value
 */
extern int FEerrno;

#endif
