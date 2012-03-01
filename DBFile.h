#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "InternalDB.h"
#include "HeapDB.h"
#include "Defs.h"
#include "SortedDB.h"
//typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	File f;
	Page p;
	int globalPageIndex;
	int fileType; //Default file type will be 0, but this will be set in Open/Create anyways.

	Record currentRec;

private:
	InternalDB *internal; //Our pointer to the internal DB virtual class
};
#endif
