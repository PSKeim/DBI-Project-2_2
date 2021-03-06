#ifndef HEAPDB_H
#define HEAPDB_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "InternalDB.h"
#include "DBFile.h"
#include "Defs.h"
/**
	Internal DB is a virtual class intended to act as a base for the 3 specific types of DBFiles that we will be creating.
	As such, it will define virtual functions for each of the DBFile methods that require specific implementations (Load, Add, GetNext)
**/

class HeapDB : public InternalDB {


public:
	
	HeapDB();
	~HeapDB();

	 int Create (char *fpath, fType file_type, void *startup);
	 int Open (char *fpath);
	 int Close ();
	 void Load (Schema &myschema, char *loadpath);
	 void Add(Record &rec);
	 void MoveFirst();
	 int GetNext(Record &fetch);
	 int GetNext (Record &fetchme, CNF &cnf, Record &literal);

private:
	File f;
	Page p;
	int globalPageIndex;
	Record currentRec;
};
#endif
