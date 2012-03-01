#ifndef INTERNALDB_H
#define INTERNALDB_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"


/**
	Internal DB is a virtual class intended to act as a base for the 3 specific types of DBFiles that we will be creating.
	As such, it will define virtual functions for each of the DBFile methods that require specific implementations (Load, Add, GetNext)
**/


class InternalDB {


public:
	
	InternalDB();
	virtual ~InternalDB();

	virtual int Create (char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;
	virtual void Load (Schema &myschema, char *loadpath) = 0;
	virtual void Add(Record& add) = 0;
	virtual int GetNext(Record &fetch) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	virtual void MoveFirst() = 0;

protected:
	File *dbFile;
};
#endif
