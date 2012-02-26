#ifndef INTERNALDB_H
#define INTERNALDB_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

/**
	Internal DB is a virtual class intended to act as a base for the 3 specific types of DBFiles that we will be creating.
	As such, it will define virtual functions for each of the DBFile methods that require specific implementations (Load, Add, GetNext)
**/

class InternalDB {


public:
	
	InternalDB(File *fileHandler);
	virtual ~InternalDB();

	virtual void Load (Schema &myschema, char *loadpath);
	virtual void Add(Record& add);
	virtual int GetNext(Record &fetch);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);

protected:
	File *dbFile;
};
#endif
