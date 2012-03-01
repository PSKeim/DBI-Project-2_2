#include "SortedDB.h"

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"

#include <iostream>
#include <fstream>

SortedDB::SortedDB(){

}

SortedDB::~SortedDB(){

}

int SortedDB::Create (char *fpath, fType file_type, void *startup){
	f.Open(0,fpath);

	//Now to make the Metafile
	string metafile;
	metafile.append(fpath);
	metafile.append(".header");
	//Open and write out the type

	ofstream dbFile;
	dbFile.open(metafile.c_str(),ios::out);
	dbFile << file_type; //In this case, writes 0. If I need it to do more metadata later, I'll mess with it.
	dbFile.close();
	return 1;
}

int SortedDB::Open (char *fpath){
	return 1;
}

int SortedDB::Close(){
	f.close();
	//TODO: Write out the sort order of the file. Not sure how to do this yet. =O<<<<
	return 1;
}

void SortedDB::MoveFirst(){
	return;
}

void SortedDB::Load(Schema &myschema, char *loadpath){	

	return;
}

void SortedDB::Add(Record &rec){
	return;
}	

int SortedDB::GetNext(Record &fetch){
	return 1;
}

int SortedDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){
	return 1;
}


