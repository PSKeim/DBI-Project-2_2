#include "InternalDB.h"

InternalDB::InternalDB(){
}
InternalDB::~InternalDB(){
}

int InternalDB::Create (char *fpath, fType file_type, void *startup){

}

int InternalDB::Open (char *fpath){
}

int InternalDB::Close (){
}
void InternalDB::Load (Schema &myschema, char *loadpath){ }

void InternalDB::Add(Record& add){ }

int InternalDB::GetNext(Record &fetch){ }

int InternalDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){ }

void InternalDB::MoveFirst(){}
