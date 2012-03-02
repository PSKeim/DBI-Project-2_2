#include "SortedDB.h"

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "BigQ.h"

#include <iostream>
#include <fstream>


#define MAX_PIPE 100

SortedDB::SortedDB(){

}

SortedDB::~SortedDB(){

}

int SortedDB::Create (char *fpath, fType file_type, void *startup){
	f.Open(0,fpath);

	sortinfo = *((SortInfo*) startup);

	//Now to make the Metafile
	string metafile;
	metafile.append(fpath);
	metafile.append(".header");
	
	/**
	Meta file will have the structure below:
	<file type, int>
	<runlen, int>
	<number of attributes, int>
	(<attribute number, int>
	<attribute type, int (actually an enum type, but whatevs)) * number of attributes
	
	**/

	//Open and write out the type
	cout << "Writing out to meta file" << endl;
	
	ofstream dbFile;
	dbFile.open(metafile.c_str(),ios::out);
	dbFile << file_type << endl; //In this case
	dbFile << sortinfo.runLength << endl;

	int numAttributes = sortinfo.sortOrder->numAtts;
	for(int i = 0; i < numAttributes; i++){
		dbFile << sortinfo.sortOrder->whichAtts[i] << endl;
		dbFile << sortinfo.sortOrder->whichTypes[i] << endl;
	}
	//Now that we've written that out, we exit the function.
	
	dbFile.close();
	
	return 1;
}

int SortedDB::Open (char *fpath){
	//We open the file itself
	f.Open(1,fpath);
	//Now to get all sorts of useful information. Like the sort order and all.
	string metafile;
	metafile.append(fpath);
	metafile.append(".header");
	ifstream metaFile;
	metaFile.open(metafile.c_str());

	
	string info; //Information that we're pulling in
	//Attributes for our struct
	int runlen = 0;
	OrderMaker *order = new OrderMaker(); 
	//Loop variable, use later when seeting up the order maker
	int numAttributes = 0;
	/*
	File structure is defined in the Create function
	*/
	getline(metaFile, info); //First line will be file type, which we can ignore
	getline(metaFile, info); //This line is run length, which we actually want to use
	runlen = atoi(info.c_str()); //Now we know the run length and can set it into our struct later
	
	getline(metaFile, info);
	numAttributes = atoi(info.c_str());
	order->numAtts = numAttributes;
	//Now we need to loop over the file and make sure we've got the sort attributes read in to the order maker
	for(int i = 0; i < numAttributes; i++){
		//Okay, now we're 
		getline(metaFile, info);
		//Order will now know what the attributes are that are sorted
		order->whichAtts[i] = atoi(info.c_str());
		
		//Every attribute has a type! So the next line after the attribute itself is the type of the attribute...
		getline(metaFile, info);
		//Now order knows what types the sorted attributes are.
		order->whichTypes[i] = (Type) atoi(info.c_str());
	}
	
	//So now we have a runlen, and an ordermaker set up. That lets us make our struct.
	
	return 1;
}

int SortedDB::Close(){
	f.Close();	//Metadata being written out is handled by the Create operator. Close just shuts down the shop.
	return 1;
}

void SortedDB::MoveFirst(){
	SetWriting(false);
	p.EmptyItOut();
	globalPageIndex = 0;
	f.GetPage(&p,globalPageIndex);
}

void SortedDB::Load(Schema &myschema, char *loadpath){	
	//By the time we've gotten here, we should have sort order, which means load should be somewhat easy.
	
	SetWriting(true); //We be writing shit to file, yo.
	//After reading spec, this needs to be replaced with a "flip" function that does the work based on what the boolean is going into it.
	//For now, writing = true will work
		
	FILE *file = fopen(loadpath, "r");
	
	return;
}

void SortedDB::Add(Record &rec){
	return;
}	

int SortedDB::GetNext(Record &fetch){
	
	SetWriting(false);
	if(p.GetFirst(&fetch) == 0){ //Check to see if anything is returned by our current page p
			globalPageIndex++; //Update page to the next one
		if(globalPageIndex < f.GetLength()-1){ //If nothing is returned, we check to see if p is the last page
			f.GetPage(&p,globalPageIndex);
			p.GetFirst(&fetch);
			return 1;
		}

		return 0;//No records left
	}

	return 1; 
}

int SortedDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){
	SetWriting(false);
	return 1;
}

/*
This function is the one that handles the change from writing to reading. Right now, all it does is reverse the writing boolean. will update more
*/
void SortedDB::SetWriting(bool newMode){

	if(isWriting == newMode) return; //If the mode isn't changing, then we don't care
	//So, we're switching either from READING to WRITING, or vice versa.
	//if isWriting is true now, then we're writing. So we'll be switching to Reading
	if(isWriting){
		//Switching to Reading phase
		//This writes everything out to the file, and GTFO's
		isWriting = false;
		//WriteToFile();
	}
	else{
		//If isWriting isn't true, then we're READING, so we need to switch to Writing
		isWriting = true;
		//Switching from Reading to Writing requires that we reset our BigQ.
		resetBQ();
	}
}

void SortedDB::WriteToFile(){

	Record outRec = new Record;
	vector<Record *> outRecs;
	//First part of this is shutting down the in pipe
	in->ShutDown();

	//Then, we need to remove records from the output pipe

	while(out->Remove(outRec)){
		Record *vecRec = new Record();
		vecRec->Copy(&temp);
		outRecs.pushBack(vecRec);
	}

	//Once this has finished, we've got al lthe records that were in the output pipe. We need to merge these in our current file

}

void SortedDB::resetBQ(){

	//Nick say delete in and out before doing new, but I'm not sure when they'd be originally set. Best to be careful and make sure I don't double free quite just yet
	in = new Pipe(MAX_PIPE);
	out = new Pipe(MAX_PIPE);

	bigQ = new BigQ(*in, *out, *(sortinfo.sortOrder), sortinfo.runLength);
}
