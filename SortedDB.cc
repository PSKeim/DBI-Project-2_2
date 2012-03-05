
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


#define MAX_PIPE 1000

SortedDB::SortedDB(): money(NULL), isWriting(false), in(NULL), out(NULL){
	//money = NULL;
}

SortedDB::~SortedDB(){

}

int SortedDB::Create (char *fpath, fType file_type, void *startup){
	f.Open(0,fpath);
	globalPageIndex  = 0;
	sortinfo = *((SortInfo*) startup);
	filePath = string(fpath);
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
	//cout << "Writing out to meta file" << endl;
	//sortinfo.sortOrder->Print(); // Prints out the OM's status, use for debugging
	
	ofstream dbFile;
	dbFile.open(metafile.c_str(),ios::out);
	dbFile << file_type << endl; //In this case
	dbFile << sortinfo.runLength << endl;

	int numAttributes = sortinfo.sortOrder->numAtts;
	dbFile << numAttributes << endl;
	for(int i = 0; i < numAttributes; i++){
		dbFile << sortinfo.sortOrder->whichAtts[i] << endl;
		dbFile << sortinfo.sortOrder->whichTypes[i] << endl;
	}
	//Now that we've written that out, we exit the function.
	
	dbFile.close();
	
	return 1;
}

int SortedDB::Open (char *fpath){
	filePath = string(fpath);
	//We open the file itself
	f.Open(1,fpath);
	globalPageIndex = 0;
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
	//cout << "First read produced: " << info << endl;
	getline(metaFile, info); //This line is run length, which we actually want to use
	//cout << "Second read produced: " << info << endl;
	runlen = atoi(info.c_str()); //Now we know the run length and can set it into our struct later
	
	getline(metaFile, info);
	//cout << "Third read produced: " << info << endl;
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
	sortinfo.runLength = runlen;
	sortinfo.sortOrder = order;
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
		
	FILE *file = fopen(loadpath, "r");

	Record sucker;

	while(sucker.SuckNextRecord(&myschema, file)){
		Record *temp = new Record();
		temp->Copy(&sucker);
		in->Insert(temp);
	}

	fclose(file);
	
	return;
}

void SortedDB::Add(Record &rec){

	SetWriting(true); //Again, writing stuff to file.
	
	in->Insert(&rec);
		
	return;
}	

int SortedDB::GetNext(Record &fetch){
	SetWriting(false);
	//cout << "In Get Next for test case." << endl;
	if(p.GetFirst(&fetch) == 0){ //Check to see if anything is returned by our current page p
			globalPageIndex++; //Update page to the next one
		if(globalPageIndex < f.GetLength()-1){ //If nothing is returned, we check to see if p is the last page
			f.GetPage(&p,globalPageIndex);
			p.GetFirst(&fetch);
			return 1;
		}
		cout << "Nothing left in the file!" << endl;
		return 0;//No records left
	}
	return 1; 
}

int SortedDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){
	cout << "In GetNext CNF for test." << endl;
	SetWriting(false, true); //The one query where we WANT To tell SetWriting that it's a CNF
	//This is because if we run multiple GNCNF queries, we don't want to have to re-do all the work
	//Because, you know, it's a lot of work and stuff.
	
	//Need to tell if money is null or not, because we delete it on any call that's not GNCNF
	
	ComparisonEngine comp;
	
	int ret = GetNext(fetchme);
	cout << "Got past the initial ret" << endl;
	if(ret == 0){ 
		return 0; //Nothing in the file, so nothing to do here!
	}	
	while(!comp.Compare (&fetchme, &literal, &cnf)){
		cout << "Comparing records!" << endl;
		ret = GetNext(fetchme);
		
		if(ret == 0){ 
			cout << "Nothing found, time to leave. " << endl;
			return 0; //Nothing in the file, so nothing to do here!
		}	
		/*if(p.GetFirst(&fetchme) == 0){
		 globalPageIndex++; //Update page to the next one
		 if(globalPageIndex < f.GetLength()-1){ //If nothing is returned, we check to see if p is the last page
		 f.GetPage(&p,globalPageIndex);
		 p.GetFirst(&fetchme);
		 }
		 
		 return 0;//No records left
		 }*/
	}
	
	return 1;
	
	/*	
	Record *possMatch = new Record;

	if(money == NULL){ //Okay, we only want to do this building IF MONEY IS NULL
		money = new OrderMaker;
	}
	
	//Fuck the work, let's go straight to getting next. I don't understand shit about where to start
	//on constructing the order maker
	
	p.GetFirst(&possMatch);
	

	return 1;
	
	*/
}

/*
This function is the one that handles the change from writing to reading. Right now, all it does is reverse the writing boolean. will update more
 
 This is a minor change
*/
void SortedDB::SetWriting(bool newMode, bool isCNF){
	
	if(!isCNF && money != NULL){
		delete money; //This deletes the object money points to?
		money = NULL;
	}
	
	if(isWriting == newMode) return; //If the mode isn't changing, then we don't care
	//So, we're switching either from READING to WRITING, or vice versa.
	//if isWriting is true now, then we're writing. So we'll be switching to Reading
	//cout << "Setting the writing mode to " << newMode << endl;
	if(isWriting){
		//Switching to Reading phase
		//This writes everything out to the file, and GTFO's
		isWriting = false;
		WriteToFile();
		MoveFirst(); //Basically, I'm saying that if you want to read after adding/loading, you start from the beginning.
		//Will change this if someone advises me not to do it that way
	}
	else{
		//If isWriting isn't true, then we're READING, so we need to switch to Writing
		isWriting = true;
		//Switching from Reading to Writing requires that we reset our BigQ.
		resetBQ();
	}
}

void SortedDB::WriteToFile(){

	in->ShutDown();

	//cout << "Have shut down in. In WriteToFile." << endl;
	cout << "File path is: "<< filePath << endl;
	//The idea here is that we have a bunch of records in the output pipe. We need to merge these in with our file.
	//This has two cases: 1) The file is empty, 2) The file is not empty

	Record readIn; //Read in from pipe
	Record pageIn; //Read in from page
	Page holderP;
	Page tempWriteoutP;
	//If the file is empty, we have an easy case. We just remove the records from the pipe, and page them in as necessary.
//	cout << "Checking that the file is empty" << endl;
	if(f.GetLength() <= 0){
		//cout << "File is empty." << endl;

		while(out->Remove(&readIn)){
			//cout << "Removing shit from the out pipe." << endl;
			if(0 == holderP.Append(&readIn)){
				f.AddPage(&holderP, globalPageIndex);
				globalPageIndex++;
				holderP.EmptyItOut();
				holderP.Append(&readIn);
			}
		}
		//cout << "Have finished removing shit from the out pipe." << endl;
		f.AddPage(&holderP, globalPageIndex);
		//cout << "File added page at index" << globalPageIndex << endl;
		globalPageIndex++;
	}
	else{
		cout << "File is not empty." << endl;
		//Assuming the file is not empty, we have to decide what the best way to do it is
		//Time for stupid ideas!
		globalPageIndex = 0;
		bool stuffInPipe = true; //Lets us know if the pipe is empty
		bool stuffInFile = true; //Lets us know when the file is empty (true because file length is > 0)
		bool pipeIsSmaller;
		//In this idea, we read from the output pipe and the file record by record, and write them out to a temp file
		File tempF;

		tempF.Open(0,"temptemptemp.bin");
		int tempIndex = 0;
		ComparisonEngine cmp;
		stuffInPipe = out->Remove(&readIn);
		
		f.GetPage(&holderP, globalPageIndex);
		globalPageIndex++;
		holderP.GetFirst(&pageIn);

		//At this point, we have the first record in the pipe, and the first record in the first page of the file
		//What to do now? We need to compare the two. We have our comparison engine, cmp.
		//We want to know which one is strictly smaller, let's do our comparison
		while(stuffInPipe || stuffInFile){
			//As we enter this the first time, we have something from the pipe and something from the file
			//Later on, though, the pipe or file might be empty.
			//How to handle this?

			if(!stuffInPipe){
				//We are out of items from the pipe. That means merge is finished, and we need to pump out the rest of the items from
				//the actual file to the temp file.
				do{
					if(0 == tempWriteoutP.Append(&readIn)){
						tempF.AddPage(&tempWriteoutP, tempIndex);
						tempIndex++;
				 		tempWriteoutP.Append(&readIn);
					}
					
					if(0 == holderP.GetFirst(&pageIn) && globalPageIndex < f.GetLength()){
						f.GetPage(&holderP, globalPageIndex);
						globalPageIndex++;
						holderP.GetFirst(&pageIn);
					}

				}while(globalPageIndex < f.GetLength());				
			
			
			}
			else if(!stuffInFile){
				//amazingly, we ran out of file stuff before we ran out of pipe stuff. So. Huh.
				//So now we just loop over the pipe until it is empty, packaging it as we go...
				
				//We should only reach this if the last record eaten was the file record (otherwise the abvoe one will trigger)
				//So:
				do{
					if(0 == tempWriteoutP.Append(&readIn)){
					tempF.AddPage(&tempWriteoutP, tempIndex);
					tempIndex++;
					tempWriteoutP.Append(&readIn);
					}
				}while(out->Remove(&readIn));

				stuffInPipe = false;
				
			}

			if(cmp.Compare(&readIn, &pageIn, sortinfo.sortOrder) < 0)
			{
				pipeIsSmaller = true;
			}
			else pipeIsSmaller = false;
	
			//If We find which one is smaller, append it to the page, load a new record from the required location, and move on
			if(pipeIsSmaller){ 
				if(0 == tempWriteoutP.Append(&readIn)){
					tempF.AddPage(&tempWriteoutP, tempIndex);
					tempIndex++;
					tempWriteoutP.Append(&readIn);
				}
				stuffInPipe = out->Remove(&readIn);
			}
			else{
				if(0 == tempWriteoutP.Append(&pageIn)){
					tempF.AddPage(&tempWriteoutP, tempIndex);
					tempIndex++;
					tempWriteoutP.Append(&pageIn);
				}

				if(0 == holderP.GetFirst(&pageIn)){ //If we run out items in this page of the pipe, we need to get a new page!
					if(globalPageIndex >= f.GetLength()){
							stuffInFile = false;
					}
					else{
						f.GetPage(&holderP, globalPageIndex);
						globalPageIndex++;
						holderP.GetFirst(&pageIn);
					}
				}
			}
		}

		tempF.Close();
		rename("temptemptemp.bin", filePath.c_str());
	} //End initial if/else statement
}

void SortedDB::resetBQ(){
	
	delete in;
	delete out;
	//cout << "Revving up BigQ." << endl;
	//Nick say delete in and out before doing new, but I'm not sure when they'd be originally set. Best to be careful and make sure I don't double free quite just yet
	in = new Pipe(MAX_PIPE);
	out = new Pipe(MAX_PIPE);
	//Apparently the sortinfo.sortOrder is not going through? IMPROV TIME!

	cout << "The number of attributes in the sortOrder is " << sortinfo.sortOrder->numAtts << endl;
	OrderMaker *o = sortinfo.sortOrder;
	//*(sortinfo.sortOrder)
	bigQ = new BigQ(*in, *out, *o , sortinfo.runLength);
}
