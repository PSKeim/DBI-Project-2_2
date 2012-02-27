#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "HeapDB.h"
#include <iostream>
#include <fstream>

/**

This is the DBFile for project 2-2. At the end of this project, it should be able to handle both a Sorted and Heap implemented file.

TODO: Implement Virtual Class implementation of Heap, to prepare for the VC implementation of Sorted

**/

DBFile::DBFile () {	
}
/**
@purpose Creates a file at a given path, as well as a meta-data file for that file
@param *f_path The path at which the file is to be created
@param f_type The type (currently only Heap) that the file uses as a D.S.
@param *startup Dunno what this does yet.
@return 1 if successful, 0 if not
*/
int DBFile::Create (char *f_path, fType f_type, void *startup) {

	fileType = f_type;

	if(f_type == heap){ //Handle for fType 0, or the Heap type. All other types (currently) result in a "Fail to create File" situation.
		
		internal = new HeapDB();
		return internal->Create(f_path, f_type, startup);
		/*f.Open(0,f_path); //Open file to the path given, let's assume file handles the "unable to be created" error?
		internal = new HeapDB();

		//Now to make the Metafile
		string metafile;
		metafile.append(f_path);
		metafile.append(".header");
		//Open and write out the type

		ofstream dbFile;
		dbFile.open(metafile.c_str(),ios::out);
		dbFile << f_type; //In this case, writes 0. If I need it to do more metadata later, I'll mess with it.
		dbFile.close();
		return 1;*/
	}

	else if(f_type == sorted){
		f.Open(0,f_path); //Open file to the path given, if this fails, then the system is exiting anyways, so no error handling here	

		//internal = new SortedDB();
		//Now to make the Metafile. I should probably start adding error handling for the meta file.
		string metafile;
		metafile.append(f_path);
		metafile.append(".header");

		ofstream dbFile;
		dbFile.open(metafile.c_str(),ios::out);
		dbFile << f_type; //In this case, writes 0. If I need it to do more metadata later, I'll mess with it.
		dbFile.close();
		return 1;
	}

	return 0;
}

/**
@purpose Opens a previously saved File
@param f_path Location of the File
@return 1 on success, system exits on failure
*/
int DBFile::Open (char *f_path) {
//Loads a previously saved DBFile in. 
	//f.Open(1,f_path);
	//Read in what the metadata file says about the file at f_path
	
	
	internal->Open(f_path);
	//For Project 2-2 this gets more complicated, as we will need to know what TYPE of file it is, so we can choose the right internalDB file implementation
	
	//TODO: Check the .header file for the information 

	return 1;
}


/**
@purpose Given a schema and a load path, this loads the datbase table into our file
@param &f_schema The pointer to the schema that the table uses
@param *loadpath The path that the table file is located at
*/
void DBFile::Load (Schema &f_schema, char *loadpath) {
//Loads a bunch of data in from the file at loadpath.
//Stores it into the file.
//Adjusts pages as necessary.
	if(fileType == 0){
		internal->Load(f_schema, loadpath);
	}
	/*FILE *tableFile = fopen (loadpath, "r");
	//Code is more or less ripped from main.cc
        Record temp; //Holding variable
	//cout << "File size now is: " << f.GetLength() << endl;
	// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0; //Counter for debug. Take out of final product!
	globalPageIndex = 0; //File needs a page offset to know where it is putting the page. This is it.
        while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
		counter++;//Debug part of loop, just making sure it works
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
		//Right now Temp is the next record from our table file...
		if(p.Append(&temp) == 0){ //If the append function returns a 0, the append failed (page is full)
			//So we need to add the page to the file, and start again
			f.AddPage(&p, globalPageIndex);
			globalPageIndex++;
			p.EmptyItOut();
			if(p.Append(&temp) == 0){
				cerr << "Page repeatedly failed to append! Skipping record!";
			}
		}
       }
	//We need to add the final page to the File. 
	f.AddPage(&p,globalPageIndex);
	// Done? Maybe? I dunno.*/
	//cout << "File size after load is: " << f.GetLength() << endl;*/
}



/**
@purpose Moves the page we've got to point at the first page in the file (first record in the DB)
*/
void DBFile::MoveFirst () {
//Move it to point to the first page in File. Or something?
//Use File's GetPage to get the first page (offset 1?), and set p as it.
//So... f.GetPage(&p,1);

internal->MoveFirst();
/*p.EmptyItOut();
globalPageIndex = 0;
f.GetPage(&p,globalPageIndex);*/
}

/**
@purpose Closes the DB File
@return 1 on succes
*/
int DBFile::Close () {
		//Closes the file. Dunno 
		return internal->Close();
}
/**
@purpose Adds a record to the end of the DBFile
@param &rec The record to be added
*/
void DBFile::Add (Record &rec) {
//Okay, to Add, we must make sure that p = last page of the file.
//And then do shit to it. Namely, SCIENCE.
//Science is good.
//Wait. That doesn't make sense. We have to get the last page... and then add it? Because f.AddPage writes it out to file.
//But if we get the last page, does it zero it out to be nothing? MUST CHECK!
	if(fileType == 0){
		internal->Add(rec);
	}/*
	int page = f.GetLength()-2;
	cout << "Page is "<<page<<endl;
	cout << "GetLength is " << f.GetLength() << endl;
	if(page < 0){ //File has nothing in it (i.e. GetLength returned 0, so page = -1)
		p.Append(&rec); //If the file has nothing in it, neither does the page, so it's a clean append.
		f.AddPage(&p,0); //We then add the page, and leave
		return;
	}
	cout << "Getting page " << endl;
	f.GetPage(&p,page); //If the file does have at least one page, we get it
	cout << "Page got" <<endl;
	if(p.Append(&rec) == 1){ //Now we test the append. If it goes through
		cout << "Appended to current page" <<endl;
		f.AddPage(&p,page); //We overwrite the file's page with the new one
		cout << "Page re-added" <<endl;
		return; //And then we leave
	}

	//If the page append doesn't go through, then we have to add a new page
	p.EmptyItOut(); //Clear the page
	p.Append(&rec); //Add the record
	cout << "Appending to new p age" << endl;
	page++; //Increment which page offset we're talking about
	cout << "Adding new page to file" <<endl;
	f.AddPage(&p, page); //Add the page, and then we out.
	cout << "Added" <<endl;*/
}

/**
@purpose Gets the next record from file
@param &fetchme The record that we'll be putting the found record into
@return 0 on failure, 1 on success
*/
int DBFile::GetNext (Record &fetchme) {
	//The first thing to do is fetch through the current page.
	//If the page returns 0, then we need to load the next page and get from there
	//If the GPI > f's size, then we've reached the end of the records
	return internal->GetNext(fetchme);
	/*
	if(p.GetFirst(&fetchme) == 0){ //Check to see if anything is returned by our current page p
			globalPageIndex++; //Update page to the next one
		if(globalPageIndex < f.GetLength()-1){ //If nothing is returned, we check to see if p is the last page
			f.GetPage(&p,globalPageIndex);
			p.GetFirst(&fetchme);
			return 1;
		}

		return 0;//No records left
	}

	return 1; */
}

/**
@purpose Gets the next record that matches a CNF from file
@param &fetchme Same as in GetNext
@param &cnf The CNF that we'll compare this stuff to
@param &literal The literal record that we use for comparison. (I don't get it)
*/
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	return internal->GetNext(fetchme, cnf, literal);
	/*ComparisonEngine comp;

	int ret = GetNext(fetchme);

	if(ret == 0){ 
		return 0; //Nothing in the file, so nothing to do here!
	}	
	while(!comp.Compare (&fetchme, &literal, &cnf)){
		ret = GetNext(fetchme);

		if(ret == 0){ 
			return 0; //Nothing in the file, so nothing to do here!
		}	
		//if(p.GetFirst(&fetchme) == 0){
	//		globalPageIndex++; //Update page to the next one
	//		if(globalPageIndex < f.GetLength()-1){ //If nothing is returned, we check to see if p is the last page
	//			f.GetPage(&p,globalPageIndex);
	//			p.GetFirst(&fetchme);
	//		}
//
//			return 0;//No records left
//		}
	}

	return 1;*/
}
