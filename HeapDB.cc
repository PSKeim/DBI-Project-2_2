#include "HeapDB.h"

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>

HeapDB::HeapDB(File *fileHandler){
	dbFile = fileHandler;
}

HeapDB::~HeapDB(){
}


void HeapDB::Load(Schema &myschema, char *loadpath){	
//Loads a bunch of data in from the file at loadpath.
//Stores it into the file.
//Adjusts pages as necessary.
       FILE *tableFile = fopen (loadpath, "r");
	//Code is more or less ripped from main.cc
        Record temp; //Holding variable
	//cout << "File size now is: " << f.GetLength() << endl;
	// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	//int counter = 0; //Counter for debug. Take out of final product!
	globalPageIndex = 0; //File needs a page offset to know where it is putting the page. This is it.
        while (temp.SuckNextRecord (&myschema, tableFile) == 1) {
		/*counter++;//Debug part of loop, just making sure it works
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}*/
		//Right now Temp is the next record from our table file...
		if(p.Append(&temp) == 0){ //If the append function returns a 0, the append failed (page is full)
			//So we need to add the page to the file, and start again
			f->AddPage(&p, globalPageIndex);
			globalPageIndex++;
			p.EmptyItOut();
			if(p.Append(&temp) == 0){
				cerr << "Page repeatedly failed to append! Skipping record!";
			}
		}
       }
	//We need to add the final page to the File. 
	f->AddPage(&p,globalPageIndex);
	// Done? Maybe? I dunno.*/
	//cout << "File size after load is: " << f.GetLength() << endl;

}

void HeapDB::Add(Record &rec){
//Okay, to Add, we must make sure that p = last page of the file.
//And then do shit to it. Namely, SCIENCE.
//Science is good.
//Wait. That doesn't make sense. We have to get the last page... and then add it? Because f.AddPage writes it out to file.
//But if we get the last page, does it zero it out to be nothing? MUST CHECK!

	int page = f->GetLength()-2;
	cout << "Page is "<<page<<endl;
	cout << "GetLength is " << f->GetLength() << endl;
	if(page < 0){ //File has nothing in it (i.e. GetLength returned 0, so page = -1)
		p.Append(&rec); //If the file has nothing in it, neither does the page, so it's a clean append.
		f->AddPage(&p,0); //We then add the page, and leave
		return;
	}
	cout << "Getting page " << endl;
	f->GetPage(&p,page); //If the file does have at least one page, we get it
	cout << "Page got" <<endl;
	if(p.Append(&rec) == 1){ //Now we test the append. If it goes through
		cout << "Appended to current page" <<endl;
		f->AddPage(&p,page); //We overwrite the file's page with the new one
		cout << "Page re-added" <<endl;
		return; //And then we leave
	}

	//If the page append doesn't go through, then we have to add a new page
	p.EmptyItOut(); //Clear the page
	p.Append(&rec); //Add the record
	cout << "Appending to new p age" << endl;
	page++; //Increment which page offset we're talking about
	cout << "Adding new page to file" <<endl;
	f->AddPage(&p, page); //Add the page, and then we out.
	cout << "Added" <<endl;
}

int HeapDB::GetNext(Record &fetch){
	//The first thing to do is fetch through the current page.
	//If the page returns 0, then we need to load the next page and get from there
	//If the GPI > f's size, then we've reached the end of the records

	if(p.GetFirst(&fetch) == 0){ //Check to see if anything is returned by our current page p
			globalPageIndex++; //Update page to the next one
		if(globalPageIndex < f->GetLength()-1){ //If nothing is returned, we check to see if p is the last page
			f->GetPage(&p,globalPageIndex);
			p.GetFirst(&fetch);
			return 1;
		}

		return 0;//No records left
	}

	return 1; 
}

int HeapDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){

	ComparisonEngine comp;

	int ret = GetNext(fetchme);

	if(ret == 0){ 
		return 0; //Nothing in the file, so nothing to do here!
	}	
	while(!comp.Compare (&fetchme, &literal, &cnf)){
		ret = GetNext(fetchme);

		if(ret == 0){ 
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
}


