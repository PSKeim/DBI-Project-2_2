#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>

using namespace std;

class BigQ {

	File f; //Used to write stuff out to file
	Pipe *input; //Input pipe, used in thread
	Pipe *output; //Output pipe, used in thread
	OrderMaker &order;
	int runlen;
	vector<int> offsets; //Vector that holds the offsets to the runs.
	pthread_t worker;
	char *tmpFile;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	void FirstPhase();
	void SecondPhase();
	void SecondPhasev2();
	static void* WorkThread(void* args);
};

#endif
