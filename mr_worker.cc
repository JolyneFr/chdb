#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <algorithm>

#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
	static const string banList = string(",.;:'?!\n\t\"()-#&*@_[] 1234567890", 31);
    vector<KeyVal> intermediate;
    size_t head = 0, rear = 0;

    rear = content.find_first_of(banList, head);
    while (rear != string::npos) {
        if (head != rear) {
            string key = content.substr(head, rear - head);
            intermediate.push_back({ .key = key, .val = "1" });
        }
        head = rear + 1;
        rear = content.find_first_of(banList, head);
    }

    return intermediate;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
	ostringstream ost;
    ost << values.size();
    return ost.str();
}

void readContent(const string &filename, string &ret) {
	ifstream in(filename);
	in.seekg(0, in.end);
	size_t length = in.tellg();
	in.seekg(0, in.beg);

	char *buf = new char[length];
	in.read(buf, length);
	ret.assign(buf, length);

	delete buf;
	in.close();
}

string getIFilename(int mIndex, int rIndex) {
	stringstream ss;
	ss << "MR-" << mIndex << "-" << rIndex;
	return ss.str();
}

string getOFilename(int rIndex) {
	stringstream ss;
	ss << "mr-out-" << rIndex;
	return ss.str();
}

void scanDir(const string &path, int rIndex, vector<ifstream*> &matchedFiles) {
	DIR *dir;
	struct dirent *rent;
	dir = opendir(path.c_str());

	while((rent = readdir(dir))){
		string filename = string(rent->d_name);
		/* not an intermediate file */
		if (filename.substr(0, 2).compare("MR")) 
			continue;

		size_t pos = filename.find_last_of('-') + 1;
		if (atoi(filename.c_str() + pos) == rIndex) {
			ifstream *fin = new ifstream(path + "/" + filename);
			matchedFiles.push_back(fin);
		}
	}

	closedir(dir);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const string &filename);
	void doReduce(int index);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const string &filename)
{
	// Lab2: Your code goes here.
	string content;
	readContent(filename, content);

	vector<KeyVal> mapResult = mapf(filename, content);

	vector<ofstream*> intermediateFiles;
	for (int rIndex = 0; rIndex < REDUCER_COUNT; ++rIndex) {
		string intermediateFilename = getIFilename(index, rIndex);
		ofstream *fout = new ofstream(basedir + "/" + intermediateFilename);
		intermediateFiles.push_back(fout);
	}

	auto hashf = hash<string>{};
	for (const KeyVal &kv : mapResult) {
		int rIndex = hashf(kv.key) % REDUCER_COUNT;
		ofstream *fout = intermediateFiles[rIndex];
		(*fout) << kv.key << " " << kv.val << "\n";
	}

	for (ofstream *fout : intermediateFiles) {
		fout->close();
		delete fout;
	}
}

void Worker::doReduce(int index)
{
	// Lab2: Your code goes here.
	vector<ifstream*> matchedFiles;
	scanDir(basedir, index, matchedFiles);

	vector<KeyVal> intermediate;
	for (ifstream *fin : matchedFiles) {
		string line, key, val;
		while (getline(*fin, line)) {
			stringstream ss(line);
			ss >> key >> val;
			intermediate.push_back({ .key = key, .val = val });
		}

		fin->close();
		delete fin;
	}

	sort(intermediate.begin(), intermediate.end(),
    	[] (KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});

	string outputFilename = getOFilename(index);
	ofstream fout(basedir + "/" + outputFilename);
	for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector < string > values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = reducef(intermediate[i].key, values);
        fout << intermediate[i].key << " " << output << "\n";

        i = j;
    }
	fout.close();

}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab2: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		int whatever = 114514;
		mr_protocol::AskTaskResponse reply;

		mr_protocol::status ret = this->cl->call(mr_protocol::asktask, whatever, reply);
		if (ret != mr_protocol::OK) {
			fprintf(stderr, "ask task failed\n");
			exit(-1);
		}

		switch (reply.taskType) {
			case mr_tasktype::MAP: {
				doMap(reply.index, reply.filename);
				doSubmit(mr_tasktype::MAP, reply.index);
				break;
			}
			case mr_tasktype::REDUCE: {
				doReduce(reply.index);
				doSubmit(mr_tasktype::REDUCE, reply.index);
				break;
			}
			case mr_tasktype::NONE: {
				usleep(500 * 1000);
			}
		}
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

