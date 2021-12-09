#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		int taskType;
		int index;
		string filename;
	};

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};

marshall &operator<< (marshall &m, const mr_protocol::AskTaskResponse &resp) {
	m << resp.taskType << resp.index << resp.filename;
	return m;
}

unmarshall &operator>> (unmarshall &u, mr_protocol::AskTaskResponse &resp) {
	int t, i;
	string f;
	u >> t >> i >> f;
	resp.taskType = t;
	resp.index = i;
	resp.filename = f;
	return u;
}

#endif

