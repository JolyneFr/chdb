#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    int term;
    int candidate_id;
    int last_log_idx;
    int last_log_term;
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    int term;
    bool vote_granted;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    int term;
    command cmd;
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    m << entry.term << entry.cmd;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    u >> entry.term >> entry.cmd;
    return u;
}

template<typename command>
class append_entries_args {
public:
    int term;
    int leader_id;
    int prev_log_idx;
    int prev_log_term;
    std::vector<log_entry<command>> entries;
    int leader_commit;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    m << args.term << args.leader_id << args.prev_log_idx 
        << args.prev_log_term << args.entries << args.leader_commit;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    u >> args.term >> args.leader_id >> args.prev_log_idx 
        >> args.prev_log_term >> args.entries >> args.leader_commit;
    return u;
}

class append_entries_reply {
public:
    int term;
    bool success;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h