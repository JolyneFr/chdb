#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <random>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    int voted_for;  // candidateId that received vote in current term (or -1 if none)
    std::vector<log_entry<command>> physical_log;  // physical log entries
    std::vector<int> next_idx;  // for each server, index of the next log entry to send to that server
    std::vector<int> match_idx;  // for each server, index of highest log entry known to be replicated on server
    int commit_idx;  // index of highest log entry known to be committed
    int last_applied;  // index of highest log entry applied to state machine
    int vote_cnt;  // vote count in this term (leader only)

    struct raft_snapshot {
        int last_included_idx;
        int last_included_term;

        raft_snapshot(): last_included_idx(0), last_included_term(0) {}

        void recover(raft_storage<command> *storage) {
            std::fstream snapshot_exist = std::fstream(
                storage->get_snapshot_filename(), 
                std::ios_base::in | std::ios_base::binary
            );
            if (snapshot_exist.is_open()) {
                snapshot_exist.read((char *)(&last_included_idx), sizeof(int));
                snapshot_exist.read((char *)(&last_included_term), sizeof(int));
            }
            snapshot_exist.close();
        }

        /* set last_included_idx & term before calls save */
        void save(raft_storage<command> *storage, const std::vector<char> &chunk_data) {

            std::ofstream snapshot_file = std::ofstream(
                storage->get_snapshot_filename(),
                std::ios::out | std::ios::binary
            );
            snapshot_file.seekp(0, std::ios_base::beg);
            int data_size = static_cast<int>(chunk_data.size());

            snapshot_file.write((char *)(&last_included_idx), sizeof(int));
            snapshot_file.write((char *)(&last_included_term), sizeof(int));
            snapshot_file.write((char *)(&data_size), sizeof(int));
            snapshot_file.write(chunk_data.data(), data_size);

            snapshot_file.close();
        }

        std::vector<char> get_data(raft_storage<command> *storage) {

            std::fstream snapshot_file = std::fstream(
                storage->get_snapshot_filename(), 
                std::ios_base::in | std::ios_base::binary
            );
            std::vector<char> data;
            if (snapshot_file.is_open()) {
                snapshot_file.seekg(2 * sizeof(int), std::ios_base::beg);
                int data_size;
                snapshot_file.read((char *)(&data_size), sizeof(int));

                data.resize(data_size);
                snapshot_file.read(&(data[0]), data_size);                
            } else printf("FUUUUUUUUUUUUUUCKCCCCCCCCCCCCKKKKKKKKKKKKYOUUUUUUUUUUUUU\n");
            snapshot_file.close();
            return data;
        }

    } snapshot;

    std::chrono::system_clock::time_point last_received_RPC_time;
    std::chrono::milliseconds timeout;

private:
    static const int F_TIMEOUT_LB_MS = 200;
    static const int F_TIMEOUT_RB_MS = 350;
    static const int C_TIMEOUT_LB_MS = 450;
    static const int C_TIMEOUT_RB_MS = 600;


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void append_entries_wrapper(int target, append_entries_args<command> arg, bool is_heartbeat);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void install_snapshot_wrapper(int target);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    void start_election();
    void start_heartbeat();

    // other functions:
    std::chrono::milliseconds generate_timeout(raft_role role);
    std::string role_str(raft_role role);
    bool vaild_commit(int N, int total);
    void persist_meta();
    void change_to(raft_role role, int new_term);

    /* wrapper of log with snapshot */
    static const int MAX_PHYSICAL_LOG_SIZE = 50;
    int physical_log_size();
    int logical_log_size();
    int last_log_idx();
    int last_log_term();
    int physical_idx(int logical_idx);
    void append_log(const log_entry<command> &new_log);
    log_entry<command> &logical_log_entry(int logical_idx);
    int logical_log_term(int logical_idx);
    void take_snapshot();
    void erase_log_from(int first_logical_idx);
    void push_back_logs(const std::vector<log_entry<command>> &logs);
    std::vector<log_entry<command>> logs_from(int first_logical_idx);

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    voted_for(-1),
    physical_log({}),
    commit_idx(0),
    last_applied(0),
    vote_cnt(0)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Recover
    snapshot.recover(storage);
    storage->recover(current_term, voted_for, physical_log);
    commit_idx = last_applied = snapshot.last_included_idx;
    if (last_applied > 0) {
        state->apply_snapshot(snapshot.get_data(storage));
    }
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;

}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    
    // RAFT_LOG("start");
    last_received_RPC_time = std::chrono::system_clock::now();
    timeout = generate_timeout(raft_role::follower);

    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);

}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);

    if (role == raft_role::leader) {
        term = current_term;
        index = last_log_idx() + 1;
        log_entry<command> entry = { term, cmd };
        append_log(entry);
        storage->append_log({entry});
        // RAFT_LOG("new command at index %d", index);

        if (physical_log.size() > MAX_PHYSICAL_LOG_SIZE) {
            /* do snapshot operation */
            // RAFT_LOG("save in new command, size = %ld", physical_log.size());
            take_snapshot();
        }
        
        return true;
    } else return false;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    // RAFT_LOG("directly save");
    take_snapshot();
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    
    std::unique_lock<std::mutex> lock(mtx);

    reply.term = current_term;
    if (args.term < current_term) {
        reply.vote_granted = false;
        return 0;
    }
    if (args.term > current_term) {
        change_to(raft_role::follower, args.term);
    }
    
    /* check: candidate’s log is at least as up-to-date as receiver’s log */
    int last_log_idx = this->last_log_idx();
    int last_log_term = this->last_log_term();
    bool up_to_date = args.last_log_term > last_log_term 
        || (args.last_log_term == last_log_term && args.last_log_idx >= last_log_idx);
    // RAFT_LOG("candidate %d, votefor %d, up_to_date %d, last_log_idx %d", args.candidate_id, voted_for, up_to_date, last_log_idx);
    if ((voted_for == -1 || voted_for == args.candidate_id) && up_to_date) {
        last_received_RPC_time = std::chrono::system_clock::now();
        reply.vote_granted = true;
        voted_for = args.candidate_id;
        persist_meta();
        return 0;
    }
    /* default: not vote */
    reply.vote_granted = false;
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        change_to(raft_role::follower, reply.term);
        return;
    }
    if (role == raft_role::candidate) {
        if (reply.vote_granted) {
            vote_cnt++;
            if (vote_cnt > num_nodes() / 2) {
                // RAFT_LOG("comes to power!!!, log size = %ld", log.size() - 1);
                change_to(raft_role::leader, current_term);
            }
            return;
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {

    std::unique_lock<std::mutex> lock(mtx);

    reply.term = current_term;
    if (arg.term < current_term) {
        reply.success = false;
        return 0;
    }
    if (role != raft_role::follower || current_term < arg.term || voted_for != -1) {
        change_to(raft_role::follower, arg.term);
        // RAFT_LOG("%d becomes follower!, cmt_idx = %d, leader is %d", my_id, commit_idx, arg.leader_id);
        timeout = generate_timeout(raft_role::follower);
    }
    last_received_RPC_time = std::chrono::system_clock::now();

    /* checking log consistency */
    // RAFT_LOG("prev_log_idx = %d, last_included_idx = %d, physical = %ld, leader is %d, heartbeat? %ld", arg.prev_log_idx, snapshot.last_included_idx, physical_log.size(), arg.leader_id, arg.entries.size());
    if (last_log_idx() < arg.prev_log_idx || 
        arg.prev_log_idx < snapshot.last_included_idx || 
        logical_log_term(arg.prev_log_idx) != arg.prev_log_term) {
        // if (last_log_idx() >= arg.prev_log_idx) {
        //     RAFT_LOG("term inconsistency: idx: %d, local: %d, arg: %d", arg.prev_log_idx, logical_log_term(arg.prev_log_idx), arg.prev_log_term);
        // }
        reply.success = false;
        reply.term = current_term;
        return 0;
    }

    /* heartbeat */
    if (arg.entries.empty()) {
        /* apply commited log to state_machine */
        if (arg.leader_commit <= last_log_idx() && arg.leader_commit > commit_idx) {
            commit_idx = arg.leader_commit;
            // RAFT_LOG("heartbeat change commit_idx to %d", commit_idx);
        }
        reply.success = true;
        reply.term = current_term;
        return 0;
    }

    int log_idx = arg.prev_log_idx + 1;
    auto new_log_itr = arg.entries.cbegin();
    int follower_log_last_idx = last_log_idx();
    while (log_idx <= follower_log_last_idx && new_log_itr != arg.entries.cend()) {
        if (logical_log_term(log_idx) != new_log_itr->term) {
            /* conflict detected! */
            break;
        }
        // RAFT_LOG("what happened? we need to know!");
        log_idx++;
        new_log_itr++;
    }
    std::vector<log_entry<command>> new_logs(new_log_itr, arg.entries.cend());
    erase_log_from(log_idx);
    push_back_logs(new_logs);
    storage->cover_log(physical_idx(log_idx - 1), new_logs);

    if (physical_log.size() > MAX_PHYSICAL_LOG_SIZE) {
        /* do snapshot operation */
        // RAFT_LOG("save in append");
        take_snapshot();
    }

    if (arg.leader_commit > commit_idx) {
        int last_new_entry_idx = last_log_idx();
        commit_idx = arg.leader_commit < last_new_entry_idx ? 
                    arg.leader_commit : last_new_entry_idx;
    }

    reply.success = true;
    reply.term = current_term;
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        change_to(raft_role::follower, reply.term);
        return;
    }
    if (role == raft_role::leader) {
        if (reply.term == current_term && !arg.entries.empty()) {
            /* not a heartbeat */
            if (!reply.success) {
                /* log inconsistency: decrement nextIndex and retry */
                --next_idx[target];
                // RAFT_LOG("append_entry to %d fail, change next[] to %d", target, next_idx[target]);
                if (next_idx[target] <= snapshot.last_included_idx) {
                    /* can't aquire log before last_included_idx */
                    // RAFT_LOG("send snapshot rpc to %d, last include %d", target,  snapshot.last_included_idx);
                    install_snapshot_wrapper(target);
                } else {
                    append_entries_wrapper(target, arg, false);
                }
            } else {
                /* match */
                // RAFT_LOG("append entries to target %d, old_last = %d, entries_size = %ld", target, arg.prev_log_idx, arg.entries.size());
                int follower_last_log_idx = arg.prev_log_idx + arg.entries.size();
                match_idx[target] = follower_last_log_idx;
                next_idx[target] = follower_last_log_idx + 1;
            }
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    
    if (args.term < current_term) {
        reply.term = current_term;
        return 0;
    }

    // RAFT_LOG("snapshot from leader %d, apply state to %d, last_included %d, pysical size %ld", args.leader_id, args.last_included_idx, snapshot.last_included_idx, physical_log.size());

    if (role != raft_role::follower || current_term < args.term || voted_for != -1) {
        change_to(raft_role::follower, args.term);
        // RAFT_LOG("%d becomes follower!, cmt_idx = %d", my_id, commit_idx);
        timeout = generate_timeout(raft_role::follower);
    }
    last_received_RPC_time = std::chrono::system_clock::now();

    /* phase0: change meta data */
    int erase_num = args.last_included_idx - snapshot.last_included_idx;
    commit_idx = last_applied = args.last_included_idx;

    /* phase1: save dump data */
    snapshot.last_included_idx = args.last_included_idx;
    snapshot.last_included_term = args.last_included_term;
    snapshot.save(storage, args.data);

    /* phase2: erase physical log */
    if (erase_num <= static_cast<int>(physical_log.size()) && 
        logical_log_term(args.last_included_idx) == args.last_included_term) {
        physical_log.erase(physical_log.cbegin(), 
                        physical_log.cbegin() + erase_num);
    } else {
        /* inconsistent: clear all */
        physical_log.clear();
    }

    /* phase3: update persisted log */
    storage->update_log(physical_log);

    /* phase4: update state machine */
    state->apply_snapshot(args.data);
    
    reply.term = current_term;
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        change_to(raft_role::follower, reply.term);
    } else if (role == raft_role::leader) {
        // RAFT_LOG("install snapshot to %d successfully, last idx %d, term %d", target, arg.last_included_idx, arg.last_included_term);
        match_idx[target] = arg.last_included_idx;
        next_idx[target] = arg.last_included_idx + 1;
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::append_entries_wrapper(int target, append_entries_args<command> arg, bool is_heartbeat) {
    if (is_heartbeat) {
        arg.prev_log_idx = last_log_idx();
        arg.prev_log_term = last_log_term();
        arg.entries = std::vector<log_entry<command>>{};
    } else {
        arg.prev_log_idx = next_idx[target] - 1;
        arg.prev_log_term = logical_log_term(arg.prev_log_idx);
        arg.entries = logs_from(arg.prev_log_idx + 1);
        // RAFT_LOG("try append entries to target %d, prev_idx = %d, entries size = %ld", target, arg.prev_log_idx, arg.entries.size());
    }
    if (thread_pool->addObjJob(this, &raft::send_append_entries, target, arg) == 0) {
        RAFT_LOG("add heartbeat_task to target %d failed.", target);
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::install_snapshot_wrapper(int target) {
    /* assert(role == raft_role::leader) */
    auto arg = install_snapshot_args {
        .term = current_term,
        .leader_id = my_id,
        .last_included_idx = snapshot.last_included_idx,
        .last_included_term = snapshot.last_included_term,
        .data = snapshot.get_data(storage)
    };
    if (thread_pool->addObjJob(this, &raft::send_install_snapshot, target, arg) == 0) {
        RAFT_LOG("add install_snapshot_task to target %d failed.", target);
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (is_stopped()) return;
        // Your code here:
        auto current_time = std::chrono::system_clock::now();
        if (current_time - last_received_RPC_time < timeout) {
            goto next_iteration;
        }
        
        switch (role) {
            case raft_role::follower:
            case raft_role::candidate: {
                start_election();
                last_received_RPC_time = std::chrono::system_clock::now();
                timeout = generate_timeout(role);
                break;
            }
            case raft_role::leader: {
                /* heartbeat: leader keeping role until failure */
            }
        }
    next_iteration:
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (is_stopped()) return;
        // Your code here:
        if (role == raft_role::leader) {
            int total = num_nodes();
            int last_log_index = last_log_idx();
            for (int N = commit_idx + 1; N <= last_log_index; ++N) {
                if (vaild_commit(N, total)) {
                    commit_idx = N;
                }
            }

            auto arg = append_entries_args<command> {
                .term = current_term,
                .leader_id = my_id,
                .leader_commit = commit_idx
            };
            // RAFT_LOG("leader commit idx = %d", commit_idx);

            for (int target = 0; target < total; ++target) {
                if (target == my_id) continue;
                if (last_log_index >= next_idx[target]) {
                    if (next_idx[target] <= snapshot.last_included_idx) {
                        /* can't aquire log before last_included_idx */
                        // RAFT_LOG("send snapshot rpc to %d, last_included %d, matched %d", target, snapshot.last_included_idx, next_idx[target]);
                        install_snapshot_wrapper(target);
                    } else {
                        append_entries_wrapper(target, arg, false);
                    }
                    
                }
            }
        }

        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (is_stopped()) return;
        // Your code here:
        if (commit_idx > last_applied) {
            // RAFT_LOG("start to apply until %d", commit_idx);
            if (commit_idx > last_log_idx()) {
                // RAFT_LOG("commit_idx = %d, log_size = %ld", commit_idx, log.size() - 1);
                assert(0);
            }
        }
        for (int idx = last_applied + 1; idx <= commit_idx; ++idx) {
            // RAFT_LOG("apply log %d to state_machine, cmt_idx = %d, snapshot last_include %d", idx, commit_idx, snapshot.last_included_idx);
            state->apply_log(logical_log_entry(idx).cmd);
        }
        last_applied = commit_idx;
        // RAFT_LOG("last applyed = %d", last_applied);
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (is_stopped()) return;
        // Your code here:
        if (role == raft_role::leader) {
            // RAFT_LOG("leader ping, cmt_idx = %d", commit_idx);
            auto arg = append_entries_args<command> {
                .term = current_term,
                .leader_id = my_id,
                .leader_commit = commit_idx
            };

            int total = num_nodes();
            for (int target = 0; target < total; ++target) {
                if (target == my_id) continue;
                append_entries_wrapper(target, arg, true);
            }
        } else {
            // RAFT_LOG("role is %s", role_str(role).c_str());
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start_election() {
    /* candidate vote for self */
    // RAFT_LOG("from %s become candidate, start to ask vote!", role_str(role).c_str());
    change_to(raft_role::candidate, current_term + 1);
    vote_cnt = 1;

    auto args = request_vote_args {
        .term = current_term,
        .candidate_id = my_id,
        .last_log_idx = this->last_log_idx(),
        .last_log_term = this->last_log_term()
    };
    // RAFT_LOG("last log index = %d,term = %d", args.last_log_idx, args.last_log_term);

    int total = num_nodes();
    for (int target = 0; target < total; ++target) {
        if (target == my_id) continue;
        if (thread_pool->addObjJob(this, &raft::send_request_vote, target, args) == 0) {
            RAFT_LOG("add ask_vote_task to target %d failed.", target);
        }
    }    
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
std::chrono::milliseconds raft<state_machine, command>::generate_timeout(raft_role cur_role) {
    static std::mt19937 generater;
    static std::uniform_int_distribution<int> f_distribution(F_TIMEOUT_LB_MS, F_TIMEOUT_RB_MS);
    static std::uniform_int_distribution<int> c_distribution(C_TIMEOUT_LB_MS, C_TIMEOUT_RB_MS);
    if (cur_role == raft<state_machine, command>::raft_role::follower) {
        return std::chrono::milliseconds(f_distribution(generater));
    } else if (cur_role == raft<state_machine, command>::raft_role::candidate) {
        return std::chrono::milliseconds(c_distribution(generater));
    }
    /* leader should never generate timeout */
    assert(0);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::change_to(raft_role new_role, int new_term) {
    role = new_role;
    switch(new_role) {
        case raft_role::follower: {
            voted_for = -1;
            current_term = new_term;
            break;
        }
        case raft_role::candidate: {
            voted_for = my_id;
            current_term = new_term;
            break;
        }
        case raft_role::leader: {
            /* initialize leader state */
            int total = num_nodes();
            next_idx = std::vector<int>(total, last_log_idx() + 1);
            match_idx = std::vector<int>(total, 0);
            break;
        }
        default: assert(0);
    }
    persist_meta();
}

template<typename state_machine, typename command>
std::string raft<state_machine, command>::role_str(raft_role role) {
    switch (role) {
        case follower: return "follower";
        case candidate: return "candidate";
        case leader: return "leader";
        default: return "";
    }
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::vaild_commit(int N, int total) {
    int cnt = 1;
    for (int i = 0; i < total; ++i) {
        if (i == my_id) continue;
        if (match_idx[i] >= N) cnt++;
    }
    return cnt > (total / 2) && logical_log_term(N) == current_term;
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::persist_meta() {
    // RAFT_LOG("persist metadata term %d, vote %d, size %ld", current_term, voted_for, log.size() - 1)
    storage->persist_metadata(current_term, voted_for);
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::physical_log_size() {
    return physical_log.size();
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::logical_log_size() {
    return physical_log.size() + snapshot.last_included_idx;
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::last_log_idx() {
    return physical_log.size() + snapshot.last_included_idx;
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::physical_idx(int logical_idx) {
    return logical_idx - snapshot.last_included_idx - 1;
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::last_log_term() {
    return physical_log.empty() ? 
        snapshot.last_included_term : physical_log.back().term;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::append_log(const log_entry<command> &new_log) {
    physical_log.push_back(new_log);
}

template<typename state_machine, typename command>
log_entry<command> &raft<state_machine, command>::logical_log_entry(int logical_idx) {
    int physical_idx = logical_idx - snapshot.last_included_idx - 1;
    // RAFT_LOG("logical: %d, last_snapshot: %d", logical_idx, snapshot.last_included_idx);
    assert(physical_idx >= 0);
    return physical_log[physical_idx];
}

template<typename state_machine, typename command>
int raft<state_machine, command>::logical_log_term(int logical_idx) {
    int physical_idx = logical_idx - snapshot.last_included_idx - 1;
    assert(physical_idx >= -1);
    return physical_idx == -1 ? 
        snapshot.last_included_term : physical_log[physical_idx].term;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::take_snapshot() {
    if (snapshot.last_included_idx >= last_applied) {
        return;
    }
    // RAFT_LOG("take a snapshot!");
    /* phase1: save dump data */
    int erase_num = last_applied - snapshot.last_included_idx;
    int new_last_included_idx = last_applied;
    int new_last_included_term = logical_log_term(last_applied);
    std::vector<char> dump_data = state->snapshot();
    snapshot.last_included_idx = new_last_included_idx;
    snapshot.last_included_term = new_last_included_term;
    snapshot.save(storage, dump_data);

    /* phase2: erase physical log */
    physical_log.erase(physical_log.cbegin(), 
                    physical_log.cbegin() + erase_num);

    /* phase3: update persisted log */
    storage->update_log(physical_log);
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::erase_log_from(int first_logical_idx) {
    int first_physical_idx = first_logical_idx - snapshot.last_included_idx - 1;
    physical_log.erase(
        physical_log.cbegin() + first_physical_idx, physical_log.cend()
    );
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::push_back_logs(
    const std::vector<log_entry<command>> &logs) {
    physical_log.insert(physical_log.cend(), logs.cbegin(), logs.cend());
}

template<typename state_machine, typename command>
std::vector<log_entry<command>> raft<state_machine, command>::logs_from(int first_logical_idx) {
    int first_physical_idx = first_logical_idx - snapshot.last_included_idx - 1;
    return std::vector<log_entry<command>>(
        physical_log.cbegin() + first_physical_idx, physical_log.cend()
    );
}

#endif // raft_h