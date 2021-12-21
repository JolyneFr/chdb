#ifndef db_server_h
#define db_server_h

#include "common.h"
#include "shard_client.h"


using shard_dispatch = int (*)(int key, int shard_num);
using chdb_raft = raft<chdb_state_machine, chdb_command>;
using chdb_raft_group = raft_group<chdb_state_machine, chdb_command>;

/**
 * Master node
 * */
class view_server {
public:
    shard_dispatch dispatch;            /* Dispatch requests to the target shard */
    rpc_node *node;
    chdb_raft_group *raft_group;

    view_server(const int base_port,
                shard_dispatch dispatch,
                const int num_raft_nodes = 3) :
            dispatch(dispatch),
            node(new rpc_node(base_port)) {
#if RAFT_GROUP
        raft_group = new chdb_raft_group(num_raft_nodes);
        init_state_machine();
#endif
    };

    chdb_raft *leader() const {
        int leader = this->raft_group->check_exact_one_leader();
        return this->raft_group->nodes[leader];
    }

    void init_state_machine() {
        int size = this->raft_group->states.size();
        for (int raft_idx = 0; raft_idx < size; ++raft_idx) {
            auto chdb_sm = dynamic_cast<chdb_state_machine*>(this->raft_group->states[raft_idx]);
            chdb_sm->set_rpc_node(node);
            chdb_sm->set_dispatch(dispatch);
        }
    }


    /**
     * Add the shard client for rpc communication
     * */
    int add_shard_client(shard_client *shard) {
        int port = shard->node->port();
        int view_server_port = this->node->port();
        shard->bind_view_server(view_server_port);
        return this->node->bind_remote_node(port);
    }

    /**
     * Shard num that the view_server manage now
     * */
    int shard_num() const {
        return this->node->rpc_clients.size();
    }

    /**
     * Dispatch the request to specific shard client(s)
     * Sync return when use normal view server (single node)
     * Async return when use raft group vie server, since the command log should be distributed first.
     * */
    int
    execute(unsigned int query_key,
            unsigned int proc,
            const chdb_protocol::operation_var &var,
            int &r);

    /* dispatch chdb_command when RAFT_GROUP is true: more simple */
    int execute_command(chdb_command &cmd);

    int check_can_commit(unsigned int client_idx, int tx_id, int &r);

    int prepare(unsigned int client_idx, int tx_id, int &r);

    int commit(unsigned int client_idx, int tx_id, int &r);

    int rollback(unsigned int client_idx, int tx_id, int &r);

    void release_tx_locks(int tx_id);

    ~view_server();

private:
    /* this is not big-lock: only used when acqiuring 2PL */
    std::mutex global_mtx;
    /* data structure for impl 2-Phase-Lock */
    std::map<int, std::mutex*> key_lock;
    std::map<int, std::vector<std::mutex*>> tx_locks;
    std::map<std::mutex*, int> lock_tx;

    /* impl Wait-Die 2-phase-lock */
    bool try_ask_lock(int tx_id, int key);

};


/*
 * chdb: One KV storage
 * */
class chdb {
public:
    chdb(const int shard_num, const int cluster_port, shard_dispatch dispatch = default_dispatch)
            : max_tx_id(0),
              vserver(new view_server(cluster_port, dispatch)) {
        for (int i = 1; i <= shard_num; ++i) {
            shard_client *shard = new shard_client(i, i + cluster_port);
            vserver->add_shard_client(shard);
            this->shards.push_back(shard);
        }
    }

    ~chdb() {
        for (auto &shard: shards) delete shard;
        delete vserver;
    }

    void set_shard_down(const int offset) {
        const int len = shards.size();
        assert(len > 0);
        shards[offset % len]->set_active(false);
    }

    void set_shards_down(const std::set<int> shards_offset) {
        const int len = shards.size();
        assert(len > 0);

        for (auto offset: shards_offset) {
            shards[offset % len]->set_active(false);
        }
    }

    void set_shards_up(const std::set<int> shards_offset) {
        const int len = shards.size();
        assert(len > 0);

        for (auto offset: shards_offset) {
            shards[offset % len]->set_active(true);
        }
    }

    /**
     * Generate one unique transaction id
     * */
    int next_tx_id() {
        int res;
        {
            tx_id_mtx.lock();
            res = max_tx_id++;
            tx_id_mtx.unlock();
        }
        return res;
    }

    int max_tx_id;
    view_server *vserver;
    std::vector<shard_client *> shards;
    std::mutex tx_id_mtx;

private:
    static int default_dispatch(const int key, int shard_num) {
        int shard_offset = key % shard_num;
        if (0 == shard_offset)++shard_offset;
        return shard_offset;
    }
};

#endif // db_server_h
