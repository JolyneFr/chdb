#include "common.h"
#include "protocol.h"
#include "chdb_state_machine.h"

class value_entry {
public:
    value_entry() {}

    value_entry(const value_entry &entry) : value(entry.value) {}

    value_entry(int value_) : value(value_) {}

    int value;
};

/**
 * Storage layer for each shard. Support fault tolerance.
 * */
class shard_client {
public:
    shard_client(const int shard_id, const int port) : active(true),
                                                       shard_id(shard_id),
                                                       node(new rpc_node(port)) {
        this->store.resize(this->replica_num);
        // reg rpc handlers. You may add more handlers if necessary.
        this->node->reg(chdb_protocol::Dummy, this, &shard_client::dummy);
        this->node->reg(chdb_protocol::Put, this, &shard_client::put);
        this->node->reg(chdb_protocol::Get, this, &shard_client::get);
        // transaction related
        this->node->reg(chdb_protocol::Prepare, this, &shard_client::prepare);
        this->node->reg(chdb_protocol::Commit, this, &shard_client::commit);
        this->node->reg(chdb_protocol::Rollback, this, &shard_client::rollback);
        this->node->reg(chdb_protocol::CheckPrepareState, this, &shard_client::check_prepare_state);
    }

    ~shard_client();

    void bind_view_server(const int server_port) {
        this->node->bind_remote_node(server_port);
        this->view_server_port = server_port;
    }

    int dummy(chdb_protocol::operation_var var, int &r) {
        printf("Receive dummy Request! tx id:%d\n", var.tx_id);
        r = var.tx_id;
        return 0;
    }

    int put(chdb_protocol::operation_var var, int &r);

    int get(chdb_protocol::operation_var var, int &r);

    int prepare(chdb_protocol::prepare_var var, int &r);

    int commit(chdb_protocol::commit_var var, int &r);

    /**
     * Execute rollback according to `undo_logs`
     * */
    int rollback(chdb_protocol::rollback_var var, int &r);

    int check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r);

    void set_active(bool active) {
        // printf("active of shard %d is set to %d\n", shard_id, active);
        this->active = active;
    }

    /**
     * Random pick a new replica for this shard client
     * Only used in testcase
     * */
    int shuffle_primary_replica() {
        sync_backup();
        int next = this->primary_replica;
        while (next == this->primary_replica) {
            next = random() % this->store.size();
        }
        this->primary_replica = next;
        return next;
    }

    inline std::map<int, value_entry> &get_store() {
        return this->store[primary_replica];
    }

    enum tx_status {
        readonly = 0x3614,
        not_readonly,
        prepared,
        commited,
        aborted
    };

    bool active;
    int shard_id;
    rpc_node *node;
    std::vector<std::map<int, value_entry>> store;
    int view_server_port;
    
    int primary_replica = 0;
    int replica_num = 5;
    

private:
    /* recording tx status for 2-Phase-Commit */
    std::map<int, tx_status> status;
    /* for rollback */
    std::vector<chdb_log> undo_logs;
    /* the big lock */
    std::mutex mtx;

    std::map<int, int> tx_max_cmd_id;

    void sync_backup();
    bool tx_can_prepare(int tx_id);
    bool tx_can_commit(int tx_id);
    bool tx_can_continue(int tx_id);
};