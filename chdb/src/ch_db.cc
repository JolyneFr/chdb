#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());
    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute_command(chdb_command &cmd) {
    int query_key = cmd.key;
    int term, index;
    if (!try_ask_lock(cmd.tx_id, query_key)) {
        return SHOULD_RETRY;
    }
    // if (cmd.cmd_tp == chdb_command::CMD_PUT) {
    //     printf("Send Put key %d, value %d\n", cmd.key, cmd.value);
    // }
    leader()->new_command(cmd, term, index);
    if (cmd.cmd_tp == chdb_command::CMD_GET) {
        /* Get should be sync */
        std::unique_lock<std::mutex> get_lock(cmd.res->mtx);
        if (!cmd.res->done) {
            cmd.res->cv.wait(get_lock);
        }
    } else {
       ASSERT(cmd.cmd_tp == chdb_command::CMD_PUT, "Invalid command type"); 
    } 
    return 0;
}

int view_server::check_can_commit(unsigned int client_idx, int tx_id, int &r) {
    ASSERT(client_idx >= 1 && (int)client_idx <= shard_num(), "Client Index out of bound.");
    int base_port = this->node->port();
    auto var = chdb_protocol::check_prepare_state_var { .tx_id = tx_id };
    return this->node->template call(base_port + client_idx, chdb_protocol::CheckPrepareState, var, r);
}

int view_server::prepare(unsigned int client_idx, int tx_id, int &r) {
    ASSERT(client_idx >= 1 && (int)client_idx <= shard_num(), "Client Index out of bound.");
    int base_port = this->node->port();
    auto var = chdb_protocol::prepare_var { .tx_id = tx_id };
    return this->node->template call(base_port + client_idx, chdb_protocol::Prepare, var, r);
}

int view_server::commit(unsigned int client_idx, int tx_id, int &r) {
    ASSERT(client_idx >= 1 && (int)client_idx <= shard_num(), "Client Index out of bound.");
    int base_port = this->node->port();
    auto var = chdb_protocol::commit_var { .tx_id = tx_id };
    return this->node->template call(base_port + client_idx, chdb_protocol::Commit, var, r);
}

int view_server::rollback(unsigned int client_idx, int tx_id, int &r) {
    ASSERT(client_idx >= 1 && (int)client_idx <= shard_num(), "Client Index out of bound.");
    int base_port = this->node->port();
    auto var = chdb_protocol::rollback_var { .tx_id = tx_id };
    return this->node->template call(base_port + client_idx, chdb_protocol::Rollback, var, r);
    
}

void view_server::release_tx_locks(int tx_id) {
    auto found = tx_locks.find(tx_id);
    if (found != tx_locks.end()) {
        for (auto hold_lock : found->second) {
            std::unique_lock<std::mutex> release_lock(global_mtx);
            hold_lock->unlock();
            lock_tx.erase(hold_lock);
        }
        tx_locks.erase(found);
        // printf("tx %d release all key\n", tx_id);
    }
}

bool view_server::try_ask_lock(int tx_id, int key) {
    /* init data-structure if necessary */
    std::unique_lock<std::mutex> acquire_lock(global_mtx);
    // printf("tx %d want key %d\n", tx_id, key);
    auto found_tx_locks = tx_locks.find(tx_id);
    if (found_tx_locks == tx_locks.end()) {
        tx_locks.emplace(tx_id, std::vector<std::mutex*>());
    }
    auto lock_found = key_lock.find(key);
    if (lock_found == key_lock.end()) {
        key_lock.emplace(key, new std::mutex());
    }
    /* actual logic */
    auto key_mtx = key_lock[key];
    auto tx_hold_lock = lock_tx.find(key_mtx);
    if (tx_hold_lock == lock_tx.end()) {
        /* no-one hold the key_lock */
        // if (!key_mtx->try_lock()) {
            
        //     return false;
        // }
        acquire_lock.unlock();
        key_mtx->lock();
        std::unique_lock<std::mutex> ds_update_lock(global_mtx);
        // printf("tx %d get (should) idle key %d\n", tx_id, key);
        tx_locks[tx_id].push_back(key_mtx);
        lock_tx.emplace(key_mtx, tx_id);
        return true;
    } else if (tx_hold_lock->second < tx_id) {
        /* current tx is elder: wait until get it */
        acquire_lock.unlock();
        key_mtx->lock();
        std::unique_lock<std::mutex> ds_update_lock(global_mtx);
        // printf("tx %d get lock for key %d\n", tx_id, key);
        tx_locks[tx_id].push_back(key_mtx);
        lock_tx.emplace(key_mtx, tx_id);
        return true;
    } else if (tx_hold_lock->second > tx_id) {
        /* current tx is younger: rollback and apply later */
        return false;
    } else {
        /* already hold the lock */
        return true;
    }
}


view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
#if !BIG_LOCK
    for (auto key_mtx : key_lock) {
        delete key_mtx.second;
    }
#endif
    delete this->node;

}