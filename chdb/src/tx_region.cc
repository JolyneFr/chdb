#include "tx_region.h"


int tx_region::put(const int key, const int val) {
#ifdef RAFT_GROUP
    wait_all_done();
    chdb_command put_cmd(chdb_command::CMD_PUT, key, val, tx_id, cmd_count++);
    int retry = db->vserver->execute_command(put_cmd);
    while (retry == SHOULD_RETRY) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        retry = db->vserver->execute_command(put_cmd);
    }
    push_back_res(put_cmd.res);
    put_cache[key] = val;
    return 0;
#else   
    auto putop_var = chdb_protocol::operation_var {
        .tx_id = tx_id,
        .key = key,
        .value = val
    };
    int r;
    db->vserver->execute(key, chdb_protocol::Put, putop_var, r);
    put_cache[key] = val;
    return r;
#endif
}

int tx_region::get(const int key) {
#ifdef RAFT_GROUP
    /* if found in current tx_cache, just return it */
    auto cache_found = put_cache.find(key);
    if (cache_found != put_cache.end()) {
        return cache_found->second;
    }
    wait_all_done();
    int ans = -1;
    chdb_command get_cmd(chdb_command::CMD_GET, key, ans, tx_id, cmd_count++);
    int retry = db->vserver->execute_command(get_cmd);
    /* wait-die 2PL */
    while (retry == SHOULD_RETRY) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        retry = db->vserver->execute_command(get_cmd);
    }
    return get_cmd.res->value;
#else
    auto getop_var = chdb_protocol::operation_var {
        .tx_id = tx_id,
        .key = key,
        .value = -1
    };
    int r;
    db->vserver->execute(key, chdb_protocol::Get, getop_var, this, r);
    /* if found in current tx_cache, just return it */
    auto cache_found = put_cache.find(key);
    if (cache_found != put_cache.end()) {
        return cache_found->second;
    }
    return r;
#endif
}

int tx_region::tx_can_commit() {
#ifdef RAFT_GROUP
    wait_all_done();
#endif
    /* check all nodes */
    for (int idx = 1; idx <= db->vserver->shard_num(); ++idx) {
        int r;
        db->vserver->check_can_commit(idx, tx_id, r);
        if (r == RPC_FAIL) return chdb_protocol::prepare_not_ok;
    }
    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    /* maybe do nothing */
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
#ifdef RAFT_GROUP
    wait_all_done();
#endif
    /* phase 1: prepare */
    bool prepare_success = true;
    for (int idx = 1; idx <= db->vserver->shard_num(); ++idx) {
        int r;
        db->vserver->prepare(idx, tx_id, r);
        if (r == RPC_FAIL) prepare_success = false;
    }
    if (!prepare_success) {
        printf("tx[%d] prepare fail, turn to abort\n", tx_id);
        tx_abort(); return -1;
    }
    /* phase 2: must commit */
    std::list<int> uncommited_shard;
    for (int idx = 1; idx <= db->vserver->shard_num(); ++idx) {
        uncommited_shard.push_back(idx);
    }
    while (!uncommited_shard.empty()) {
        int cur_idx = uncommited_shard.front();
        uncommited_shard.pop_front();
        int r;
        db->vserver->commit(cur_idx, tx_id, r);
        if (r == RPC_FAIL) uncommited_shard.push_back(cur_idx);
    }
    printf("tx[%d] commit\n", tx_id);
    db->vserver->release_tx_locks(tx_id);
    return 0;
}

int tx_region::tx_abort() {
#ifdef RAFT_GROUP
    wait_all_done();
#endif
    for (int idx = 1; idx <= db->vserver->shard_num(); ++idx) {
        int r;
        db->vserver->rollback(idx, tx_id, r);
        // if (r != -1) printf("tx[%d] in %dth shard abort\n", tx_id, idx);
    }
    printf("tx[%d] abort\n", tx_id);
    db->vserver->release_tx_locks(tx_id);
    return 0;
}

void tx_region::push_back_res(std::shared_ptr<chdb_command::result> &res) {
    put_result.push_back(res);
}

void tx_region::wait_all_done() {
    for (auto res : put_result) {
        std::unique_lock<std::mutex> lock(res->mtx);
        if (!res->done) {
            res->cv.wait(lock);
        }
    }
}