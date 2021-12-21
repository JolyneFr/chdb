#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    /* avoid multi-command by raft */
    if (tx_max_cmd_id.count(var.tx_id) && tx_max_cmd_id[var.tx_id] >= var.cmd_id) return 0;
    tx_max_cmd_id[var.tx_id] = var.cmd_id;

    /* validate tx */
    // printf("TRY: tx[%d] shard %d put [%d]: %d\n", var.tx_id, shard_id, var.key, var.value);
    if (!tx_can_continue(var.tx_id)) {
        printf("Prepared or commited tx can't put value\n");
        r = RPC_FAIL;
        return 1;
    }
    status[var.tx_id] = tx_status::not_readonly;
    if (!active) { r = RPC_FAIL; return 1; }
    /* do actual put */
    auto log_entry = chdb_log {
        .tx_id = var.tx_id,
        .key = var.key,
        .new_v = var.value 
    };
    auto found_key = get_store().find(var.key);
    if (found_key != get_store().end()) {
        log_entry.old_v = found_key->second.value;
        log_entry.is_old_empty = false;
    } else {
        log_entry.old_v = NOT_FOUND;
        log_entry.is_old_empty = true;
    }
    get_store()[var.key] = value_entry(var.value);
    // printf("tx[%d] shard %d put [%d]: %d\n", var.tx_id, shard_id, var.key, var.value);
    undo_logs.push_back(log_entry);
    r = var.tx_id;
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    /* avoid multi-command by raft */
    if (tx_max_cmd_id.count(var.tx_id) && tx_max_cmd_id[var.tx_id] > var.cmd_id) return 0;
    tx_max_cmd_id[var.tx_id] = var.cmd_id;
    
    /* validate tx */
    // printf("TRY: tx[%d] shard %d get [%d]\n", var.tx_id, shard_id, var.key);
    if (!status.count(var.tx_id)) {
        status[var.tx_id] = tx_status::readonly;
    } else if (!tx_can_continue(var.tx_id)) {
        // printf("Prepared or commited tx can't get value\n");
        r = RPC_FAIL; return 1;
    }
    auto found_key = get_store().find(var.key);
    if (found_key != get_store().end()) {
        r = found_key->second.value;
    } else r = NOT_FOUND;
    // printf("tx[%d] shard %d get [%d]: %d\n", var.tx_id, shard_id, var.key, r);
    return 0;
    
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    if (tx_can_commit(var.tx_id)) {
        status[var.tx_id] = tx_status::commited;
        r = var.tx_id;
        return 0;
    } else {
        printf("Can't commit tx %d\n", var.tx_id);
        r = RPC_FAIL; return 1;
    }
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    auto found_status = status.find(var.tx_id);
    if (found_status != status.end() && 
        found_status->second == tx_status::commited) {
        printf("Can't rollback commited tx %d\n", var.tx_id);
        r = RPC_FAIL; return 1;
    }
    status[var.tx_id] = aborted;
    auto r_itr = undo_logs.rbegin();
    /* if other tx put value to key after var.tx, then it's confirmed */
    std::set<int> confirmed_key;
    while (r_itr != undo_logs.rend()) {
        if (r_itr->tx_id == var.tx_id && !confirmed_key.count(r_itr->key)) {
            /* can undo */
            ASSERT(get_store()[r_itr->key].value == r_itr->new_v, "Ah-Oh, something bad happend.");
            if (r_itr->is_old_empty) get_store().erase(r_itr->key);
            else get_store()[r_itr->key].value = r_itr->old_v;
        } else if (r_itr->tx_id != var.tx_id) {
            /* confirmed key */
            confirmed_key.insert(r_itr->key);
        }
        r_itr++;
    }
    r = var.tx_id;
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    r = tx_can_prepare(var.tx_id) ? 1 : RPC_FAIL;
    // printf("tx[%d] prepare state in shard %d is %d\n", var.tx_id, shard_id, r);
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    if (tx_can_prepare(var.tx_id)) {
        status[var.tx_id] = tx_status::prepared;
        r = var.tx_id; 
        return 0;
    } else {
        printf("Can't prepare not normal tx %d\n", var.tx_id);
        r = RPC_FAIL; return 1;
    }
}

void shard_client::sync_backup() {
    for (int idx = 0; idx < replica_num; ++idx) {
        if (idx == primary_replica) continue;
        store[idx] = store[primary_replica];
    }
}

bool shard_client::tx_can_prepare(int tx_id) {
    auto found_status = status.find(tx_id);
    bool result = found_status == status.end() || 
        found_status->second == tx_status::readonly ||
        (found_status->second == tx_status::not_readonly && active);
    if (found_status == status.end()) {
        status[tx_id] = tx_status::readonly;
    }
    return result;
}

bool shard_client::tx_can_commit(int tx_id) {
    auto found_status = status.find(tx_id);
    return found_status != status.end() &&
        found_status->second == tx_status::prepared;
}

bool shard_client::tx_can_continue(int tx_id) {
    auto found_status = status.find(tx_id);
    // if (found_status != status.end()) {
    //     printf("tx[%d] status is %d\n", tx_id, found_status->second);
    // }
    return found_status == status.end() || 
        (found_status->second != tx_status::prepared &&
        found_status->second != tx_status::commited &&
        found_status->second != tx_status::aborted);
}


shard_client::~shard_client() {
    delete node;
}