#include "chdb_state_machine.h"

chdb_command::chdb_command() : chdb_command(CMD_NONE, 0, 0, -1, -1) { }

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id, const int cmd_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), cmd_id(cmd_id), res(std::make_shared<result>()) {
    res->start = std::chrono::system_clock::now();
    res->tp = tp;
    res->key = key;
    res->tx_id = tx_id;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), cmd_id(cmd.cmd_id), res(cmd.res) {}


void chdb_command::serialize(char *buf, int sz) const {
    if (sz != size()) return;
    int offset = 0;
    *((int *)(buf + offset)) = tx_id; offset += sizeof(int);
    *((int *)(buf + offset)) = key; offset += sizeof(int);
    *((int *)(buf + offset)) = value; offset += sizeof(int);
    *((int *)(buf + offset)) = cmd_id; offset += sizeof(int);
    *((int *)(buf + offset)) = static_cast<int>(cmd_tp);
}

void chdb_command::deserialize(const char *buf, int sz) {
    if (sz != size()) return;
    int offset = 0;
    tx_id = *((int *)(buf + offset)); offset += sizeof(int);
    key = *((int *)(buf + offset)); offset += sizeof(int);
    value = *((int *)(buf + offset)); offset += sizeof(int);
    cmd_id = *((int *)(buf + offset)); offset += sizeof(int);
    cmd_tp = static_cast<command_type>(*((int *)(buf + offset)));
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    m << cmd.tx_id << cmd.key << cmd.value << cmd.cmd_id << static_cast<int>(cmd.cmd_tp);
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    int type;
    u >> cmd.tx_id >> cmd.key >> cmd.value >> cmd.cmd_id >> type;
    cmd.cmd_tp = static_cast<chdb_command::command_type>(type);
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    std::unique_lock<std::mutex> lock(mtx);
    chdb_command &chdb_cmd = dynamic_cast<chdb_command&>(cmd);
    std::unique_lock<std::mutex> res_lock(chdb_cmd.res->mtx);
    /* do the op */
    int base_port = this->node->port();
    int shard_num = this->node->rpc_clients.size();
    int shard_offset = this->dispatch(chdb_cmd.key, shard_num);
    auto var = chdb_protocol::operation_var {
        .tx_id = chdb_cmd.tx_id,
        .key = chdb_cmd.key,
        .value = chdb_cmd.value,
        .cmd_id = chdb_cmd.cmd_id
    };
    int r;
    switch (chdb_cmd.cmd_tp) {
        case chdb_command::CMD_GET: {
            node->template call(base_port + shard_offset, chdb_protocol::Get, var, r);
            // printf("Get key %d, value %d\n", var.key, r);
            chdb_cmd.res->value = r;
            break;
        }
        case chdb_command::CMD_PUT: {
            node->template call(base_port + shard_offset, chdb_protocol::Put, var, r);
            // printf("Put key %d, value %d, cmd-id %d\n", var.key, var.value, chdb_cmd.cmd_id);
            chdb_cmd.res->value = r;
            break;
        }
        default: ASSERT(0, "Invalid command type"); break;
    }
    chdb_cmd.res->key = chdb_cmd.key;
    chdb_cmd.res->done = true;
    chdb_cmd.res->cv.notify_all();
}