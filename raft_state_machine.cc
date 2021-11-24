#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    /* type + key_size + value_size + key + value */
    return 1 + 2 * sizeof(int) + key.size() + value.size();
}


void kv_command::serialize(char* buf, int sz) const {
    if (sz != size()) return;
    int offset = 0;
    int key_size = static_cast<int>(key.size());
    int value_size = static_cast<int>(value.size());
    buf[offset] = cmd_tp & 0xff; offset += 1;
    *((int *)(buf + offset)) = key_size; offset += sizeof(int);
    *((int *)(buf + offset)) = value_size; offset += sizeof(int);
    memcpy(buf + offset, key.c_str(), key_size); offset += key_size;
    memcpy(buf + offset, value.c_str(), value_size);
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    int offset = 0, key_size = 0, value_size = 0;
    cmd_tp = static_cast<command_type>(buf[offset]); offset += 1;
    key_size = *((int *)(buf + offset)); offset += sizeof(int);
    value_size = *((int *)(buf + offset)); offset += sizeof(int);
    assert(size == 1 + 2 * (int)sizeof(int) + key_size + value_size);
    key.assign(buf + offset, key_size); offset += key_size;
    value.assign(buf + offset, value_size);
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    m << static_cast<int>(cmd.cmd_tp) << cmd.key << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    int type;
    u >> type >> cmd.key >> cmd.value;
    cmd.cmd_tp = static_cast<kv_command::command_type>(type);
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    std::unique_lock<std::mutex> lock(mtx);
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> res_lock(kv_cmd.res->mtx);
    // Your code here:
    printf("apply log type: %d, key: %s, value: %s\n", kv_cmd.cmd_tp, kv_cmd.key.c_str(), kv_cmd.value.c_str());
    kv_map::iterator find_itr = store.find(kv_cmd.key);
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_GET: {
            printf("get lv pair %s\n", kv_cmd.key.c_str());
            if (find_itr != store.end()) {
                kv_cmd.res->value = find_itr->second;
                kv_cmd.res->succ = true;
            } else {
                kv_cmd.res->value = "";
                kv_cmd.res->succ = false;
            }
            break;
        }
        case kv_command::CMD_DEL: {
            if (find_itr != store.end()) {
                kv_cmd.res->value = find_itr->second;
                kv_cmd.res->succ = true;
                store.erase(find_itr);
            } else {
                kv_cmd.res->value = "";
                kv_cmd.res->succ = false;
            }
            break;
        }
        case kv_command::CMD_PUT: {
            if (find_itr != store.end()) {
                kv_cmd.res->value = find_itr->second;
                kv_cmd.res->succ = false;
            } else {
                kv_cmd.res->value = kv_cmd.value;
                kv_cmd.res->succ = true;
            }
            store[kv_cmd.key] = kv_cmd.value;
            break;
        }
        default: /* Do Nothing */ break;
    }
    kv_cmd.res->key = kv_cmd.key;
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    std::unique_lock<std::mutex> lock(mtx);
    /* using stringstream feels good! */
    std::stringstream buf;

    int map_size = static_cast<int>(store.size());
    buf.write((char *)(&map_size), sizeof(int));

    for (auto &key_value : store) {
        int key_size = static_cast<int>(key_value.first.size());
        int value_size = static_cast<int>(key_value.second.size());
        buf.write((char *)(&key_size), sizeof(int));
        buf.write(key_value.first.c_str(), key_size);
        buf.write((char *)(&value_size), sizeof(int));
        buf.write(key_value.second.c_str(), value_size);
    }

    std::string str = buf.str();
    return std::vector<char>(str.cbegin(), str.cend());
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    std::unique_lock<std::mutex> lock(mtx);
    // Your code here:
    std::string str(snapshot.cbegin() ,snapshot.cend());
    std::stringstream buf(str);
    store = kv_map();

    buf.seekg(0, std::ios::beg);
    int map_size;
    buf.read((char *)(&map_size), sizeof(int));
    printf("apply snapshot size = %d\n", map_size);

    for (int idx = 0; idx < map_size; ++idx) {
        int key_size, value_size;
        std::string key, value;

        buf.read((char *)(&key_size), sizeof(int));
        key.resize(key_size);
        buf.read(&key[0], key_size);
        buf.read((char *)(&value_size), sizeof(int));
        value.resize(value_size);
        buf.read(&value[0], value_size);

        store.insert(std::make_pair(key, value));
        printf("insert pair %s : %s\n", key.c_str(), value.c_str());
    }

}
