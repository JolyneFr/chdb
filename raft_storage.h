#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <fstream>
#include <mutex>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void persist_metadata(int current_term, int voted_for);
    void cover_log(int prev_physical_idx, const std::vector<log_entry<command>> &log);
    void append_log(const std::vector<log_entry<command>> &log);
    void update_log(const std::vector<log_entry<command>> &log);
    void recover(int &term, int &voted_for, std::vector<log_entry<command>> &log);
    std::string get_snapshot_filename() { return snapshot_filename; }
private:
    std::mutex mtx;
    std::string metadata_filename;
    std::string log_filename;
    std::string snapshot_filename;
    std::string file_dir;
    std::vector<int> log_offset;
    int log_end;

    std::vector<log_entry<command>> recovered_log;
    int current_term;
    int voted_for;

    static constexpr const char* METADATA_FILENAME = "/metadata";
    static constexpr const char* LOG_FILENAME = "/log";
    static constexpr const char* SNAPSHOT_FILENAME = "/snapshot";

    bool touch_file();
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir): 
    metadata_filename(dir + METADATA_FILENAME), log_filename(dir + LOG_FILENAME), 
    snapshot_filename(dir + SNAPSHOT_FILENAME), file_dir(dir), log_offset({}), log_end(sizeof(int)) {
    // Your code here
    std::unique_lock<std::mutex> lock(mtx);
    // printf("stub to start creating storage\n");
    if (touch_file()) {
        /* new create node */
        current_term = -1;
        return;
    }

    /* found old node: start recovery */
    std::ifstream meta_file = std::ifstream(
        metadata_filename, std::ios_base::in
    );
    meta_file >> current_term >> voted_for;
    meta_file.close();
    // printf("found metadata in %s!\n", metadata_filename.c_str());

    /* read log and build offset table */
    int physical_log_cnt;
    std::ifstream log_file = std::ifstream(
        log_filename, std::ios_base::in | std::ios_base::binary
    );
    log_file.seekg(0, std::ios_base::beg);
    log_file.read((char *)(&physical_log_cnt), sizeof(int));
    // printf("%d logs found:\n", log_cnt);

    for (int i = 0; i < physical_log_cnt; ++i) {
        /* build offset table */
        log_offset.push_back(log_file.tellg());
        /* read entry data */
        int term, size;
        log_file.read((char *)(&term), sizeof(int));
        log_file.read((char *)(&size), sizeof(int));
        char *buf = new char[size];
        log_file.read(buf, size);
        command cmd = command();
        // printf("    at %ld: index = %ld term = %d ", (long)log_file.tellg() - (8 + size), recovered_log.size() + 1, term);
        cmd.deserialize(buf, size);
        delete buf;
        recovered_log.push_back(log_entry<command> {
            .term = term, .cmd = cmd
        });
    }
    log_end = log_file.tellg();
    log_file.close();
    
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   std::unique_lock<std::mutex> lock(mtx);
   remove(log_filename.c_str());
   remove(metadata_filename.c_str());
   remove(snapshot_filename.c_str());
   remove(file_dir.c_str());
}

template<typename command>
void raft_storage<command>::persist_metadata(int c_t, int v_f) {
    std::unique_lock<std::mutex> lock(mtx);
    std::ofstream meta_file = std::ofstream(
        metadata_filename, std::ios_base::out
    );
    meta_file << c_t << ' ' << v_f;
    meta_file.close();
}

template<typename command>
void raft_storage<command>::cover_log(int prev_physical_idx, const std::vector<log_entry<command>> &log) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream log_file = std::fstream(
        log_filename, 
        std::ios::out | std::ios::in | std::ios::binary
    );
    log_file.seekp(0, std::ios_base::beg);

    int new_size = prev_physical_idx + log.size() + 1;
    log_file.write((char *)(&new_size), sizeof(int));

    int start_offset = prev_physical_idx + 1 < static_cast<int>(log_offset.size()) 
        ? log_offset[prev_physical_idx + 1] : log_end;
    log_file.seekp(start_offset, std::ios_base::beg);
    log_offset.erase(log_offset.cbegin() + prev_physical_idx + 1, log_offset.cend());

    for (const auto &entry : log) {
        assert(entry.term != 0);
        // printf("at %d ", (int)log_file.tellp());
        log_offset.push_back(log_file.tellp());
        int size = entry.cmd.size();
        char *buf = new char[size];
        // printf("(cover)total %d, set log to %s: term = %d size = %d ", new_size, file_dir.c_str(), entry.term, size);
        entry.cmd.serialize(buf, size);
        log_file.write((char *)(&(entry.term)), sizeof(int));
        log_file.write((char *)(&(size)), sizeof(int));
        log_file.write(buf, size);
        delete buf;
    }
    log_end = log_file.tellp();
    log_file.close();
}

template<typename command>
void raft_storage<command>::append_log(const std::vector<log_entry<command>> &log) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream log_file = std::fstream(
        log_filename, 
        std::ios::out | std::ios::in | std::ios::binary
    );
    log_file.seekp(0, std::ios_base::beg);
    
    int new_size = log_offset.size() + log.size();
    log_file.write((char *)(&new_size), sizeof(int));

    log_file.seekp(log_end, std::ios_base::beg);

    for (const auto &entry : log) {
        assert(entry.term != 0);
        // printf("at %d ", (int)log_file.tellp());
        log_offset.push_back(log_file.tellp());
        int size = entry.cmd.size();
        char *buf = new char[size];
        // printf("(append)total %d, set log to %s: term = %d size = %d ", new_size, file_dir.c_str(), entry.term, size);
        entry.cmd.serialize(buf, size);
        log_file.write((char *)(&entry.term), sizeof(int));
        log_file.write((char *)(&size), sizeof(int));
        log_file.write(buf, size);
        delete buf;
    }
    log_end = log_file.tellp();
    log_file.close();
}

template<typename command>
void raft_storage<command>::update_log(const std::vector<log_entry<command>> &log) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream log_file = std::fstream(
        log_filename, 
        std::ios::out | std::ios::in | std::ios::binary
    );
    log_file.seekp(0, std::ios_base::beg);
    log_offset.clear();

    int total_size = static_cast<int>(log.size());
    log_file.write((char *)(&total_size), sizeof(int));

    for (const auto &entry : log) {
        assert(entry.term != 0);
        // printf("at %d ", (int)log_file.tellp());
        log_offset.push_back(log_file.tellp());
        int size = entry.cmd.size();
        char *buf = new char[size];
        // printf("(update)total %d, set log to %s: term = %d size = %d ", new_size, file_dir.c_str(), entry.term, size);
        entry.cmd.serialize(buf, size);
        log_file.write((char *)(&entry.term), sizeof(int));
        log_file.write((char *)(&size), sizeof(int));
        log_file.write(buf, size);
        delete buf;
    }
    log_end = log_file.tellp();
    log_file.close();
}

template<typename command>
void raft_storage<command>::recover(
    int &term, int &v_f, std::vector<log_entry<command>> &log) {
    std::unique_lock<std::mutex> lock(mtx);
    if (current_term != -1) {
        term = current_term;
        v_f = voted_for;
        log.insert(log.cend(), recovered_log.cbegin(), recovered_log.cend());
    }
}

template<typename command>
bool raft_storage<command>::touch_file() {
    int init_term = -1, init_vote = -1, init_size = 0;

    std::fstream meta_exist = std::fstream(
        metadata_filename, std::ios_base::in | std::ios_base::binary
    );
    std::fstream log_exist = std::fstream(
        log_filename, std::ios_base::in | std::ios_base::binary
    );
    bool new_create = !meta_exist.is_open() || !log_exist.is_open();
    meta_exist.close();
    log_exist.close();

    if (new_create) {
        std::ofstream meta_create = std::ofstream(
            metadata_filename, 
            std::ios_base::out | std::ios_base::trunc
        );
        std::ofstream log_create = std::ofstream(
            log_filename,
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc
        );
        meta_create << init_term << ' ' << init_vote;
        log_create.write((char *)(&init_size), sizeof(int));
        meta_create.close();
        log_create.close();
    }

    return new_create;
}

#endif // raft_storage_h