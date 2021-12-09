// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

void
chfs_client::put_dirent(inum parent, dirent entry) 
{
    std::string buf;
    ec->get(parent, buf);
    buf.append(entry.name + "/" + 
               filename(entry.inum) + "/");
    ec->put(parent, buf);
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    extent_protocol::attr a;
    std::string buf;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    ec->get(ino, buf);
    buf.resize(size);
    ec->put(ino, buf);

release:
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found; 
    inum whatever;

    if (lookup(parent, name, found, whatever) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    printf("create fucking file: %s in parent %llu\n", name, parent);

    if (found) {
        printf("file/dir %s already exists in parent dir!\n", name);
        r = EXIST;
        goto release;
    }

    ec->create(extent_protocol::T_FILE, ino_out);
    put_dirent(parent, { std::string(name), ino_out });

release:
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found; 
    inum whatever;

    if (lookup(parent, name, found, whatever) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    if (found) {
        printf("file/dir %s already exists in parent dir!\n", name);
        r = EXIST;
        goto release;
    }

    ec->create(extent_protocol::T_DIR, ino_out);
    put_dirent(parent, { std::string(name), ino_out });

release:
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;
    std::list<dirent>::const_iterator c_itr;

    if (readdir(parent, list) != extent_protocol::OK) {
        r = IOERR;  /* legality has been checked by readdir */
        goto release;
    }

    c_itr = list.cbegin();
    while (c_itr != list.cend()) {
        if (!c_itr->name.compare(name)) {
            found = true;
            ino_out = c_itr->inum;
            goto release;
        }
        c_itr++;
    }
    found = false;

release:
    return r;
}

/*
 * my directory content format:
 * dir:
 *       filename-1/inum-1/
 *       filename-2/inum-2/
 *       ...
 */

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    extent_protocol::attr a;
    std::string buf;
    std::size_t prev, curr;
    struct chfs_client::dirent entry;

    /* check exist */
    if (ec->getattr(dir, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    /* check is dir: avoid reading failure */
    if (a.type != extent_protocol::T_DIR) {
        printf("Error creating file: inum %lld is not dir\n", dir);
        r = TYPERR;
        goto release;
    }

    ec->get(dir, buf);
    /* traverse: using 2 cursor */
    prev = 0;
    curr = buf.find("/");
    while (curr != std::string::npos) {
        /* read name */
        std::string name = buf.substr(prev, curr - prev);
        prev = curr + 1; curr = buf.find("/", prev);

        assert(curr != std::string::npos);
        /* read inum */
        std::string inum_str = buf.substr(prev, curr - prev);
        prev = curr + 1; curr = buf.find("/", prev);

        list.push_back({ name, n2i(inum_str) });
    }

    // printf("++++++++++++++++++++ dir %lld ++++++++++++++++\n", dir);
    // for (dirent e : list) {
    //     printf("filename:\n%s\nino: %lld\n", e.name.c_str(), e.inum);
    // }
    // printf("----------------------------------------------------------------\n");
    
release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;

    ec->get(ino, buf);
    /* illegal offset: too large */
    if (buf.size() < (std::size_t) off) {
        data = "";
        goto release;
    }
    /* illegal size: too long */
    if (buf.size() < off + size) {
        data = buf.substr(off);
        goto release;
    }
    data = buf.substr(off, size);

release:
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    std::size_t old_size;

    ec->get(ino, buf);
    old_size = buf.size();
    if (off + size > old_size) {
        /* write hole: fill it first, kinda tricky */
        buf.resize(off + size, '\0');
    }
    memcpy((void *)(buf.c_str() + off), data, size);

    ec->put(ino, buf);

    bytes_written = size;

    printf("system write off: %ld, size: %ld\n", off, size);
    printf("================after write: %lld==============\n", ino);
    printf("old: %ld, new: %ld\n", old_size, buf.size());

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found;
    inum target;
    std::string buf;
    std::size_t entry_head, entry_rear;

    if (lookup(parent, name, found, target) != chfs_client::OK) {
        r = IOERR;
        goto release;
    }

    if (!found) {
        printf("not found! file name %s is not in parent\n", name);
        r = NOENT;
        goto release;
    }

    if (ec->remove(target) != extent_protocol::OK) {
        printf("remove target file %lld error\n", target);
        r = IOERR;
        goto release;
    }

    /* update parent content: legality has been checked */
    ec->get(parent, buf);
    entry_head = buf.find(name);
    entry_rear = buf.find('/', entry_head);
    entry_rear = buf.find('/', entry_rear + 1);
    buf = buf.erase(entry_head, entry_rear - entry_head + 1);
    ec->put(parent, buf);

release:
    return r;
}

int
chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) 
{
    int r = OK;

    bool found;
    inum whatever;

    if (lookup(parent, name, found, whatever) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    if (found) {
        printf("file/dir %s already exists in parent dir!\n", name);
        r = EXIST;
        goto release;
    }

    ec->create(extent_protocol::T_SYMLINK, ino_out);
    put_dirent(parent, { std::string(name), ino_out });
    ec->put(ino_out, std::string(link));

release:
    return r;
}

int
chfs_client::readlink(inum ino, std::string &link) 
{
    int r = OK;

    if (ec->get(ino, link) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    } 

release:
    return r;
}

