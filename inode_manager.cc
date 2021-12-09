#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t bid = DBLOCK_HEAD(sb.nblocks);

  for (; bid < BLOCK_NUM; ++bid) {
    if (!using_blocks.test(bid)) {
      using_blocks.set(bid);
      return bid;
    }
  }

  printf("\tbm: no free block to alloc");
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  
  using_blocks.set(id, false);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

uint32_t
cal_block_num(uint32_t byte_size) {
  uint32_t full_block_n = byte_size / BLOCK_SIZE;
  uint32_t remain_byte_n = byte_size % BLOCK_SIZE;
  return full_block_n + (remain_byte_n != 0);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */

  static uint32_t inum = 1;
  struct inode *ino;

  for (uint32_t i = 0; i < INODE_NUM; ++i) {
    if ((ino = get_inode(inum)) == NULL) break;
    free(ino);
    inum = (inum + 1) % INODE_NUM;
  }

  // if (inum > INODE_NUM) {
  //   printf("\tim: no empty inode\n");
  //   return 0;
  // }
  // get_inode(inum) is NULL: empty inode
  ino = (struct inode *)malloc(sizeof(struct inode));
  ino->type = type;
  ino->size = 0;
  ino->atime = time(NULL);
  ino->mtime = time(NULL);
  ino->ctime = time(NULL);
  bzero(ino->blocks, sizeof(ino->blocks));

  put_inode(inum, ino);
  free(ino);  // remember to free all malloc blocks
  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode *ino = get_inode(inum);

  if (ino == NULL) {
    printf("\tim: inode is already free\n");
    return;
  }

  ino->type = 0;
  put_inode(inum, ino);
  free(ino);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  // printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    // printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

blockid_t
inode_manager::get_blockid(struct inode *ino, uint32_t n)
{
  /*
   * get blockid of nth block in inode,
   * guarantee this block is not free,
   * using inode* as param type to be faster
   */
  char buf[BLOCK_SIZE];

  if (ino == NULL) {
    printf("\tim: inode not exist\n");
    return 0;
  }

  if (n < NDIRECT) {
    return ino->blocks[n];
  } else if (n < MAXFILE) {
    bm->read_block(ino->blocks[NDIRECT], buf);
    return ((blockid_t *) buf)[n - NDIRECT];
  }

  printf("\tim: n out of range");
  return 0;
}

void 
inode_manager::alloc_inode_block(struct inode *ino, uint32_t n)
{
  /*
   * alloc nth block in inode,
   * using inode* as param type to be faster
   */
  char buf[BLOCK_SIZE];

  if (ino == NULL) {
    printf("\tim: inode not exist\n");
    return;
  }

  if (n < NDIRECT) {
    ino->blocks[n] = bm->alloc_block();
  } else if (n < MAXFILE) {
    if (!ino->blocks[NDIRECT]) {
      // alloc indirect block
      ino->blocks[NDIRECT]  = bm->alloc_block();
    }

    bm->read_block(ino->blocks[NDIRECT], buf);
    ((blockid_t *) buf)[n - NDIRECT] = bm->alloc_block();
    bm->write_block(ino->blocks[NDIRECT], buf);
  }
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  // printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  
  struct inode *ino = get_inode(inum);
  int full_block_n, remain_byte_n, i;

  if (ino == NULL) {
    printf("\tim: inode not exist\n");
  } else {
    *size = ino->size;
    *buf_out = (char *)malloc(*size);

    full_block_n = *size / BLOCK_SIZE;
    remain_byte_n = *size % BLOCK_SIZE;

    for (i = 0; i < full_block_n; ++i) {
      bm->read_block(get_blockid(ino, i), *buf_out + i * BLOCK_SIZE);
    }

    if (remain_byte_n) {
      char buf[BLOCK_SIZE];
      bm->read_block(get_blockid(ino, full_block_n), buf);
      memcpy(*buf_out + full_block_n * BLOCK_SIZE, 
              buf, remain_byte_n);
    }

    free(ino);
  }
  
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino = get_inode(inum);
  uint32_t prev_block_n, curr_block_n, i, 
          full_block_n, remain_byte_n;

  if (ino == NULL) {
    printf("\tim: inode not exist\n");
  } else {
    prev_block_n = cal_block_num(ino->size);
    curr_block_n = cal_block_num(size);

    ino->size = size;
    ino->atime = time(NULL);
    ino->mtime = time(NULL);
    ino->ctime = time(NULL);

    // alloc/free blocks when needed
    if (prev_block_n > curr_block_n) {
      for (i = curr_block_n; i < prev_block_n; ++i) {
        bm->free_block(get_blockid(ino, i));
      }
    } else if (prev_block_n < curr_block_n) {
      for (i = prev_block_n; i < curr_block_n; ++i) {
        alloc_inode_block(ino, i);
      }
    }

    // write data to inode data blocks
    full_block_n = size / BLOCK_SIZE;
    remain_byte_n = size % BLOCK_SIZE;

    for (i = 0; i < full_block_n; ++i) {
      blockid_t bid = get_blockid(ino, i);
      bm->write_block(bid, buf + i * BLOCK_SIZE);
    }

    if (remain_byte_n) {
      char in_buf[BLOCK_SIZE];
      memcpy(in_buf, buf + full_block_n * BLOCK_SIZE, remain_byte_n);
      bm->write_block(get_blockid(ino, full_block_n), in_buf);
    }

    // update inode in disk
    put_inode(inum, ino);
    free(ino);
  }
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode *ino;
  
  ino = get_inode(inum);
  if (ino == NULL) {
    // printf("\tim: inode not exist\n");
    a.type = 0; // not exist
    return;
  }

  a.type = ino->type;
  a.size = ino->size;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;

  free(ino);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  struct inode *ino = get_inode(inum);
  uint32_t block_n, i;

  if (ino == NULL) {
    printf("\tim: inode not exist\n");
    return;
  }

  // free all data blocks
  block_n = cal_block_num(ino->size);
  for (i = 0; i < block_n; ++i) {
    bm->free_block(get_blockid(ino, i));
  }

  // free indirect index block if necessary
  if (block_n > NDIRECT) {
    bm->free_block((blockid_t) ino->blocks[NDIRECT]);
  }

  free(ino);
  free_inode(inum);
}
