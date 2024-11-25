#include <tree/btree.hpp>

ExecContext ExecContext::global_ctx;

ExecContext &ExecContext::getGlobalContext() { return global_ctx; }

BufferManager::BufferManager()
    : virtSize(envOr("VIRTGB", 16) * gb), physSize(envOr("PHYSGB", 4) * gb),
      virtCount(virtSize / pageSize), physCount(physSize / pageSize),
      residentSet(physCount) {
  assert(virtSize >= physSize);
  const char *path = getenv("BLOCK") ? getenv("BLOCK") : "/tmp/bm";
  blockfd = open(path, O_RDWR | O_DIRECT, S_IRWXU);
  if (blockfd == -1) {
    cerr << "cannot open BLOCK device '" << path << "'" << endl;
    exit(EXIT_FAILURE);
  }
  u64 virtAllocSize = virtSize + (1 << 16); // we allocate 64KB extra to prevent
                                            // segfaults during optimistic reads

  virtMem = (Page *)mmap(NULL, virtAllocSize, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);

  pageState = (PageState *)allocHuge(virtCount * sizeof(PageState));
  for (u64 i = 0; i < virtCount; i++)
    pageState[i].init();
  if (virtMem == MAP_FAILED)
    die("mmap failed");

  libaioInterface.reserve(maxWorkerThreads);
  for (unsigned i = 0; i < maxWorkerThreads; i++)
    libaioInterface.emplace_back(LibaioInterface(blockfd, virtMem));

  physUsedCount = 0;
  allocCount = 1; // pid 0 reserved for meta data
  readCount = 0;
  writeCount = 0;
  batch = envOr("BATCH", 64);

  cerr << "vmcache "
       << "blk:" << path << " virtgb:" << virtSize / gb
       << " physgb:" << physSize / gb << " exmap:" << false << endl;
}

void BufferManager::ensureFreePages() {
  if (physUsedCount >= physCount * 0.95)
    evict();
}

// allocated new page and fix it
Page *BufferManager::allocPage() {
  physUsedCount++;
  ensureFreePages();
  u64 pid = allocCount++;
  if (pid >= virtCount) {
    cerr << "VIRTGB is too low" << endl;
    exit(EXIT_FAILURE);
  }
  u64 stateAndVersion = getPageState(pid).stateAndVersion;
  bool succ = getPageState(pid).tryLockX(stateAndVersion);
  assert(succ);
  residentSet.insert(pid);

  virtMem[pid].dirty = true;

  return virtMem + pid;
}

void BufferManager::handleFault(PID pid) {
  physUsedCount++;
  ensureFreePages();
  readPage(pid);
  residentSet.insert(pid);
}

Page *BufferManager::fixX(PID pid) {
  PageState &ps = getPageState(pid);
  for (u64 repeatCounter = 0;; repeatCounter++) {
    u64 stateAndVersion = ps.stateAndVersion.load();
    switch (PageState::getState(stateAndVersion)) {
    case PageState::Evicted: {
      if (ps.tryLockX(stateAndVersion)) {
        handleFault(pid);
        return virtMem + pid;
      }
      break;
    }
    case PageState::Marked:
    case PageState::Unlocked: {
      if (ps.tryLockX(stateAndVersion))
        return virtMem + pid;
      break;
    }
    }
    yield(repeatCounter);
  }
}

Page *BufferManager::fixS(PID pid) {
  PageState &ps = getPageState(pid);
  for (u64 repeatCounter = 0;; repeatCounter++) {
    u64 stateAndVersion = ps.stateAndVersion;
    switch (PageState::getState(stateAndVersion)) {
    case PageState::Locked: {
      break;
    }
    case PageState::Evicted: {
      if (ps.tryLockX(stateAndVersion)) {
        handleFault(pid);
        ps.unlockX();
      }
      break;
    }
    default: {
      if (ps.tryLockS(stateAndVersion))
        return virtMem + pid;
    }
    }
    yield(repeatCounter);
  }
}

void BufferManager::unfixS(PID pid) { getPageState(pid).unlockS(); }

void BufferManager::unfixX(PID pid) { getPageState(pid).unlockX(); }

void BufferManager::readPage(PID pid) {
  int ret = pread(blockfd, virtMem + pid, pageSize, pid * pageSize);
  assert(ret == pageSize);
  readCount++;
}

void BufferManager::evict() {
  vector<PID> toEvict;
  toEvict.reserve(batch);
  vector<PID> toWrite;
  toWrite.reserve(batch);

  // 0. find candidates, lock dirty ones in shared mode
  while (toEvict.size() + toWrite.size() < batch) {
    residentSet.iterateClockBatch(batch, [&](PID pid) {
      PageState &ps = getPageState(pid);
      u64 v = ps.stateAndVersion;
      switch (PageState::getState(v)) {
      case PageState::Marked:
        if (virtMem[pid].dirty) {
          if (ps.tryLockS(v))
            toWrite.push_back(pid);
        } else {
          toEvict.push_back(pid);
        }
        break;
      case PageState::Unlocked:
        ps.tryMark(v);
        break;
      default:
        break; // skip
      };
    });
  }

  // 1. write dirty pages
  libaioInterface[workerThreadId].writePages(toWrite);
  writeCount += toWrite.size();

  // 2. try to lock clean page candidates
  toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(),
                               [&](PID pid) {
                                 PageState &ps = getPageState(pid);
                                 u64 v = ps.stateAndVersion;
                                 return (PageState::getState(v) !=
                                         PageState::Marked) ||
                                        !ps.tryLockX(v);
                               }),
                toEvict.end());

  // 3. try to upgrade lock for dirty page candidates
  for (auto &pid : toWrite) {
    PageState &ps = getPageState(pid);
    u64 v = ps.stateAndVersion;
    if ((PageState::getState(v) == 1) &&
        ps.stateAndVersion.compare_exchange_weak(
            v, PageState::sameVersion(v, PageState::Locked)))
      toEvict.push_back(pid);
    else
      ps.unlockS();
  }

  // 4. remove from page table
  for (u64 &pid : toEvict)
    madvise(virtMem + pid, pageSize, MADV_DONTNEED);

  // 5. remove from hash table and unlock
  for (u64 &pid : toEvict) {
    bool succ = residentSet.remove(pid);
    assert(succ);
    getPageState(pid).unlockXEvicted();
  }

  physUsedCount -= toEvict.size();
}

BTree::BTree() : splitOrdered(false) {
  GuardX<MetaDataPage> page(metadataPageId);
  AllocGuard<BTreeNode> rootNode(true);
  slotId = btreeslotcounter++;
  page->roots[slotId] = rootNode.pid;
}

BTree::~BTree() {}

void BTree::trySplit(GuardX<BTreeNode> &&node, GuardX<BTreeNode> &&parent,
                     span<u8> key, unsigned payloadLen) {

  // create new root if necessary
  if (parent.pid == metadataPageId) {
    MetaDataPage *metaData = reinterpret_cast<MetaDataPage *>(parent.ptr);
    AllocGuard<BTreeNode> newRoot(false);
    newRoot->upperInnerNode = node.pid;
    metaData->roots[slotId] = newRoot.pid;
    parent = move(newRoot);
  }

  // split
  BTreeNode::SeparatorInfo sepInfo = node->findSeparator(splitOrdered.load());
  u8 sepKey[sepInfo.len];
  node->getSep(sepKey, sepInfo);

  if (parent->hasSpaceFor(
          sepInfo.len,
          sizeof(
              PID))) { // is there enough space in the parent for the separator?
    node->splitNode(parent.ptr, sepInfo.slot, {sepKey, sepInfo.len});
    return;
  }

  // must split parent to make space for separator, restart from root to do this
  node.release();
  parent.release();
  ensureSpace(parent.ptr, {sepKey, sepInfo.len}, sizeof(PID));
}

void BTree::ensureSpace(BTreeNode *toSplit, span<u8> key, unsigned payloadLen) {
  for (u64 repeatCounter = 0;; repeatCounter++) {
    try {
      GuardO<BTreeNode> parent(metadataPageId);
      GuardO<BTreeNode> node(
          reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId),
          parent);

      while (node->isInner() && (node.ptr != toSplit)) {
        parent = move(node);
        node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
      }
      if (node.ptr == toSplit) {
        if (node->hasSpaceFor(key.size(), payloadLen))
          return; // someone else did split concurrently
        GuardX<BTreeNode> parentLocked(move(parent));
        GuardX<BTreeNode> nodeLocked(move(node));
        trySplit(move(nodeLocked), move(parentLocked), key, payloadLen);
      }
      return;
    } catch (const OLCRestartException &) {
      yield(repeatCounter);
    }
  }
}

void BTree::insert(span<u8> key, span<u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNode::maxKVSize);

  for (u64 repeatCounter = 0;; repeatCounter++) {
    try {
      GuardO<BTreeNode> parent(metadataPageId);
      GuardO<BTreeNode> node(
          reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId),
          parent);

      while (node->isInner()) {
        parent = move(node);
        node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
      }

      if (node->hasSpaceFor(key.size(), payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> nodeLocked(move(node));
        parent.release();
        nodeLocked->insertInPage(key, payload);
        return; // success
      }

      // lock parent and leaf
      GuardX<BTreeNode> parentLocked(move(parent));
      GuardX<BTreeNode> nodeLocked(move(node));
      trySplit(move(nodeLocked), move(parentLocked), key, payload.size());
      // insert hasn't happened, restart from root
    } catch (const OLCRestartException &) {
      yield(repeatCounter);
    }
  }
}

bool BTree::remove(span<u8> key) {
  for (u64 repeatCounter = 0;; repeatCounter++) {
    try {
      GuardO<BTreeNode> parent(metadataPageId);
      GuardO<BTreeNode> node(
          reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId),
          parent);

      u16 pos;
      while (node->isInner()) {
        pos = node->lowerBound(key);
        PID nextPage =
            (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
        parent = move(node);
        node = GuardO<BTreeNode>(nextPage, parent);
      }

      bool found;
      unsigned slotId = node->lowerBound(key, found);
      if (!found)
        return false;

      unsigned sizeEntry =
          node->slot[slotId].keyLen + node->slot[slotId].payloadLen;
      if ((node->freeSpaceAfterCompaction() + sizeEntry >=
           BTreeNodeHeader::underFullSize) &&
          (parent.pid != metadataPageId) && (parent->count >= 2) &&
          ((pos + 1) < parent->count)) {
        // underfull
        GuardX<BTreeNode> parentLocked(move(parent));
        GuardX<BTreeNode> nodeLocked(move(node));
        GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1));
        nodeLocked->removeSlot(slotId);
        if (rightLocked->freeSpaceAfterCompaction() >=
            (pageSize - BTreeNodeHeader::underFullSize)) {
          if (nodeLocked->mergeNodes(pos, parentLocked.ptr, rightLocked.ptr)) {
            // XXX: should reuse page Id
          }
        }
      } else {
        GuardX<BTreeNode> nodeLocked(move(node));
        parent.release();
        nodeLocked->removeSlot(slotId);
      }
      return true;
    } catch (const OLCRestartException &) {
      yield(repeatCounter);
    }
  }
}