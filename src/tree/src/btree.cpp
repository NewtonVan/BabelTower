#include <common/ThreadContext.hpp>
#include <tree/btree.hpp>

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
    parent = std::move(newRoot);
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
        parent = std::move(node);
        node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
      }
      if (node.ptr == toSplit) {
        if (node->hasSpaceFor(key.size(), payloadLen))
          return; // someone else did split concurrently
        GuardX<BTreeNode> parentLocked(std::move(parent));
        GuardX<BTreeNode> nodeLocked(std::move(node));
        trySplit(std::move(nodeLocked), std::move(parentLocked), key,
                 payloadLen);
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
        parent = std::move(node);
        node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
      }

      if (node->hasSpaceFor(key.size(), payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> nodeLocked(std::move(node));
        parent.release();
        nodeLocked->insertInPage(key, payload);
        return; // success
      }

      // lock parent and leaf
      GuardX<BTreeNode> parentLocked(std::move(parent));
      GuardX<BTreeNode> nodeLocked(std::move(node));
      trySplit(std::move(nodeLocked), std::move(parentLocked), key,
               payload.size());
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
        parent = std::move(node);
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
        GuardX<BTreeNode> parentLocked(std::move(parent));
        GuardX<BTreeNode> nodeLocked(std::move(node));
        GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1));
        nodeLocked->removeSlot(slotId);
        if (rightLocked->freeSpaceAfterCompaction() >=
            (pageSize - BTreeNodeHeader::underFullSize)) {
          if (nodeLocked->mergeNodes(pos, parentLocked.ptr, rightLocked.ptr)) {
            // XXX: should reuse page Id
          }
        }
      } else {
        GuardX<BTreeNode> nodeLocked(std::move(node));
        parent.release();
        nodeLocked->removeSlot(slotId);
      }
      return true;
    } catch (const OLCRestartException &) {
      yield(repeatCounter);
    }
  }
}