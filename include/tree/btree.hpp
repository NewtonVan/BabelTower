#pragma once
#include <csignal>
#include <fcntl.h>
#include <span>

#include <tree/btree_node.hpp>

using namespace std;

static const u64 metadataPageId = 0;

struct MetaDataPage {
  bool dirty;
  PID roots[(pageSize - 8) / 8];

  PID getRoot(unsigned slot) { return roots[slot]; }
};

struct BTree {
private:
  void trySplit(GuardX<BTreeNode> &&node, GuardX<BTreeNode> &&parent,
                span<u8> key, unsigned payloadLen);
  void ensureSpace(BTreeNode *toSplit, span<u8> key, unsigned payloadLen);

public:
  unsigned slotId;
  atomic<bool> splitOrdered;

  BTree();
  ~BTree();

  GuardO<BTreeNode> findLeafO(span<u8> key) {
    GuardO<MetaDataPage> meta(metadataPageId);
    GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
    meta.release();

    while (node->isInner())
      node = GuardO<BTreeNode>(node->lookupInner(key), node);
    return node;
  }

  // point lookup, returns payload len on success, or -1 on failure
  int lookup(span<u8> key, u8 *payloadOut, unsigned payloadOutSize) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return -1;

        // key found, copy payload
        memcpy(payloadOut, node->getPayload(pos).data(),
               min(node->slot[pos].payloadLen, payloadOutSize));
        return node->slot[pos].payloadLen;
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  template <class Fn> bool lookup(span<u8> key, Fn fn) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return false;

        // key found
        fn(node->getPayload(pos));
        return true;
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  void insert(span<u8> key, span<u8> payload);
  bool remove(span<u8> key);

  template <class Fn> bool updateInPlace(span<u8> key, Fn fn) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<BTreeNode> node = findLeafO(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
          return false;

        {
          GuardX<BTreeNode> nodeLocked(move(node));
          fn(nodeLocked->getPayload(pos));
          return true;
        }
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  GuardS<BTreeNode> findLeafS(span<u8> key) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
      try {
        GuardO<MetaDataPage> meta(metadataPageId);
        GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
        meta.release();

        while (node->isInner())
          node = GuardO<BTreeNode>(node->lookupInner(key), node);

        return GuardS<BTreeNode>(move(node));
      } catch (const OLCRestartException &) {
        yield(repeatCounter);
      }
    }
  }

  template <class Fn> void scanAsc(span<u8> key, Fn fn) {
    GuardS<BTreeNode> node = findLeafS(key);
    bool found;
    unsigned pos = node->lowerBound(key, found);
    for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
      if (pos < node->count) {
        if (!fn(*node.ptr, pos))
          return;
        pos++;
      } else {
        if (!node->hasRightNeighbour())
          return;
        pos = 0;
        node = GuardS<BTreeNode>(node->nextLeafNode);
      }
    }
  }

  template <class Fn> void scanDesc(span<u8> key, Fn fn) {
    GuardS<BTreeNode> node = findLeafS(key);
    bool exactMatch;
    int pos = node->lowerBound(key, exactMatch);
    if (pos == node->count) {
      pos--;
      exactMatch = true; // XXX:
    }
    for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
      while (pos >= 0) {
        if (!fn(*node.ptr, pos, exactMatch))
          return;
        pos--;
      }
      if (!node->hasLowerFence())
        return;
      node = findLeafS(node->getLowerFence());
      pos = node->count - 1;
    }
  }
};

static unsigned btreeslotcounter = 0;