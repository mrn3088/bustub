//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  auto cmp = [&](const auto &a, const auto &b) {
    if (a->second.access_history_.size() < k_ && b->second.access_history_.size() == k_) {
      return true;
    }
    if (a->second.access_history_.size() == k_ && b->second.access_history_.size() < k_) {
      return false;
    }
    return a->second.access_history_.front() < b->second.access_history_.front();
  };

  auto to_evict = frame_map_.end();
  for (auto it = frame_map_.begin(); it != frame_map_.end(); ++it) {
    if (!it->second.evictable_) {
      continue;
    }
    // select the first candidate
    if (to_evict == frame_map_.end() || cmp(it, to_evict)) {
      to_evict = it;
    }
  }
  *frame_id = to_evict->first;
  frame_map_.erase(to_evict);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "Invalid frame_id!");
  if (frame_map_.count(frame_id) == 0U) {
    auto frame_info = FrameInfo{};
    frame_info.access_history_.push_back(current_timestamp_++);
    frame_map_[frame_id] = frame_info;
  } else {
    frame_map_[frame_id].access_history_.push_back(current_timestamp_++);
    if (frame_map_[frame_id].access_history_.size() > k_) {
      frame_map_[frame_id].access_history_.pop_front();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(frame_map_.count(frame_id), "Invalid frame_id!");
  if (frame_map_.count(frame_id) != 0U && frame_map_[frame_id].evictable_ != set_evictable) {
    frame_map_[frame_id].evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.count(frame_id) == 0U) {
    return;
  }
  if (!frame_map_[frame_id].evictable_) {
    throw "Remove a non-evictable frame!";
  }
  frame_map_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
