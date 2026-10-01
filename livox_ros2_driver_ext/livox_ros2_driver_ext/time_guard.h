#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace livox_ros {

// Do not clamp or synthesize point timestamps. Reject the affected frame when
// increasing packet starts nevertheless produce overlapping point spans.
inline bool AcceptPointSpan(uint64_t stamp, uint64_t span, uint64_t& last_end) {
  const bool overlap = last_end && stamp < last_end;
  last_end = stamp + span;
  return !overlap;
}

// Owned by the single polling thread; one instance per physical LiDAR.
class TimeGuard {
 public:
  double stable_seconds = 1.0;
  double max_gap_seconds = 1.0;
  double max_age_seconds = 2.0;
  double future_seconds = 0.1;
  bool ready = false;
  uint64_t faults = 0;
  uint64_t overlaps = 0;
  std::string reason = "startup";

  void Reset(const std::string& why) {
    ready = false;
    reason = why;
    ++faults;
    streams_ = {};
    stable_since_ = -1.0;
  }

  // false: discard this packet. fault distinguishes reset from warm-up.
  bool Check(int stream, uint64_t raw, uint8_t type, bool synchronized,
             bool require_ptp, uint64_t offset, int64_t ros_now,
             double steady_now, bool& fault) {
    fault = false;
    const auto fail = [&](const char* why) {
      Reset(why);
      fault = true;
      return false;
    };
    if (!raw) return fail("zero_timestamp");
    if (require_ptp && (type != 1 || !synchronized))
      return fail("ptp_unlocked");
    if (type == 1) {
      if (raw < offset || raw - offset > INT64_MAX)
        return fail("invalid_timestamp");
      const double age = (static_cast<double>(ros_now) -
                          static_cast<double>(raw - offset)) / 1e9;
      if (age > max_age_seconds || age < -future_seconds)
        return fail("utc_epoch_or_age_mismatch");
    }
    auto& s = streams_[stream];
    if (s.seen) {
      if (type != s.type) return fail("timestamp_type_changed");
      if (raw <= s.raw) return fail("packet_time_backwards");
      if (raw - s.raw > max_gap_seconds * 1e9 ||
          steady_now - s.arrival > max_gap_seconds)
        return fail("packet_time_gap");
    }
    s = {true, type, raw, steady_now};
    const bool both = streams_[0].seen && streams_[1].seen &&
        steady_now - streams_[0].arrival <= max_gap_seconds &&
        steady_now - streams_[1].arrival <= max_gap_seconds;
    if (!both) {
      ready = false;
      stable_since_ = -1;
      reason = "waiting_for_both_streams";
      return false;
    }
    if (stable_since_ < 0) stable_since_ = steady_now;
    ready = steady_now - stable_since_ >= stable_seconds;
    // Preserve the publication watermark across recovery. A configurable short
    // warm-up must not let a backwards clock step leak into published stamps.
    if (ready && (raw <= watermark_[stream] ||
                  streams_[1 - stream].raw < watermark_[1 - stream])) {
      ready = false;
      reason = "waiting_for_previous_epoch";
      return false;
    }
    if (ready) watermark_[stream] = raw;
    reason = ready ? "ready" : "waiting_for_stability";
    return ready;
  }

 private:
  struct Stream {
    bool seen = false;
    uint8_t type = 0;
    uint64_t raw = 0;
    double arrival = 0;
  };
  std::array<Stream, 2> streams_{};
  std::array<uint64_t, 2> watermark_{};
  double stable_since_ = -1;
};
}  // namespace livox_ros
