#include "time_guard.h"
#include "ldq.h"
#include <cstdlib>
#include <iostream>

#define CHECK(x) do { if (!(x)) { std::cerr << "failed line " << __LINE__ << ": " << #x << '\n'; std::exit(1); } } while (0)

int main() {
  using namespace livox_ros;
  const uint64_t base = 1790000000000000000ULL;
  const uint64_t offset = 37000000000ULL;
  TimeGuard g;
  bool fault = false;
  auto packet = [&](int stream, double seconds, uint8_t type = 1, bool sync = true,
                    int64_t jump = 0) {
    const uint64_t raw = base + offset + uint64_t(seconds * 1e9) + jump;
    return g.Check(stream, raw, type, sync, true, offset,
                   base + int64_t(seconds * 1e9), seconds, fault);
  };
  for (int i = 0; i <= 20; ++i) {
    packet(0, i * .1); packet(1, i * .1);
  }
  CHECK(g.ready);
  CHECK(!packet(0, 2.1, 1, true, -200000000));
  CHECK(fault && !g.ready && g.reason == "packet_time_backwards");
  CHECK(!packet(1, 2.2)); // other stream must also pause
  for (int i = 23; i <= 36; ++i) { packet(0, i*.1); packet(1, i*.1); }
  CHECK(g.ready);
  CHECK(!packet(0, 3.7, 1, true, 37000000000LL));
  CHECK(fault && g.reason == "utc_epoch_or_age_mismatch");
  CHECK(!packet(0, 3.8, 1, false));
  CHECK(fault && g.reason == "ptp_unlocked");
  CHECK(!packet(1, 3.9, 0));
  CHECK(fault);
  for (int i = 40; i <= 55; ++i) { packet(0, i*.1); packet(1, i*.1); }
  CHECK(g.ready);
  CHECK(!packet(0, 7)); // missing data resets the stability window
  CHECK(fault && !g.ready);
  for (int i = 71; i <= 85; ++i) packet(0, i*.1);
  CHECK(!g.ready); // one stream alone cannot resume
  for (int i = 86; i <= 100; ++i) { packet(0, i*.1); packet(1, i*.1); }
  CHECK(g.ready);
  // A real ring buffer: consuming an invalid head and draining a snapshot
  // must make progress without modifying the producer's index.
  LidarDataQueue q{};
  CHECK(InitQueue(&q, 8) == 0);
  StoragePacket p{};
  for (int i = 0; i < 5; ++i) QueuePush(&q, &p);
  QueuePrePop(&q, &p); QueuePopUpdate(&q);
  const auto producer = q.wr_idx;
  const auto pending = QueueUsedSize(&q);
  for (uint32_t i = 0; i < pending; ++i) QueuePopUpdate(&q);
  CHECK(QueueIsEmpty(&q) && q.wr_idx == producer);
  QueuePush(&q, &p);
  CHECK(QueueUsedSize(&q) == 1);
  DeInitQueue(&q);
  uint64_t last_end = 0;
  CHECK(AcceptPointSpan(base, 395865, last_end));
  CHECK(!AcceptPointSpan(base + 385000, 395865, last_end));
  CHECK(last_end == base + 780865); // no clamping or fabricated time
  CHECK(AcceptPointSpan(base + 800000, 395865, last_end));
  CHECK(AcceptPointSpan(base + 2000000, 395865, last_end)); // loss is a real gap
  TimeGuard short_window;
  short_window.stable_seconds = .01;
  for (int i = 0; i <= 5; ++i) {
    for (int stream = 0; stream < 2; ++stream)
      short_window.Check(stream, base + offset + i*100000000ULL, 1, true,
          true, offset, base + i*100000000ULL, i*.1, fault);
  }
  CHECK(short_window.ready);
  short_window.Check(0, base + offset + 100000000ULL, 1, true, true,
      offset, base + 600000000ULL, .6, fault);
  CHECK(fault);
  for (int i = 0; i < 4; ++i) {
    for (int stream = 0; stream < 2; ++stream)
      short_window.Check(stream, base + offset + 200000000ULL + i*10000000ULL,
          1, true, true, offset, base + 700000000ULL + i*10000000ULL,
          .7+i*.01, fault);
  }
  CHECK(!short_window.ready); // stable, but still older than published data
  std::cout << "time guard and queue recovery tests passed\n";
}
