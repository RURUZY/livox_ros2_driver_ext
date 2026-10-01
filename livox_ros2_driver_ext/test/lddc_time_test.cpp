#include "lddc.h"
#include <thread>
#include <cstdlib>
#include <iostream>

#define CHECK(x) do { if (!(x)) { std::cerr << "failed line " << __LINE__ << ": " << #x << '\n'; std::exit(1); } } while (0)

namespace livox_ros {
struct LddcTimeTest {
  static void Run(int format) {
    auto node = std::make_shared<rclcpp::Node>("lddc_time_test_" + std::to_string(format),
      rclcpp::NodeOptions().parameter_overrides({
        rclcpp::Parameter("require_ptp", true),
        rclcpp::Parameter("time_stable_seconds", 0.01)}));
    Lds source(100, kSourceRawLidar);
    source.lidar_count_ = 1;
    auto& lidar = source.lidars_[0];
    lidar.info.type = kDeviceTypeLidarAvia;
    lidar.raw_data_type = 2;
    InitQueue(&lidar.data, 16);
    InitQueue(&lidar.imu_data, 16);
    std::string frame = "test";
    Lddc d(format, 0, kSourceRawLidar, kOutputToRosBagFile, 10, frame);
    d.RegisterLds(&source);
    d.SetRosNode(node.get());
    uint64_t t = node->now().nanoseconds() + 37000000000ULL;
    auto packet = [&](bool imu, uint64_t stamp, bool sync = true) {
      StoragePacket p{};
      p.point_num = imu ? 1 : 96;
      auto* raw = reinterpret_cast<LivoxEthPacket*>(p.raw_data);
      raw->version = 5;
      raw->timestamp_type = 1;
      raw->data_type = imu ? 6 : 2;
      memcpy(raw->timestamp, &stamp, 8);
      const uint32_t status = sync ? 24576 : 65536;
      memcpy(p.raw_data + 4, &status, 4);
      return p;
    };
    auto warm = [&]() {
      auto p = packet(false, t); d.AcceptPacket(0, p, false);
      p = packet(true, t); d.AcceptPacket(0, p, true);
      std::this_thread::sleep_for(std::chrono::milliseconds(15));
      t += 2000000;
      p = packet(false, t); d.AcceptPacket(0, p, false);
      p = packet(true, t); d.AcceptPacket(0, p, true);
      CHECK(d.time_guards_[0].ready);
    };
    auto publish = [&]() {
      if (format == 0) return d.PublishPointcloud2(&lidar.data, 2, 0);
      if (format == 1) return d.PublishCustomPointcloud(&lidar.data, 2, 0);
      return d.PublishPointcloudData(&lidar.data, 2, 0);
    };
    warm();
    auto p = packet(false, t + 1000000); QueuePush(&lidar.data, &p);
    p = packet(false, t + 2000000); QueuePush(&lidar.data, &p);
    CHECK(publish() == 2 && QueueIsEmpty(&lidar.data));
    // A fault in the second packet must discard the partial frame and advance
    // both queues; all three actual publishing implementations exercise this.
    p = packet(false, t + 3000000); QueuePush(&lidar.data, &p);
    p = packet(false, t + 37004000000ULL); QueuePush(&lidar.data, &p);
    p = packet(true, t + 3000000); QueuePush(&lidar.imu_data, &p);
    publish();
    CHECK(!d.time_guards_[0].ready);
    CHECK(QueueIsEmpty(&lidar.data) && QueueIsEmpty(&lidar.imu_data));
    t += 5000000;
    warm();
    // IMU faults use the same gate but single-packet consumption.
    p = packet(true, t + 1000000, false); QueuePush(&lidar.imu_data, &p);
    CHECK(d.PublishImuData(&lidar.imu_data, 1, 0) == 1);
    CHECK(!d.time_guards_[0].ready && QueueIsEmpty(&lidar.imu_data));
    t += 3000000;
    warm();
    p = packet(false, t + 1000000); QueuePush(&lidar.data, &p);
    p = packet(false, t + 1385000); QueuePush(&lidar.data, &p);
    publish();
    CHECK(d.time_guards_[0].ready && d.time_guards_[0].overlaps == 1);
    CHECK(QueueIsEmpty(&lidar.data));
  }
};
}

int main(int argc, char** argv) {
  rclcpp::init(argc, argv);
  for (int format = 0; format < 3; ++format) livox_ros::LddcTimeTest::Run(format);
  rclcpp::shutdown();
  std::cout << "all three point formats and IMU recover without exceptions\n";
}
