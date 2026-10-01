//
// The MIT License (MIT)
//
// Copyright (c) 2019 Livox. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include "lddc.h"

#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdexcept>
#include <chrono>
#include <sstream>

#include <rclcpp/rclcpp.hpp>
#include <pcl_conversions/pcl_conversions.h>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include "livox_interfaces_ext/msg/custom_point.hpp"
#include "livox_interfaces_ext/msg/custom_msg.hpp"

#include "lds_lidar.h"
#include "lds_lvx.h"

namespace livox_ros {

static double SteadySeconds() {
  return std::chrono::duration<double>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

void Lddc::SetRosNode(rclcpp::Node* node) {
  cur_node_ = node;
  require_ptp_ = node->declare_parameter<bool>("require_ptp", false);
  ptp_utc_offset_seconds_ = node->declare_parameter<int>("ptp_utc_offset_seconds", 37);
  const double stable = node->declare_parameter<double>("time_stable_seconds", 1.0);
  max_gap_seconds_ = node->declare_parameter<double>("time_max_gap_seconds", 1.0);
  const double age = node->declare_parameter<double>("time_max_age_seconds", 2.0);
  const double future = node->declare_parameter<double>("time_future_tolerance_seconds", 0.1);
  if (ptp_utc_offset_seconds_ < 0 || !std::isfinite(stable) || stable <= 0 ||
      !std::isfinite(max_gap_seconds_) || max_gap_seconds_ <= 0 ||
      !std::isfinite(age) || age <= 0 || !std::isfinite(future) || future < 0)
    throw std::invalid_argument("Invalid Livox time guard parameters");
  for (auto& g : time_guards_) {
    g.stable_seconds = stable;
    g.max_gap_seconds = max_gap_seconds_;
    g.max_age_seconds = age;
    g.future_seconds = future;
  }
  time_status_pub_ = node->create_publisher<std_msgs::msg::String>(
      "/livox/time_status", rclcpp::QoS(1).transient_local().reliable());
  time_status_timer_ = node->create_wall_timer(std::chrono::seconds(1), [this]() {
    std::lock_guard<std::mutex> lock(status_mutex_);
    std::ostringstream out;
    out << "{\"devices\":[";
    bool comma = false;
    for (size_t i = 0; i < status_.size(); ++i) {
      const auto& s = status_[i];
      if (!s.seen) continue;
      const bool stale = SteadySeconds() - s.last[0] > max_gap_seconds_ ||
                         SteadySeconds() - s.last[1] > max_gap_seconds_;
      if (comma) out << ',';
      comma = true;
      out << "{\"handle\":" << i << ",\"ready\":"
          << (s.ready && !stale ? "true" : "false")
          << ",\"reason\":\"" << (stale ? "stream_stale" : s.reason)
          << "\",\"faults\":" << s.faults
          << ",\"overlap_drops\":" << s.overlaps << '}';
    }
    out << "],\"state\":\"" << (comma ? "monitoring" : "waiting_for_packets") << "\"}";
    std_msgs::msg::String msg;
    msg.data = out.str();
    time_status_pub_->publish(msg);
  });
}

void Lddc::UpdateTimeStatus(uint8_t handle) {
  std::lock_guard<std::mutex> lock(status_mutex_);
  auto& s = status_[handle];
  const auto& g = time_guards_[handle];
  s.seen = true;
  s.ready = g.ready;
  s.reason = g.reason;
  s.faults = g.faults;
  s.overlaps = g.overlaps;
}

void Lddc::FlushTimeQueues(uint8_t handle) {
  // Advance only the consumer index. Never reset the producer's indices.
  for (auto* q : {&lds_->lidars_[handle].data, &lds_->lidars_[handle].imu_data}) {
    if (!q->storage_packet) continue;
    const uint32_t pending = QueueUsedSize(q);
    for (uint32_t i = 0; i < pending; ++i) QueuePopUpdate(q);
  }
  point_end_[handle] = 0;
  lds_->lidars_[handle].data_is_pubulished = false;
}

bool Lddc::AcceptPacket(uint8_t handle, StoragePacket& packet, bool imu) {
  auto& lidar = lds_->lidars_[handle];
  // Recorded LVX timestamps need not belong to the current wall-clock epoch.
  if (lidar.data_src == kSourceLvxFile) return true;
  auto* raw = reinterpret_cast<LivoxEthPacket*>(packet.raw_data);
  uint32_t status;
  memcpy(&status, packet.raw_data + 4, sizeof(status));
  const bool synchronized = ((status >> 13) & 1) && ((status >> 14) & 7) == 1;
  const auto stamp = GetStoragePacketTimestamp(&packet, lidar.data_src);
  auto& g = time_guards_[handle];
  bool fault = false;
  const bool accepted = g.Check(imu ? 1 : 0, stamp, raw->timestamp_type,
      synchronized, require_ptp_, uint64_t(ptp_utc_offset_seconds_) * 1000000000ULL,
      cur_node_->now().nanoseconds(), SteadySeconds(), fault);
  {
    std::lock_guard<std::mutex> lock(status_mutex_);
    status_[handle].last[imu ? 1 : 0] = SteadySeconds();
  }
  if (fault) {
    FlushTimeQueues(handle);
    RCLCPP_WARN_THROTTLE(cur_node_->get_logger(), *cur_node_->get_clock(), 5000,
                        "Livox[%u] time paused: %s", handle, g.reason.c_str());
  }
  if (!accepted) {
    UpdateTimeStatus(handle);
    return false;
  }
  if (!imu) {
    const uint64_t span = packet.point_num ?
        uint64_t(packet.point_num - 1) * GetPointInterval(lidar.info.type) : 0;
    if (!AcceptPointSpan(stamp, span, point_end_[handle])) {
      ++g.overlaps;
      // Packet starts still increase: this is a point-span overlap, not an epoch reset.
      UpdateTimeStatus(handle);
      return false;
    }
  }
  UpdateTimeStatus(handle);
  return true;
}

/** Lidar Data Distribute Control--------------------------------------------*/
Lddc::Lddc(int format, int multi_topic, int data_src, int output_type,
           double frq, std::string &frame_id)
    : transfer_format_(format),
      use_multi_topic_(multi_topic),
      data_src_(data_src),
      output_type_(output_type),
      publish_frq_(frq),
      frame_id_(frame_id) {
  publish_period_ns_ = kNsPerSecond / publish_frq_;
  lds_ = nullptr;
#if 0
  bag_ = nullptr;
#endif
}

Lddc::~Lddc() {
  if (lds_) {
    lds_->PrepareExit();
  }
}

void Lddc::InitPointcloud2MsgHeader(sensor_msgs::msg::PointCloud2& cloud) {
  cloud.header.frame_id.assign(frame_id_);
  cloud.height = 1;
  cloud.width = 0;
  cloud.fields.resize(6);
  cloud.fields[0].offset = 0;
  cloud.fields[0].name = "x";
  cloud.fields[0].count = 1;
  cloud.fields[0].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[1].offset = 4;
  cloud.fields[1].name = "y";
  cloud.fields[1].count = 1;
  cloud.fields[1].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[2].offset = 8;
  cloud.fields[2].name = "z";
  cloud.fields[2].count = 1;
  cloud.fields[2].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[3].offset = 12;
  cloud.fields[3].name = "intensity";
  cloud.fields[3].count = 1;
  cloud.fields[3].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[4].offset = 16;
  cloud.fields[4].name = "tag";
  cloud.fields[4].count = 1;
  cloud.fields[4].datatype = sensor_msgs::msg::PointField::UINT8;
  cloud.fields[5].offset = 17;
  cloud.fields[5].name = "line";
  cloud.fields[5].count = 1;
  cloud.fields[5].datatype = sensor_msgs::msg::PointField::UINT8;
  cloud.point_step = sizeof(LivoxPointXyzrtl);
}

uint32_t Lddc::PublishPointcloud2(LidarDataQueue *queue, uint32_t packet_num,
                                  uint8_t handle) {
  uint64_t timestamp = 0;
  uint32_t published_packet = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];

  sensor_msgs::msg::PointCloud2 cloud;
  InitPointcloud2MsgHeader(cloud);
  cloud.data.resize(packet_num * kMaxPointPerEthPacket *
                    sizeof(LivoxPointXyzrtl));
  cloud.point_step = sizeof(LivoxPointXyzrtl);

  uint8_t *point_base = cloud.data.data();
  uint8_t data_source = lidar->data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  while ((published_packet < packet_num) && !QueueIsEmpty(queue)) {
    QueuePrePop(queue, &storage_packet);
    QueuePopUpdate(queue);
    if (!AcceptPacket(handle, storage_packet, false)) return published_packet + 1;
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);

    /** Use the first packet timestamp as pointcloud2 msg timestamp */
    if (!published_packet) {
      cloud.header.stamp = rclcpp::Time(GetSyncedTimeNs(timestamp, raw_packet->timestamp_type));
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        point_base = pf_point_convert(point_base, raw_packet,
            lidar->extrinsic_parameter, line_num);
      } else {
        /** Skip the packet */
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                 raw_packet->data_type);
        break;
      }
    } else {
      point_base = LivoxPointToPxyzrtl(point_base, raw_packet,
          lidar->extrinsic_parameter, line_num);
    }

    cloud.width += single_point_num;
    ++published_packet;
  }
  if (published_packet != packet_num) return published_packet;
  cloud.row_step     = cloud.width * cloud.point_step;
  cloud.is_bigendian = false;
  cloud.is_dense     = true;
  cloud.data.resize(cloud.row_step); /** Adjust to the real size */
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr publisher =
      std::dynamic_pointer_cast<rclcpp::Publisher
      <sensor_msgs::msg::PointCloud2>>(GetCurrentPublisher(handle));
  if (kOutputToRos == output_type_) {
    publisher->publish(cloud);
  } else {
#if 0    
    if (bag_) {
      bag_->write(p_publisher->getTopic(), rclcpp::Time(timestamp),
                  cloud);
    }
#endif    
  }
  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

void Lddc::FillPointsToPclMsg(PointCloud& pcl_msg, \
    LivoxPointXyzrtl* src_point, uint32_t num) {
  LivoxPointXyzrtl* point_xyzrtl = (LivoxPointXyzrtl*)src_point;
  for (uint32_t i = 0; i < num; i++) {
    pcl::PointXYZI point;
    point.x = point_xyzrtl->x;
    point.y = point_xyzrtl->y;
    point.z = point_xyzrtl->z;
    point.intensity = point_xyzrtl->reflectivity;
    ++point_xyzrtl;
    pcl_msg.points.push_back(point);
  }
}

/* for pcl::pxyzi */
uint32_t Lddc::PublishPointcloudData(LidarDataQueue *queue, uint32_t packet_num,
                                     uint8_t handle) {
  uint64_t timestamp = 0;
  uint32_t published_packet = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];

  PointCloud cloud;
  cloud.header.frame_id.assign(frame_id_);
  cloud.height = 1;
  cloud.width = 0;

  uint8_t point_buf[2048];
  uint8_t data_source = lidar->data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  while ((published_packet < packet_num) && !QueueIsEmpty(queue)) {
    QueuePrePop(queue, &storage_packet);
    QueuePopUpdate(queue);
    if (!AcceptPacket(handle, storage_packet, false)) return published_packet + 1;
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);

    if (!published_packet) {
      cloud.header.stamp = GetSyncedTimeNs(timestamp, raw_packet->timestamp_type) / 1000;  // to pcl ros time stamp
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        pf_point_convert(point_buf, raw_packet, lidar->extrinsic_parameter, \
            line_num);
      } else {
        /* Skip the packet */
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                 raw_packet->data_type);
        break;
      }
    } else {
      LivoxPointToPxyzrtl(point_buf, raw_packet, lidar->extrinsic_parameter, \
          line_num);
    }
    LivoxPointXyzrtl *dst_point = (LivoxPointXyzrtl *)point_buf;
    FillPointsToPclMsg(cloud, dst_point, single_point_num);

    cloud.width += single_point_num;
    ++published_packet;
  }

  if (published_packet != packet_num) return published_packet;
    rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr publisher =
    std::dynamic_pointer_cast<rclcpp::Publisher<sensor_msgs::msg::PointCloud2>>
    (GetCurrentPublisher(handle));
  if (kOutputToRos == output_type_) {
    sensor_msgs::msg::PointCloud2 cloud_ros;
    pcl::toROSMsg(cloud,cloud_ros);
    publisher->publish(cloud_ros);
  } else {
#if 0    
    if (bag_) {
      bag_->write(p_publisher->getTopic(), rclcpp::Time(timestamp),
                  cloud);
    }
#endif    
  }
  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

void Lddc::FillPointsToCustomMsg(livox_interfaces_ext::msg::CustomMsg& livox_msg, \
    LivoxPointXyzrtl* src_point, uint32_t num, uint32_t offset_time, \
    uint32_t point_interval, uint32_t echo_num) {
  LivoxPointXyzrtl* point_xyzrtl = (LivoxPointXyzrtl*)src_point;
  for (uint32_t i = 0; i < num; i++) {
    livox_interfaces_ext::msg::CustomPoint point;
    if (echo_num > 1) { /** dual return mode */
      point.offset_time = offset_time + (i / echo_num) * point_interval;
    } else {
      point.offset_time = offset_time + i * point_interval;
    }
    point.x = point_xyzrtl->x;
    point.y = point_xyzrtl->y;
    point.z = point_xyzrtl->z;
    point.reflectivity = point_xyzrtl->reflectivity;
    point.tag = point_xyzrtl->tag;
    point.line = point_xyzrtl->line;
    ++point_xyzrtl;
    livox_msg.points.push_back(point);
  }
}

uint32_t Lddc::PublishCustomPointcloud(LidarDataQueue *queue,
                                       uint32_t packet_num, uint8_t handle) {
  // static uint32_t msg_seq = 0;
  uint64_t timestamp = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];

  livox_interfaces_ext::msg::CustomMsg livox_msg;
  livox_msg.header.frame_id.assign(frame_id_);
  // livox_msg.header.seq = msg_seq;
  // ++msg_seq;
  livox_msg.timebase = 0;
  livox_msg.point_num = 0;
  livox_msg.lidar_id = handle;

  uint8_t point_buf[2048];
  uint8_t data_source = lds_->lidars_[handle].data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  uint32_t point_interval = GetPointInterval(lidar->info.type);
  uint32_t published_packet = 0;
  uint32_t packet_offset_time = 0;  /** uint:ns */
  uint64_t first_packet_raw_time = 0;
  while (published_packet < packet_num && !QueueIsEmpty(queue)) {
    QueuePrePop(queue, &storage_packet);
    QueuePopUpdate(queue);
    if (!AcceptPacket(handle, storage_packet, false)) return published_packet + 1;
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);

    /** first packet */
    if (!published_packet) {
      first_packet_raw_time = timestamp;
      uint64_t synced_time_ns = GetSyncedTimeNs(timestamp, raw_packet->timestamp_type);

      livox_msg.timebase = synced_time_ns;
      packet_offset_time = 0;
      /** convert to ros time stamp */
      livox_msg.header.stamp = rclcpp::Time(synced_time_ns);
    } else {
      if (timestamp < first_packet_raw_time ||
          timestamp - first_packet_raw_time > UINT32_MAX) {
        time_guards_[handle].Reset("scan_span_overflow");
        FlushTimeQueues(handle);
        UpdateTimeStatus(handle);
        return published_packet + 1;
      }
      packet_offset_time = static_cast<uint32_t>(timestamp - first_packet_raw_time);
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        pf_point_convert(point_buf, raw_packet, lidar->extrinsic_parameter, \
            line_num);
      } else {
        /* Skip the packet */
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                 lidar->raw_data_type);
        break;
      }
    } else {
      LivoxPointToPxyzrtl(point_buf, raw_packet, lidar->extrinsic_parameter, \
          line_num);
    }
    LivoxPointXyzrtl *dst_point = (LivoxPointXyzrtl *)point_buf;
    FillPointsToCustomMsg(livox_msg, dst_point, single_point_num, \
        packet_offset_time, point_interval, echo_num);


    livox_msg.point_num += single_point_num;
    ++published_packet;
  }

  if (published_packet != packet_num) return published_packet;
  rclcpp::Publisher<livox_interfaces_ext::msg::CustomMsg>::SharedPtr publisher =
      std::dynamic_pointer_cast<rclcpp::Publisher
      <livox_interfaces_ext::msg::CustomMsg>>(GetCurrentPublisher(handle));  
  if (kOutputToRos == output_type_) {
    publisher->publish(livox_msg);
  } else {
#if 0    
    if (bag_) {
      bag_->write(p_publisher->getTopic(), rclcpp::Time(timestamp),
          livox_msg);
    }
#endif    
  }

  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

uint32_t Lddc::PublishImuData(LidarDataQueue *queue, uint32_t packet_num,
                              uint8_t handle) {
  uint64_t timestamp = 0;
  uint32_t published_packet = 0;

  sensor_msgs::msg::Imu imu_data;
  imu_data.header.frame_id = "livox_frame";

  uint8_t data_source = lds_->lidars_[handle].data_src;
  StoragePacket storage_packet;
  QueuePrePop(queue, &storage_packet);
  QueuePopUpdate(queue);
  if (!AcceptPacket(handle, storage_packet, true)) return 1;
  LivoxEthPacket *raw_packet =
      reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
  timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);
  if (timestamp) {
    imu_data.header.stamp =
        rclcpp::Time(GetSyncedTimeNs(timestamp, raw_packet->timestamp_type));  // to ros time stamp
  }

  uint8_t point_buf[2048];
  LivoxImuDataProcess(point_buf, raw_packet);

  LivoxImuPoint *imu = (LivoxImuPoint *)point_buf;
  imu_data.angular_velocity.x = imu->gyro_x;
  imu_data.angular_velocity.y = imu->gyro_y;
  imu_data.angular_velocity.z = imu->gyro_z;
  imu_data.linear_acceleration.x = imu->acc_x;
  imu_data.linear_acceleration.y = imu->acc_y;
  imu_data.linear_acceleration.z = imu->acc_z;

  ++published_packet;

  rclcpp::Publisher<sensor_msgs::msg::Imu>::SharedPtr publisher =
      std::dynamic_pointer_cast<rclcpp::Publisher
      <sensor_msgs::msg::Imu>>(GetCurrentImuPublisher(handle));
  if (kOutputToRos == output_type_) {
    publisher->publish(imu_data);
  } else {
#if 0    
    if (bag_) {
      bag_->write(p_publisher->getTopic(), rclcpp::Time(timestamp),
                  imu_data);
    }
#endif    
  }
  return published_packet;
}

int Lddc::RegisterLds(Lds *lds) {
  if (lds_ == nullptr) {
    lds_ = lds;
    return 0;
  } else {
    return -1;
  }
}

void Lddc::PollingLidarPointCloudData(uint8_t handle, LidarDevice *lidar) {
  LidarDataQueue *p_queue = &lidar->data;
  if (p_queue->storage_packet == nullptr) {
    return;
  }

  while (!QueueIsEmpty(p_queue)) {
    uint32_t used_size = QueueUsedSize(p_queue);
    uint32_t onetime_publish_packets = lidar->onetime_publish_packets;
    if (used_size < onetime_publish_packets) {
      break;
    }

    if (kPointCloud2Msg == transfer_format_) {
      PublishPointcloud2(p_queue, onetime_publish_packets, handle);
    } else if (kLivoxCustomMsg == transfer_format_) {
      PublishCustomPointcloud(p_queue, onetime_publish_packets, handle);
    } else if (kPclPxyziMsg == transfer_format_) {
      PublishPointcloudData(p_queue, onetime_publish_packets, handle);
    }
  }
}

void Lddc::PollingLidarImuData(uint8_t handle, LidarDevice *lidar) {
  LidarDataQueue *p_queue = &lidar->imu_data;
  if (p_queue->storage_packet == nullptr) {
    return;
  }
  while (!QueueIsEmpty(p_queue)) {
    PublishImuData(p_queue, 1, handle);
  }
}

void Lddc::DistributeLidarData(void) {
  if (lds_ == nullptr) {
    return;
  }
  lds_->semaphore_.Wait();
  for (uint32_t i = 0; i < lds_->lidar_count_; i++) {
    uint32_t lidar_id = i;
    LidarDevice *lidar = &lds_->lidars_[lidar_id];
    LidarDataQueue *p_queue = &lidar->data;
    if ((kConnectStateSampling != lidar->connect_state) ||
        (p_queue == nullptr)) {
      continue;
    }
    PollingLidarPointCloudData(lidar_id, lidar);
    PollingLidarImuData(lidar_id, lidar);
  }

  if (lds_->IsRequestExit()) {
    PrepareExit();
  }
}

//System clock implementation
uint64_t Lddc::GetSyncedTimeNs(uint64_t hw_time, uint8_t timestamp_type) {
  if (timestamp_type == kTimestampTypePtp) {
    RCLCPP_INFO_ONCE(cur_node_->get_logger(),
      "Livox PTP timestamps: converting to ROS UTC by subtracting %d seconds",
      ptp_utc_offset_seconds_);
    return hw_time - static_cast<uint64_t>(ptp_utc_offset_seconds_) * 1000000000ULL;
  }
  static bool global_is_time_synced_ = false;
  static uint64_t global_initial_hw_time_ = 0;
  static uint64_t global_initial_ros_time_ = 0;

  if (!global_is_time_synced_ && hw_time > 0) {
    global_initial_hw_time_ = hw_time;
    
    global_initial_ros_time_ = cur_node_->now().nanoseconds();
    global_is_time_synced_ = true;
    RCLCPP_INFO(cur_node_->get_logger(), "Livox Hardware clock synced to ROS System Time.");
  }
  
  return global_initial_ros_time_ + (hw_time - global_initial_hw_time_);
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::CreatePublisher(uint8_t msg_type,
    std::string &topic_name, uint32_t queue_size) {
    if (kPointCloud2Msg == msg_type) {
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use PointCloud2 format", topic_name.c_str());
      return cur_node_->create_publisher<
          sensor_msgs::msg::PointCloud2>(topic_name, queue_size);
    } else if (kLivoxCustomMsg == msg_type) {
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use livox custom format", topic_name.c_str());
      return cur_node_->create_publisher<
          livox_interfaces_ext::msg::CustomMsg>(topic_name, queue_size);
    }
#if 0
    else if (kPclPxyziMsg == msg_type)  {
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use pcl PointXYZI format", topic_name.c_str());
      return cur_node_->create_publisher<PointCloud>(topic_name, queue_size);
    }
#endif    
    else if (kLivoxImuMsg == msg_type)  {
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use imu format", topic_name.c_str());
      return cur_node_->create_publisher<sensor_msgs::msg::Imu>(topic_name,
          queue_size);
    } else {
      std::shared_ptr<rclcpp::PublisherBase>null_publisher(nullptr);
      return null_publisher;
    }
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::GetCurrentPublisher(uint8_t handle) {
  uint32_t queue_size = kMinEthPacketQueueSize;
  if (use_multi_topic_) {
    if (!private_pub_[handle]) {
      char name_str[48];
      memset(name_str, 0, sizeof(name_str));
      snprintf(name_str, sizeof(name_str), "livox/lidar_%s",
          lds_->lidars_[handle].info.broadcast_code);
      std::string topic_name(name_str);
      queue_size = queue_size * 2; // queue size is 64 for only one lidar
      private_pub_[handle] = CreatePublisher(transfer_format_, topic_name,
          queue_size);
    }
    return private_pub_[handle];
  } else {
    if (!global_pub_) {
      std::string topic_name("livox/lidar");
      queue_size = queue_size * 8; // shared queue size is 256, for all lidars
      global_pub_ = CreatePublisher(transfer_format_, topic_name, queue_size);
    }
    return global_pub_;
  }
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::GetCurrentImuPublisher(uint8_t handle) {
  uint32_t queue_size = kMinEthPacketQueueSize;
  if (use_multi_topic_) {
    if (!private_imu_pub_[handle]) {
      char name_str[48];
      memset(name_str, 0, sizeof(name_str));
      snprintf(name_str, sizeof(name_str), "livox/imu_%s",
          lds_->lidars_[handle].info.broadcast_code);
      std::string topic_name(name_str);
      queue_size = queue_size * 2; // queue size is 64 for only one lidar
      private_imu_pub_[handle] = CreatePublisher(kLivoxImuMsg, topic_name,
          queue_size);
    }
    return private_imu_pub_[handle];
  } else {
    if (!global_imu_pub_) {
      std::string topic_name("livox/imu");
      queue_size = queue_size * 8; // shared queue size is 256, for all lidars
      global_imu_pub_ = CreatePublisher(kLivoxImuMsg, topic_name, queue_size);
    }
    return global_imu_pub_;
  }
}

void Lddc::CreateBagFile(const std::string &file_name) {
  // if (!bag_) {
  //   bag_ = new rosbag::Bag;
  //   bag_->open(file_name, rosbag::bagmode::Write);
  //   RCLCPP_INFO(cur_node_->get_logger(), "Create bag file :%s!", file_name.c_str());
  // }
}

void Lddc::PrepareExit(void) {
  // if (bag_) {
  //   RCLCPP_INFO(cur_node_->get_logger(), "Waiting to save the bag file!");
  //   bag_->close();
  //   RCLCPP_INFO(cur_node_->get_logger(), "Save the bag file successfully!");
  //   bag_ = nullptr;
  // }
  if (lds_) {
    lds_->PrepareExit();
    lds_ = nullptr;
  }
}

}  // namespace livox_ros
