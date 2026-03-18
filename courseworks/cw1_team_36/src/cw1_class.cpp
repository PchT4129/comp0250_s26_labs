/* feel free to change any part of this file, or delete this file. In general,
you can do whatever you want with this template code, including deleting it all
and starting from scratch. The only requirment is to make sure your entire 
solution is contained within the cw1_team_<your_team_number> package */

#include <cw1_class.h>
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <moveit/planning_interface/planning_interface.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl_conversions/pcl_conversions.h>
#include <tf2/LinearMath/Quaternion.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <tf2_ros/buffer.h>
#include <tf2_ros/transform_listener.h>
#include <rmw/qos_profiles.h>
#include <cmath>
#include <limits>
#include <queue>
#include <string>
#include <vector>
///////////////////////////////////////////////////////////////////////////////

cw1::cw1(const rclcpp::Node::SharedPtr &node)
{
  /* class constructor */
  // Initialize MoveIt components
  node_ = node;
  service_cb_group_ = node_->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
  sensor_cb_group_ = node_->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

  // advertise solutions for coursework tasks
  t1_service_ = node_->create_service<cw1_world_spawner::srv::Task1Service>(
    "/task1_start",
    std::bind(&cw1::t1_callback, this, std::placeholders::_1, std::placeholders::_2),
    rmw_qos_profile_services_default, service_cb_group_);
  t2_service_ = node_->create_service<cw1_world_spawner::srv::Task2Service>(
    "/task2_start",
    std::bind(&cw1::t2_callback, this, std::placeholders::_1, std::placeholders::_2),
    rmw_qos_profile_services_default, service_cb_group_);
  t3_service_ = node_->create_service<cw1_world_spawner::srv::Task3Service>(
    "/task3_start",
    std::bind(&cw1::t3_callback, this, std::placeholders::_1, std::placeholders::_2),
    rmw_qos_profile_services_default, service_cb_group_);

  // Service and sensor callbacks use separate callback groups to align with the
  // current runtime architecture used in cw1_team_0.
  rclcpp::SubscriptionOptions joint_state_sub_options;
  joint_state_sub_options.callback_group = sensor_cb_group_;
  auto joint_state_qos = rclcpp::QoS(rclcpp::KeepLast(50));
  joint_state_qos.reliable();
  joint_state_qos.durability_volatile();
  joint_state_sub_ = node_->create_subscription<sensor_msgs::msg::JointState>(
    "/joint_states", joint_state_qos,
    [this](const sensor_msgs::msg::JointState::ConstSharedPtr msg) {
      const int64_t stamp_ns =
        static_cast<int64_t>(msg->header.stamp.sec) * 1000000000LL +
        static_cast<int64_t>(msg->header.stamp.nanosec);
      latest_joint_state_stamp_ns_.store(stamp_ns, std::memory_order_relaxed);
      joint_state_msg_count_.fetch_add(1, std::memory_order_relaxed);
    },
    joint_state_sub_options);

  rclcpp::SubscriptionOptions cloud_sub_options;
  cloud_sub_options.callback_group = sensor_cb_group_;
  auto cloud_qos = rclcpp::QoS(rclcpp::KeepLast(10));
  cloud_qos.reliable();
  cloud_qos.durability_volatile();
  cloud_sub_ = node_->create_subscription<sensor_msgs::msg::PointCloud2>(
    "/r200/camera/depth_registered/points", cloud_qos,
    [this](const sensor_msgs::msg::PointCloud2::ConstSharedPtr msg) {
      {
        std::lock_guard<std::mutex> lock(cloud_mutex_);
        latest_cloud_ = msg;
      }
      const int64_t stamp_ns =
        static_cast<int64_t>(msg->header.stamp.sec) * 1000000000LL +
        static_cast<int64_t>(msg->header.stamp.nanosec);
      latest_cloud_stamp_ns_.store(stamp_ns, std::memory_order_relaxed);
      cloud_msg_count_.fetch_add(1, std::memory_order_relaxed);
    },
    cloud_sub_options);

  // Parameter declarations intentionally mirror cw1_team_0 for compatibility.
  const bool use_gazebo_gui = node_->declare_parameter<bool>("use_gazebo_gui", true);
  (void)use_gazebo_gui;
  enable_cloud_viewer_ = node_->declare_parameter<bool>("enable_cloud_viewer", false);
  move_home_on_start_ = node_->declare_parameter<bool>("move_home_on_start", false);
  use_path_constraints_ = node_->declare_parameter<bool>("use_path_constraints", false);
  use_cartesian_reach_ = node_->declare_parameter<bool>("use_cartesian_reach", false);
  allow_position_only_fallback_ = node_->declare_parameter<bool>(
    "allow_position_only_fallback", allow_position_only_fallback_);
  cartesian_eef_step_ = node_->declare_parameter<double>(
    "cartesian_eef_step", cartesian_eef_step_);
  cartesian_jump_threshold_ = node_->declare_parameter<double>(
    "cartesian_jump_threshold", cartesian_jump_threshold_);
  cartesian_min_fraction_ = node_->declare_parameter<double>(
    "cartesian_min_fraction", cartesian_min_fraction_);
  publish_programmatic_debug_ = node_->declare_parameter<bool>(
    "publish_programmatic_debug", publish_programmatic_debug_);
  enable_task1_snap_ = node_->declare_parameter<bool>("enable_task1_snap", false);
  return_home_between_pick_place_ = node_->declare_parameter<bool>(
    "return_home_between_pick_place", return_home_between_pick_place_);
  return_home_after_pick_place_ = node_->declare_parameter<bool>(
    "return_home_after_pick_place", return_home_after_pick_place_);
  pick_offset_z_ = node_->declare_parameter<double>("pick_offset_z", pick_offset_z_);
  task3_pick_offset_z_ = node_->declare_parameter<double>(
    "task3_pick_offset_z", task3_pick_offset_z_);
  task2_capture_enabled_ = node_->declare_parameter<bool>(
    "task2_capture_enabled", task2_capture_enabled_);
  task2_capture_dir_ = node_->declare_parameter<std::string>(
    "task2_capture_dir", task2_capture_dir_);
  place_offset_z_ = node_->declare_parameter<double>("place_offset_z", place_offset_z_);
  grasp_approach_offset_z_ = node_->declare_parameter<double>(
    "grasp_approach_offset_z", grasp_approach_offset_z_);
  post_grasp_lift_z_ = node_->declare_parameter<double>(
    "post_grasp_lift_z", post_grasp_lift_z_);
  gripper_grasp_width_ = node_->declare_parameter<double>(
    "gripper_grasp_width", gripper_grasp_width_);
  joint_state_wait_timeout_sec_ = node_->declare_parameter<double>(
    "joint_state_wait_timeout_sec", joint_state_wait_timeout_sec_);

  // Initialise TF2 listener before MoveIt so transforms are available early.
  tf_buffer_ = std::make_shared<tf2_ros::Buffer>(node_->get_clock());
  tf_listener_ = std::make_shared<tf2_ros::TransformListener>(*tf_buffer_);

  // Create MoveIt interfaces once and reuse them in callbacks.
  // Group names come from the Panda SRDF: "panda_arm" for 7-DOF arm, "hand" for gripper.
  arm_group_ = std::make_shared<moveit::planning_interface::MoveGroupInterface>(node_, "panda_arm");
  hand_group_ = std::make_shared<moveit::planning_interface::MoveGroupInterface>(node_, "hand");

  // Conservative defaults are easier to debug in coursework simulation.
  arm_group_->setPlanningTime(5.0);
  arm_group_->setNumPlanningAttempts(5);
  hand_group_->setPlanningTime(2.0);
  hand_group_->setNumPlanningAttempts(3);

  if (task2_capture_enabled_) {
    RCLCPP_INFO(
      node_->get_logger(),
      "Template capture mode enabled, output dir: %s",
      task2_capture_dir_.c_str());
  }

  RCLCPP_INFO(node_->get_logger(), "cw1 template class initialised with compatibility scaffold");
}

///////////////////////////////////////////////////////////////////////////////

geometry_msgs::msg::Quaternion
cw1::make_top_down_q() const
{
  // A simple top-down grasp attitude:
  // - rotate around X by pi so tool points down
  // - small yaw to avoid singular-ish straight alignment in some scenes
  tf2::Quaternion q;
  q.setRPY(3.14159265358979323846, 0.0, -0.78539816339744830962);
  q.normalize();
  return tf2::toMsg(q);
}

///////////////////////////////////////////////////////////////////////////////

std::string
cw1::identify_basket_colour(
  const sensor_msgs::msg::PointCloud2::ConstSharedPtr &cloud,
  const geometry_msgs::msg::PointStamped &basket_loc)
{
  // Transform the basket world-frame position into the cloud's own frame so we
  // can search for nearby points directly in the cloud coordinate system.
  // Use stamp=0 (latest available) because basket_loc carries the wall-clock
  // time from the service request, which does not match Gazebo simulation time.
  geometry_msgs::msg::PointStamped basket_query = basket_loc;
  basket_query.header.stamp = rclcpp::Time(0);

  geometry_msgs::msg::PointStamped basket_in_cloud;
  try {
    basket_in_cloud = tf_buffer_->transform(
      basket_query,
      cloud->header.frame_id,
      tf2::durationFromSec(1.0));
  } catch (const tf2::TransformException &ex) {
    RCLCPP_WARN(node_->get_logger(),
      "TF transform failed for basket at (%.3f, %.3f): %s",
      basket_loc.point.x, basket_loc.point.y, ex.what());
    return "none";
  }

  pcl::PointCloud<pcl::PointXYZRGB>::Ptr pcl_cloud(
    new pcl::PointCloud<pcl::PointXYZRGB>);
  pcl::fromROSMsg(*cloud, *pcl_cloud);

  // Search in the x-y plane of the cloud frame within a circle 
  const float cx = static_cast<float>(basket_in_cloud.point.x);
  const float cy = static_cast<float>(basket_in_cloud.point.y);
  const float cz = static_cast<float>(basket_in_cloud.point.z);

  constexpr float kSearchRadius = 0.035f;
  constexpr float kSearchRadiusSq = kSearchRadius * kSearchRadius;
  constexpr float kZHalfWindow = 0.05f;

  size_t nearby_xyz_points = 0;
  size_t nonzero_rgb_points = 0;

  double sum_r = 0.0;
  double sum_g = 0.0;
  double sum_b = 0.0;

  for (const auto &pt : pcl_cloud->points) {
    if (!std::isfinite(pt.x) || !std::isfinite(pt.y) || !std::isfinite(pt.z)) {
      continue;
    }

    const float dx = pt.x - cx;
    const float dy = pt.y - cy;
    const float dz = pt.z - cz;

    if (dx * dx + dy * dy > kSearchRadiusSq) {
      continue;
    }

    if (std::fabs(dz) > kZHalfWindow) {
      continue;
    }

    nearby_xyz_points++;

    if (pt.r == 0 && pt.g == 0 && pt.b == 0) {
      continue;
    }

    nonzero_rgb_points++;
    sum_r += static_cast<double>(pt.r);
    sum_g += static_cast<double>(pt.g);
    sum_b += static_cast<double>(pt.b);
  }

  if (nearby_xyz_points == 0 || nonzero_rgb_points == 0) {
    RCLCPP_WARN(node_->get_logger(),
      "Task 2: no usable coloured points found near basket at (%.3f, %.3f)",
      basket_loc.point.x, basket_loc.point.y);
    return "none";
  }

  const double mean_r = sum_r / static_cast<double>(nonzero_rgb_points);
  const double mean_g = sum_g / static_cast<double>(nonzero_rgb_points);
  const double mean_b = sum_b / static_cast<double>(nonzero_rgb_points);

  std::string result = "none";

  if (mean_r > 100.0 && mean_b > 100.0 && mean_g < 100.0) {
    result = "purple";
  } else if (mean_r > mean_b + 35.0 && mean_r > mean_g + 35.0 && mean_r > 100.0) {
    result = "red";
  } else if (mean_b > mean_r + 35.0 && mean_b > mean_g + 35.0 && mean_b > 100.0) {
    result = "blue";
  }

  RCLCPP_INFO(node_->get_logger(),
    "Task 2: basket at (%.3f, %.3f) mean RGB = (%.1f, %.1f, %.1f) -> %s",
    basket_loc.point.x, basket_loc.point.y, mean_r, mean_g, mean_b, result.c_str());

  return result;
}

///////////////////////////////////////////////////////////////////////////////

bool
cw1::move_arm_to_pose(const geometry_msgs::msg::Pose &target_pose)
{
  // Always start planning from current measured state.
  arm_group_->setStartStateToCurrentState();
  arm_group_->setPoseTarget(target_pose);

  moveit::planning_interface::MoveGroupInterface::Plan plan;
  const bool planning_ok =
    (arm_group_->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS);
  if (!planning_ok) {
    arm_group_->clearPoseTargets();
    RCLCPP_WARN(node_->get_logger(), "Arm planning failed for requested pose");
    return false;
  }

  const bool execution_ok =
    (arm_group_->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS);
  arm_group_->clearPoseTargets();
  if (!execution_ok) {
    RCLCPP_WARN(node_->get_logger(), "Arm execution failed after successful planning");
    return false;
  }

  return true;
}

///////////////////////////////////////////////////////////////////////////////

bool
cw1::set_gripper_width(double width_m)
{
  // Panda hand limits are roughly [0.0, 0.08] total opening.
  const double clamped_width = std::clamp(width_m, 0.0, 0.08);
  const double finger_joint_target = clamped_width * 0.5;

  std::map<std::string, double> joint_targets;
  joint_targets["panda_finger_joint1"] = finger_joint_target;
  joint_targets["panda_finger_joint2"] = finger_joint_target;

  hand_group_->setStartStateToCurrentState();
  hand_group_->setJointValueTarget(joint_targets);

  moveit::planning_interface::MoveGroupInterface::Plan plan;
  const bool planning_ok =
    (hand_group_->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS);
  if (!planning_ok) {
    RCLCPP_WARN(node_->get_logger(), "Gripper planning failed");
    return false;
  }

  const bool execution_ok =
    (hand_group_->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS);
  if (!execution_ok) {
    RCLCPP_WARN(node_->get_logger(), "Gripper execution failed");
    return false;
  }

  return true;
}

bool
cw1::move_arm_linear_to(const geometry_msgs::msg::Pose &target_pose, double eef_step)
{
  // eef_step: distance between consecutive IK waypoints along the path.
  // Smaller = smoother path but more IK queries and higher failure rate.
  const double safe_eef_step = std::clamp(eef_step, 0.005, 0.02);
  // jump_threshold=0.0 disables joint-jump rejection. Safe here because
  // velocity/acceleration are already capped to 10%, preventing wild motion.
  const double jump_threshold = 0.0;

  // Cartesian path should start from the latest measured robot state.
  arm_group_->setStartStateToCurrentState();

  // Reduce execution aggressiveness; this helps avoid overshoot-like behavior in Gazebo.
  arm_group_->setMaxVelocityScalingFactor(0.05);
  arm_group_->setMaxAccelerationScalingFactor(0.10);

  // Build an explicit short segment from current pose to target pose.
  const auto current_pose_stamped = arm_group_->getCurrentPose();
  std::vector<geometry_msgs::msg::Pose> waypoints;
  waypoints.push_back(current_pose_stamped.pose);
  waypoints.push_back(target_pose);

  moveit_msgs::msg::RobotTrajectory traj;
  const double fraction = arm_group_->computeCartesianPath(
    waypoints, safe_eef_step, jump_threshold, traj);

  if (fraction < 0.8) {
    RCLCPP_WARN(
      node_->get_logger(),
      "Cartesian path incomplete: fraction=%.3f eef_step=%.4f",
      fraction, safe_eef_step);
    return false;
  }

  moveit::planning_interface::MoveGroupInterface::Plan plan;
  plan.trajectory_ = traj;
  const bool execution_ok =
    (arm_group_->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS);
  if (!execution_ok) {
    RCLCPP_WARN(node_->get_logger(), "Cartesian execution failed");
    return false;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////

void
cw1::t1_callback(
  const std::shared_ptr<cw1_world_spawner::srv::Task1Service::Request> request,
  std::shared_ptr<cw1_world_spawner::srv::Task1Service::Response> response)
{
  /* Task 1: pick at known pose, place into known basket point. */

  (void)response;

  // The service request gives:
  // - object_loc: centroid pose of the cube
  // - goal_loc: basket location point
  const auto object_pose = request->object_loc.pose;
  const auto goal_point = request->goal_loc.point;
  const auto top_down_q = make_top_down_q();

  // Build a deterministic sequence of waypoints:
  // 1) pre-grasp above object
  // 2) descend to grasp
  // 3) lift
  // 4) pre-place above basket
  // 5) place and return
  geometry_msgs::msg::Pose pre_grasp;
  pre_grasp.position = object_pose.position;
  pre_grasp.position.z += pick_offset_z_;
  pre_grasp.orientation = top_down_q;

  // grasp: descend vertically from pre_grasp (same x/y/orientation, lower z).
  geometry_msgs::msg::Pose grasp = pre_grasp;
  grasp.position.z -= 0.24;

  // lift: return to pre_grasp height after grasping.
  // Using the same height we safely approached from guarantees the arm clears
  // any obstacles before any lateral movement begins.
  geometry_msgs::msg::Pose lift = pre_grasp;

  // pre_place: safe high point directly above the basket.
  geometry_msgs::msg::Pose pre_place;
  pre_place.position.x = goal_point.x;
  pre_place.position.y = goal_point.y;
  pre_place.position.z = goal_point.z + place_offset_z_;
  pre_place.orientation = top_down_q;

  // Execute pick-and-place sequence.
  // Each step short-circuits on failure so we never execute in a bad state.
  bool ok = true;
  ok = ok && set_gripper_width(0.07);                       // open gripper
  ok = ok && move_arm_to_pose(pre_grasp);                   // global plan: move above object
  ok = ok && move_arm_linear_to(grasp);                     // straight down to grasp height
  ok = ok && set_gripper_width(gripper_grasp_width_);       // close gripper
  ok = ok && move_arm_linear_to(lift);                      // straight up back to safe height
  ok = ok && move_arm_to_pose(pre_place);                   // global plan: transit to above basket
  ok = ok && set_gripper_width(0.07);                       // release cube
  ok = ok && move_arm_linear_to(pre_grasp);                   // straight up out of basket

  if (ok) {
    RCLCPP_INFO(node_->get_logger(), "Task 1 execution finished successfully");
  } else {
    RCLCPP_WARN(node_->get_logger(), "Task 1 sequence finished with at least one failure");
  }
}

void
cw1::t2_callback(
  const std::shared_ptr<cw1_world_spawner::srv::Task2Service::Request> request,
  std::shared_ptr<cw1_world_spawner::srv::Task2Service::Response> response)
{
  RCLCPP_INFO(node_->get_logger(),
    "Task 2 started: %zu basket location(s) to check",
    request->basket_locs.size());

  // Observation height above the ground.  The wrist camera needs to be close
  // enough to resolve basket colours but far enough to keep the arm safe.
  constexpr double kObsZ = 0.50;

  for (const auto &basket_loc : request->basket_locs) {
    // 1. Move camera to a position directly above this basket 
    geometry_msgs::msg::Pose obs_pose;
    obs_pose.position.x = basket_loc.point.x;
    obs_pose.position.y = basket_loc.point.y;
    obs_pose.position.z = kObsZ;
    obs_pose.orientation = make_top_down_q();

    if (!move_arm_to_pose(obs_pose)) {
      RCLCPP_WARN(node_->get_logger(),
        "Could not reach observation pose for basket at (%.3f, %.3f) – marking none",
        basket_loc.point.x, basket_loc.point.y);
      response->basket_colours.push_back("none");
      continue;
    }

    // 2. Wait for the camera to settle and deliver a fresh frame
    // The sensor callback group runs on a separate thread (MultiThreadedExecutor),
    // so latest_cloud_ keeps updating while we sleep here.
    rclcpp::sleep_for(std::chrono::milliseconds(500));

    sensor_msgs::msg::PointCloud2::ConstSharedPtr cloud;
    {
      const auto deadline =
        node_->now() + rclcpp::Duration::from_seconds(3.0);
      while (rclcpp::ok()) {
        {
          std::lock_guard<std::mutex> lock(cloud_mutex_);
          if (latest_cloud_) {
            cloud = latest_cloud_;
          }
        }
        if (cloud) { break; }
        if (node_->now() > deadline) { break; }
        rclcpp::sleep_for(std::chrono::milliseconds(100));
      }
    }

    if (!cloud) {
      RCLCPP_ERROR(node_->get_logger(), "No point cloud received – marking none");
      response->basket_colours.push_back("none");
      continue;
    }

    // 3. Identify colour from the point cloud 
    const std::string colour = identify_basket_colour(cloud, basket_loc);
    response->basket_colours.push_back(colour);
  }

  for (size_t i = 0; i < response->basket_colours.size(); ++i) {
    RCLCPP_INFO(node_->get_logger(),
      "  basket_colours[%zu] = \"%s\"  (loc: %.3f, %.3f)",
      i,
      response->basket_colours[i].c_str(),
      request->basket_locs[i].point.x,
      request->basket_locs[i].point.y);
  }

  RCLCPP_INFO(node_->get_logger(), "Task 2 complete");
}

///////////////////////////////////////////////////////////////////////////////

void
cw1::t3_callback(
  const std::shared_ptr<cw1_world_spawner::srv::Task3Service::Request> request,
  std::shared_ptr<cw1_world_spawner::srv::Task3Service::Response> response)
{
  (void)request;
  (void)response;

  struct ClusterPoint
  {
    double x;
    double y;
    double z;
    uint8_t r;
    uint8_t g;
    uint8_t b;
    std::string seed_colour;
  };

  struct RawDetection
  {
    std::string colour;
    geometry_msgs::msg::Point position;

    double size_x;
    double size_y;
    double size_z;

    double top_z;
    double centre_top_z;
    double rim_top_z;
    double rim_gap;
    double centre_fill_ratio;

    double mean_r;
    double mean_g;
    double mean_b;

    size_t point_count;
    size_t red_votes;
    size_t blue_votes;
    size_t purple_votes;
  };

  struct MergedObject
  {
    std::string colour;
    geometry_msgs::msg::Point position;

    double size_x;
    double size_y;
    double size_z;

    double max_rim_gap;
    double min_centre_fill_ratio;

    double mean_r;
    double mean_g;
    double mean_b;

    size_t point_count;
    size_t red_votes;
    size_t blue_votes;
    size_t purple_votes;

    size_t observations;
  };

  auto make_scan_pose =
    [this](double x, double y, double z) -> geometry_msgs::msg::Pose
    {
      geometry_msgs::msg::Pose pose;
      pose.position.x = x;
      pose.position.y = y;
      pose.position.z = z;
      pose.orientation = make_top_down_q();
      return pose;
    };

  auto get_latest_cloud_blocking =
    [this](double timeout_sec) -> sensor_msgs::msg::PointCloud2::ConstSharedPtr
    {
      sensor_msgs::msg::PointCloud2::ConstSharedPtr cloud;
      const auto deadline =
        node_->now() + rclcpp::Duration::from_seconds(timeout_sec);

      while (rclcpp::ok()) {
        {
          std::lock_guard<std::mutex> lock(cloud_mutex_);
          if (latest_cloud_) {
            cloud = latest_cloud_;
          }
        }

        if (cloud) {
          return cloud;
        }

        if (node_->now() > deadline) {
          break;
        }

        rclcpp::sleep_for(std::chrono::milliseconds(100));
      }

      return nullptr;
    };

  auto classify_seed_colour = [](uint8_t r, uint8_t g, uint8_t b) -> std::string {
    if (r > 55 && b > 55 && g < 0.90 * std::min(r, b)) {
      return "purple";
    }
    if (r > 60 && r > 1.20 * g && r > 1.10 * b) {
      return "red";
    }
    if (b > 60 && b > 1.20 * g && b > 1.10 * r) {
      return "blue";
    }
    return "none";
  };

  auto dominant_colour_from_top_points =
    [](const std::vector<ClusterPoint> &cluster, double top_z,
       size_t &red_votes, size_t &blue_votes, size_t &purple_votes,
       double &mean_r, double &mean_g, double &mean_b) -> std::string
    {
      red_votes = 0;
      blue_votes = 0;
      purple_votes = 0;
      mean_r = 0.0;
      mean_g = 0.0;
      mean_b = 0.0;

      size_t used = 0;
      const double top_band = 0.015;

      for (const auto &p : cluster) {
        if (p.z < top_z - top_band) {
          continue;
        }

        mean_r += static_cast<double>(p.r);
        mean_g += static_cast<double>(p.g);
        mean_b += static_cast<double>(p.b);
        used++;

        if (p.r > 55 && p.b > 55 && p.g < 0.90 * std::min(p.r, p.b)) {
          purple_votes++;
        } else if (p.r > 60 && p.r > 1.20 * p.g && p.r > 1.10 * p.b) {
          red_votes++;
        } else if (p.b > 60 && p.b > 1.20 * p.g && p.b > 1.10 * p.r) {
          blue_votes++;
        }
      }

      if (used > 0) {
        mean_r /= static_cast<double>(used);
        mean_g /= static_cast<double>(used);
        mean_b /= static_cast<double>(used);
      }

      if (red_votes == 0 && blue_votes == 0 && purple_votes == 0) {
        return "none";
      }

      if (purple_votes >= red_votes && purple_votes >= blue_votes) {
        return "purple";
      }
      if (red_votes >= blue_votes && red_votes >= purple_votes) {
        return "red";
      }
      return "blue";
    };

  auto detect_raw_objects_from_cloud =
    [this, &classify_seed_colour, &dominant_colour_from_top_points](
      const sensor_msgs::msg::PointCloud2::ConstSharedPtr &cloud)
      -> std::vector<RawDetection>
    {
      std::vector<RawDetection> objects;

      pcl::PointCloud<pcl::PointXYZRGB>::Ptr pcl_cloud(
        new pcl::PointCloud<pcl::PointXYZRGB>);
      pcl::fromROSMsg(*cloud, *pcl_cloud);

      geometry_msgs::msg::TransformStamped tf_cloud_to_world;
      try {
        tf_cloud_to_world = tf_buffer_->lookupTransform(
          "world",
          cloud->header.frame_id,
          rclcpp::Time(0),
          tf2::durationFromSec(1.0));
      } catch (const tf2::TransformException &ex) {
        RCLCPP_WARN(
          node_->get_logger(),
          "Task 3: could not transform cloud to world: %s",
          ex.what());
        return objects;
      }

      tf2::Quaternion q(
        tf_cloud_to_world.transform.rotation.x,
        tf_cloud_to_world.transform.rotation.y,
        tf_cloud_to_world.transform.rotation.z,
        tf_cloud_to_world.transform.rotation.w);

      tf2::Vector3 t(
        tf_cloud_to_world.transform.translation.x,
        tf_cloud_to_world.transform.translation.y,
        tf_cloud_to_world.transform.translation.z);

      tf2::Transform tf_c2w(q, t);

      std::vector<ClusterPoint> pts;
      pts.reserve(pcl_cloud->points.size());

      for (const auto &pt : pcl_cloud->points) {
        if (!std::isfinite(pt.x) || !std::isfinite(pt.y) || !std::isfinite(pt.z)) {
          continue;
        }

        const std::string seed_colour = classify_seed_colour(pt.r, pt.g, pt.b);
        if (seed_colour == "none") {
          continue;
        }

        tf2::Vector3 p_cloud(pt.x, pt.y, pt.z);
        tf2::Vector3 p_world = tf_c2w * p_cloud;

        if (p_world.x() < 0.20 || p_world.x() > 0.75) {
          continue;
        }
        if (p_world.y() < -0.45 || p_world.y() > 0.45) {
          continue;
        }
        if (p_world.z() < -0.02 || p_world.z() > 0.20) {
          continue;
        }

        pts.push_back({
          p_world.x(), p_world.y(), p_world.z(),
          pt.r, pt.g, pt.b, seed_colour
        });
      }

      if (pts.empty()) {
        return objects;
      }

      std::vector<bool> visited(pts.size(), false);

      constexpr double kClusterRadius = 0.055;
      constexpr double kClusterRadiusSq = kClusterRadius * kClusterRadius;
      constexpr double kMaxDz = 0.06;
      constexpr size_t kMinClusterSize = 18;

      for (size_t i = 0; i < pts.size(); ++i) {
        if (visited[i]) {
          continue;
        }

        visited[i] = true;
        std::queue<size_t> qidx;
        qidx.push(i);

        std::vector<size_t> cluster_indices;
        cluster_indices.push_back(i);

        while (!qidx.empty()) {
          const size_t cur = qidx.front();
          qidx.pop();

          for (size_t j = 0; j < pts.size(); ++j) {
            if (visited[j]) {
              continue;
            }

            if (pts[j].seed_colour != pts[cur].seed_colour) {
              continue;
            }

            const double dx = pts[j].x - pts[cur].x;
            const double dy = pts[j].y - pts[cur].y;
            const double dz = std::fabs(pts[j].z - pts[cur].z);

            if ((dx * dx + dy * dy) < kClusterRadiusSq && dz < kMaxDz) {
              visited[j] = true;
              qidx.push(j);
              cluster_indices.push_back(j);
            }
          }
        }

        if (cluster_indices.size() < kMinClusterSize) {
          continue;
        }

        std::vector<ClusterPoint> cluster;
        cluster.reserve(cluster_indices.size());

        double min_x = std::numeric_limits<double>::max();
        double min_y = std::numeric_limits<double>::max();
        double min_z = std::numeric_limits<double>::max();
        double max_x = -std::numeric_limits<double>::max();
        double max_y = -std::numeric_limits<double>::max();
        double max_z = -std::numeric_limits<double>::max();

        double sum_x = 0.0;
        double sum_y = 0.0;
        double sum_z = 0.0;

        for (const auto idx : cluster_indices) {
          const auto &p = pts[idx];
          cluster.push_back(p);

          min_x = std::min(min_x, p.x);
          min_y = std::min(min_y, p.y);
          min_z = std::min(min_z, p.z);
          max_x = std::max(max_x, p.x);
          max_y = std::max(max_y, p.y);
          max_z = std::max(max_z, p.z);

          sum_x += p.x;
          sum_y += p.y;
          sum_z += p.z;
        }

        const double cx = sum_x / static_cast<double>(cluster.size());
        const double cy = sum_y / static_cast<double>(cluster.size());
        const double cz = sum_z / static_cast<double>(cluster.size());

        const double size_x = max_x - min_x;
        const double size_y = max_y - min_y;
        const double size_z = max_z - min_z;
        const double max_xy = std::max(size_x, size_y);

        const double half_extent = 0.5 * max_xy;
        const double inner_radius = std::max(0.012, 0.30 * half_extent);
        const double outer_r_min = std::max(0.015, 0.45 * half_extent);
        const double outer_r_max = std::max(0.025, 0.85 * half_extent);

        double centre_top_z = -std::numeric_limits<double>::max();
        double rim_top_z = -std::numeric_limits<double>::max();
        size_t centre_count = 0;
        size_t rim_count = 0;

        for (const auto &p : cluster) {
          const double dx = p.x - cx;
          const double dy = p.y - cy;
          const double rr = std::sqrt(dx * dx + dy * dy);

          if (rr <= inner_radius) {
            centre_count++;
            centre_top_z = std::max(centre_top_z, p.z);
          }

          if (rr >= outer_r_min && rr <= outer_r_max) {
            rim_count++;
            rim_top_z = std::max(rim_top_z, p.z);
          }
        }

        if (centre_top_z < -1e8) {
          centre_top_z = max_z;
        }
        if (rim_top_z < -1e8) {
          rim_top_z = max_z;
        }

        const double rim_gap = rim_top_z - centre_top_z;
        const double centre_fill_ratio =
          static_cast<double>(centre_count) / static_cast<double>(cluster.size());

        size_t red_votes = 0;
        size_t blue_votes = 0;
        size_t purple_votes = 0;
        double mean_r = 0.0;
        double mean_g = 0.0;
        double mean_b = 0.0;

        const std::string final_colour =
          dominant_colour_from_top_points(
            cluster, max_z,
            red_votes, blue_votes, purple_votes,
            mean_r, mean_g, mean_b);

        if (final_colour == "none") {
          continue;
        }

        RawDetection obj;
        obj.colour = final_colour;
        obj.position.x = cx;
        obj.position.y = cy;
        obj.position.z = cz;

        obj.size_x = size_x;
        obj.size_y = size_y;
        obj.size_z = size_z;

        obj.top_z = max_z;
        obj.centre_top_z = centre_top_z;
        obj.rim_top_z = rim_top_z;
        obj.rim_gap = rim_gap;
        obj.centre_fill_ratio = centre_fill_ratio;

        obj.mean_r = mean_r;
        obj.mean_g = mean_g;
        obj.mean_b = mean_b;

        obj.point_count = cluster.size();
        obj.red_votes = red_votes;
        obj.blue_votes = blue_votes;
        obj.purple_votes = purple_votes;

        objects.push_back(obj);
      }

      return objects;
    };

  auto merge_raw_detections =
    [](std::vector<MergedObject> &all_objects,
       const std::vector<RawDetection> &new_objects)
    {
      constexpr double kMergeXY = 0.09;
      constexpr double kMergeZ = 0.06;

      for (const auto &obj : new_objects) {
        bool merged = false;

        for (auto &existing : all_objects) {
          if (existing.colour != obj.colour) {
            continue;
          }

          const double dx = existing.position.x - obj.position.x;
          const double dy = existing.position.y - obj.position.y;
          const double dz = std::fabs(existing.position.z - obj.position.z);
          const double dxy = std::sqrt(dx * dx + dy * dy);

          if (dxy < kMergeXY && dz < kMergeZ) {
            const double n = static_cast<double>(existing.observations);

            existing.position.x =
              (existing.position.x * n + obj.position.x) / (n + 1.0);
            existing.position.y =
              (existing.position.y * n + obj.position.y) / (n + 1.0);
            existing.position.z =
              (existing.position.z * n + obj.position.z) / (n + 1.0);

            existing.size_x = std::max(existing.size_x, obj.size_x);
            existing.size_y = std::max(existing.size_y, obj.size_y);
            existing.size_z = std::max(existing.size_z, obj.size_z);

            existing.max_rim_gap = std::max(existing.max_rim_gap, obj.rim_gap);
            existing.min_centre_fill_ratio =
              std::min(existing.min_centre_fill_ratio, obj.centre_fill_ratio);

            existing.mean_r = (existing.mean_r * n + obj.mean_r) / (n + 1.0);
            existing.mean_g = (existing.mean_g * n + obj.mean_g) / (n + 1.0);
            existing.mean_b = (existing.mean_b * n + obj.mean_b) / (n + 1.0);

            existing.point_count = std::max(existing.point_count, obj.point_count);
            existing.red_votes += obj.red_votes;
            existing.blue_votes += obj.blue_votes;
            existing.purple_votes += obj.purple_votes;
            existing.observations++;

            merged = true;
            break;
          }
        }

        if (!merged) {
          MergedObject mo;
          mo.colour = obj.colour;
          mo.position = obj.position;

          mo.size_x = obj.size_x;
          mo.size_y = obj.size_y;
          mo.size_z = obj.size_z;

          mo.max_rim_gap = obj.rim_gap;
          mo.min_centre_fill_ratio = obj.centre_fill_ratio;

          mo.mean_r = obj.mean_r;
          mo.mean_g = obj.mean_g;
          mo.mean_b = obj.mean_b;

          mo.point_count = obj.point_count;
          mo.red_votes = obj.red_votes;
          mo.blue_votes = obj.blue_votes;
          mo.purple_votes = obj.purple_votes;
          mo.observations = 1;

          all_objects.push_back(mo);
        }
      }
    };

  auto classify_final_type =
    [](const MergedObject &obj) -> std::string
    {
      const double max_xy = std::max(obj.size_x, obj.size_y);

      // Small objects are cubes.
      if (max_xy < 0.070) {
        return "cube";
      }

      // Large objects are very likely baskets. Use hollow evidence to confirm.
      // if (max_xy > 0.085) {
      //   return "basket";
      // }

      // Middle range: use structure.
      if (obj.max_rim_gap > 0.012 && obj.min_centre_fill_ratio < 0.18) {
        return "basket";
      }

      return "cube";
    };

  RCLCPP_INFO(node_->get_logger(), "Task 3: scan stage started");

  std::vector<geometry_msgs::msg::Pose> scan_poses;
  scan_poses.push_back(make_scan_pose(0.30, -0.30, 0.65));
  scan_poses.push_back(make_scan_pose(0.50, -0.30, 0.65));
  scan_poses.push_back(make_scan_pose(0.50,  0.00, 0.65));
  scan_poses.push_back(make_scan_pose(0.30,  0.00, 0.65));
  scan_poses.push_back(make_scan_pose(0.30,  0.30, 0.65));
  scan_poses.push_back(make_scan_pose(0.50,  0.30, 0.65));

  std::vector<MergedObject> all_objects;

  for (size_t i = 0; i < scan_poses.size(); ++i) {
    RCLCPP_INFO(node_->get_logger(), "Task 3: moving to scan pose %zu", i);

    if (!move_arm_to_pose(scan_poses[i])) {
      RCLCPP_WARN(node_->get_logger(), "Task 3: failed to reach scan pose %zu", i);
      continue;
    }

    rclcpp::sleep_for(std::chrono::milliseconds(700));

    for (int frame_idx = 0; frame_idx < 4; ++frame_idx) {
      auto cloud = get_latest_cloud_blocking(1.5);
      if (!cloud) {
        RCLCPP_WARN(
          node_->get_logger(),
          "Task 3: no cloud at scan pose %zu frame %d",
          i, frame_idx);
        continue;
      }

      const auto detections = detect_raw_objects_from_cloud(cloud);
      merge_raw_detections(all_objects, detections);

      RCLCPP_INFO(
        node_->get_logger(),
        "Task 3: pose %zu frame %d found %zu object(s), total merged = %zu",
        i, frame_idx, detections.size(), all_objects.size());

      rclcpp::sleep_for(std::chrono::milliseconds(150));
    }
  }

  if (all_objects.empty()) {
    RCLCPP_WARN(node_->get_logger(), "Task 3: no objects detected");
    return;
  }

  size_t cube_count = 0;
  size_t basket_count = 0;

  RCLCPP_INFO(node_->get_logger(), "Task 3: final detections");
  for (const auto &obj : all_objects) {
    const std::string type = classify_final_type(obj);

    if (type == "cube") {
      cube_count++;
    } else {
      basket_count++;
    }

    RCLCPP_INFO(
      node_->get_logger(),
      "  %s %s at (%.3f, %.3f, %.3f), size=(%.3f, %.3f, %.3f), "
      "max_rim_gap=%.3f, min_centre_fill=%.3f, meanRGB=(%.1f, %.1f, %.1f), "
      "votes[R/B/P]=(%zu, %zu, %zu), points=%zu, obs=%zu",
      obj.colour.c_str(),
      type.c_str(),
      obj.position.x,
      obj.position.y,
      obj.position.z,
      obj.size_x,
      obj.size_y,
      obj.size_z,
      obj.max_rim_gap,
      obj.min_centre_fill_ratio,
      obj.mean_r,
      obj.mean_g,
      obj.mean_b,
      obj.red_votes,
      obj.blue_votes,
      obj.purple_votes,
      obj.point_count,
      obj.observations);
  }

  RCLCPP_INFO(
    node_->get_logger(),
    "Task 3: scan complete, %zu cube(s), %zu basket(s)",
    cube_count, basket_count);
}