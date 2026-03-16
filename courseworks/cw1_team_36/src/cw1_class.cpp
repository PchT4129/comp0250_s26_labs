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

  // Search in the x-y plane of the cloud frame within a circle whose radius is
  // slightly larger than half the basket diagonal (basket = 0.1x0.1 m).
  const float cx = static_cast<float>(basket_in_cloud.point.x);
  const float cy = static_cast<float>(basket_in_cloud.point.y);
  constexpr float kSearchRadius = 0.08f;   // 8 cm
  constexpr float kSearchRadiusSq = kSearchRadius * kSearchRadius;

  // Known basket colours from the coursework spec (normalised 0-1).
  // Using squared distance avoids std::sqrt for the comparison.
  auto colour_dist_sq = [](float r, float g, float b,
                            float tr, float tg, float tb) {
    return (r - tr) * (r - tr) + (g - tg) * (g - tg) + (b - tb) * (b - tb);
  };
  constexpr float kColourThreshSq = 0.09f;  // max distance of 0.3 in RGB space

  std::map<std::string, int> votes;

  for (const auto &pt : pcl_cloud->points) {
    if (!std::isfinite(pt.x) || !std::isfinite(pt.y) || !std::isfinite(pt.z)) {
      continue;
    }
    const float dx = pt.x - cx;
    const float dy = pt.y - cy;
    if (dx * dx + dy * dy > kSearchRadiusSq) {
      continue;
    }

    const float r = pt.r / 255.0f;
    const float g = pt.g / 255.0f;
    const float b = pt.b / 255.0f;

    const float d_red    = colour_dist_sq(r, g, b, 0.8f, 0.1f, 0.1f);
    const float d_blue   = colour_dist_sq(r, g, b, 0.1f, 0.1f, 0.8f);
    const float d_purple = colour_dist_sq(r, g, b, 0.8f, 0.1f, 0.8f);

    const float min_d = std::min({d_red, d_blue, d_purple});
    if (min_d > kColourThreshSq) {
      continue;  // not close enough to any known basket colour
    }

    if (min_d == d_red)         { votes["red"]++; }
    else if (min_d == d_blue)   { votes["blue"]++; }
    else                        { votes["purple"]++; }
  }

  if (votes.empty()) {
    return "none";
  }

  const auto best = std::max_element(
    votes.begin(), votes.end(),
    [](const auto &a, const auto &b) { return a.second < b.second; });

  RCLCPP_INFO(node_->get_logger(),
    "Basket at (%.3f, %.3f): %s (%d votes)",
    basket_loc.point.x, basket_loc.point.y,
    best->first.c_str(), best->second);

  return best->first;
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
  arm_group_->setMaxVelocityScalingFactor(0.10);
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
  // 5) descend to place
  // 6) retreat up
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

  // place: lower into the basket while keeping the same x/y/orientation.
  geometry_msgs::msg::Pose place = pre_place;
  place.position.z = goal_point.z + 0.10;

  // retreat: rise back to pre_place height after releasing the cube.
  geometry_msgs::msg::Pose retreat = pre_place;

  // Execute pick-and-place sequence.
  // Each step short-circuits on failure so we never execute in a bad state.
  bool ok = true;
  ok = ok && set_gripper_width(0.07);                       // open gripper
  ok = ok && move_arm_to_pose(pre_grasp);                   // global plan: move above object
  ok = ok && move_arm_linear_to(grasp);                     // straight down to grasp height
  ok = ok && set_gripper_width(gripper_grasp_width_);       // close gripper
  ok = ok && move_arm_linear_to(lift);                      // straight up back to safe height
  ok = ok && move_arm_to_pose(pre_place);                   // global plan: transit to above basket
  ok = ok && move_arm_linear_to(place);                     // straight down into basket
  ok = ok && set_gripper_width(0.07);                       // release cube
  ok = ok && move_arm_linear_to(retreat);                   // straight up out of basket

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
    // ── 1. Move camera to a position directly above this basket ──────────
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

    // ── 2. Wait for the camera to settle and deliver a fresh frame ────────
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

    // ── 3. Identify colour from the point cloud ───────────────────────────
    const std::string colour = identify_basket_colour(cloud, basket_loc);
    response->basket_colours.push_back(colour);
  }

  RCLCPP_INFO(node_->get_logger(), "Task 2 complete");
}

///////////////////////////////////////////////////////////////////////////////

void
cw1::t3_callback(
  const std::shared_ptr<cw1_world_spawner::srv::Task3Service::Request> request,
  std::shared_ptr<cw1_world_spawner::srv::Task3Service::Response> response)
{
  /* function which should solve task 3 */

  (void)request;
  (void)response;
  RCLCPP_INFO_STREAM(
    node_->get_logger(),
    "Task 3 callback triggered (template stub). joint_msgs=" <<
      joint_state_msg_count_.load(std::memory_order_relaxed) <<
      ", cloud_msgs=" << cloud_msg_count_.load(std::memory_order_relaxed));
}
