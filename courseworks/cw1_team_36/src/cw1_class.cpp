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
#include <tf2/LinearMath/Quaternion.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>

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
  // Keep Cartesian motion conservative for debugging stability.
  const double safe_eef_step = std::clamp(eef_step, 0.001, 0.02);
  const double jump_threshold = 1.0;  // enable joint-jump checking (0 disables it)

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

  geometry_msgs::msg::Pose grasp = pre_grasp;
  grasp.position.z -= 0.24;  // 先固定下探 6cm

  geometry_msgs::msg::Pose lift = grasp;
  lift.position.z += post_grasp_lift_z_;

  geometry_msgs::msg::Pose pre_place;
  pre_place.position.x = goal_point.x;
  pre_place.position.y = goal_point.y;
  pre_place.position.z = goal_point.z + place_offset_z_;
  pre_place.orientation = top_down_q;

  // Place height is lower than pre-place, but still above basket bottom.
  geometry_msgs::msg::Pose place = pre_place;
  place.position.z = goal_point.z + 0.10;

  geometry_msgs::msg::Pose retreat = pre_place;

  // Execute sequence with explicit failure checks.
  bool ok = true;
  ok = ok && set_gripper_width(0.07);                      // ensure open before approach
  ok = ok && move_arm_to_pose(pre_grasp);                  // approach
  ok = ok && move_arm_to_pose(grasp);                    // descend
  ok = ok && set_gripper_width(gripper_grasp_width_);   // close to grasp cube
                      // lift clear of scene
  ok = ok && move_arm_to_pose(pre_grasp);               // move above basket
  // ok = ok && move_arm_to_pose(lift);
  ok = ok && move_arm_to_pose(pre_place);                   // descend to release pose
  ok = ok && set_gripper_width(0.07);                   // release
  // ok = ok && move_arm_to_pose(retreat);                 // leave basket area

  if (ok) {
    RCLCPP_INFO(node_->get_logger(), "Task 1 execution finished successfully");
  } else {
    RCLCPP_WARN(node_->get_logger(), "Task 1 sequence finished with at least one failure");
  }
}

/////////ok = ok && move_arm_to_pose(grasp); //////////////////////////////////////////////////////////////////////

void
cw1::t2_callback(
  const std::shared_ptr<cw1_world_spawner::srv::Task2Service::Request> request,
  std::shared_ptr<cw1_world_spawner::srv::Task2Service::Response> response)
{
  /* function which should solve task 2 */

  (void)request;
  (void)response;
  RCLCPP_INFO_STREAM(
    node_->get_logger(),
    "Task 2 callback triggered (template stub). joint_msgs=" <<
      joint_state_msg_count_.load(std::memory_order_relaxed) <<
      ", cloud_msgs=" << cloud_msg_count_.load(std::memory_order_relaxed));
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
