/*
Navicat MySQL Data Transfer

Source Server         : doitedu01_mysql
Source Server Version : 50744
Source Host           : doitedu01:3306
Source Database       : dw_50

Target Server Type    : MYSQL
Target Server Version : 50744
File Encoding         : 65001

Date: 2024-10-29 17:13:46
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for dq_applog_detail
-- ----------------------------
DROP TABLE IF EXISTS `dq_applog_detail`;
CREATE TABLE `dq_applog_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_server` varchar(255) DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `line_cnt` int(11) DEFAULT NULL,
  `target_dt` varchar(255) DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of dq_applog_detail
-- ----------------------------
INSERT INTO `dq_applog_detail` VALUES ('1', 'doitedu01', '/opt/data/app_log/log_2024-10-28_app.log', '100000', '2024-10-28', '2024-10-29 17:03:51');
INSERT INTO `dq_applog_detail` VALUES ('2', 'doitedu01', '/opt/data/app_log/log_2024-10-28_app.log.1', '200000', '2024-10-28', '2024-10-29 17:03:56');
INSERT INTO `dq_applog_detail` VALUES ('3', 'doitedu02', '/opt/data/app_log/log_2024-10-28_app.log', '100000', '2024-10-28', '2024-10-29 17:04:00');
INSERT INTO `dq_applog_detail` VALUES ('4', 'doitedu02', '/opt/data/app_log/log_2024-10-28_app.log.1', '150000', '2024-10-28', '2024-10-29 17:04:04');
INSERT INTO `dq_applog_detail` VALUES ('5', 'doitedu03', '/opt/data/app_log/log_2024-10-28_app.log', '100000', '2024-10-28', '2024-10-29 17:04:15');
INSERT INTO `dq_applog_detail` VALUES ('6', 'doitedu03', '/opt/data/app_log/log_2024-10-28_app.log.1', '200000', '2024-10-28', '2024-10-29 17:04:16');
INSERT INTO `dq_applog_detail` VALUES ('7', 'doitedu03', '/opt/data/app_log/log_2024-10-28_app.log.2', '300000', '2024-10-28', '2024-10-29 17:04:17');

-- ----------------------------
-- Table structure for dq_applog_hdfs
-- ----------------------------
DROP TABLE IF EXISTS `dq_applog_hdfs`;
CREATE TABLE `dq_applog_hdfs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `target_dt` varchar(255) DEFAULT NULL,
  `file_cnt` int(255) DEFAULT NULL,
  `lines_amt` int(255) DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of dq_applog_hdfs
-- ----------------------------
INSERT INTO `dq_applog_hdfs` VALUES ('1', '2024-10-28', '5', '1156000', '2024-10-29 15:03:12');

-- ----------------------------
-- Table structure for dq_applog_ods
-- ----------------------------
DROP TABLE IF EXISTS `dq_applog_ods`;
CREATE TABLE `dq_applog_ods` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(255) DEFAULT NULL,
  `table_name` varchar(255) DEFAULT NULL,
  `partition_name` varchar(255) DEFAULT NULL,
  `lines_amt` int(11) DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of dq_applog_ods
-- ----------------------------
INSERT INTO `dq_applog_ods` VALUES ('1', 'ods', 'user_action_log', '2024-10-28', '1150000', '2024-10-29 17:10:09');
