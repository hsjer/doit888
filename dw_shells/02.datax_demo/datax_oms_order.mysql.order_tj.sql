/*
Navicat MySQL Data Transfer

Source Server         : doitedu01_mysql
Source Server Version : 50744
Source Host           : doitedu01:3306
Source Database       : doit50

Target Server Type    : MYSQL
Target Server Version : 50744
File Encoding         : 65001

Date: 2024-10-30 17:54:23
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for order_tj
-- ----------------------------
DROP TABLE IF EXISTS `order_tj`;
CREATE TABLE `order_tj` (
  `pid` int(11) NOT NULL,
  `order_count` int(11) DEFAULT NULL,
  `user_count` int(11) DEFAULT NULL,
  `amt` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`pid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of order_tj
-- ----------------------------
INSERT INTO `order_tj` VALUES ('1', '2', '1', '238.80');
INSERT INTO `order_tj` VALUES ('2', '4', '3', '1049.20');
INSERT INTO `order_tj` VALUES ('3', '2', '2', '147.60');
