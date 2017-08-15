DROP DATABASE IF EXISTS `iota`;
CREATE DATABASE `iota` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `iota`;

--
-- Table structure for table `address`
--

DROP TABLE IF EXISTS `address`;
CREATE TABLE `address` (
  `id_address` int(11) NOT NULL UNIQUE,
  `address` char(81) NOT NULL,
  `checksum` char(9) DEFAULT NULL,
  PRIMARY KEY (`id_address`),
  UNIQUE KEY `address_UNIQUE` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Table structure for table `bundle`
--

DROP TABLE IF EXISTS `bundle`;
CREATE TABLE `bundle` (
  `id_bundle` int(11) NOT NULL UNIQUE,
  `bundle` char(81) NOT NULL,
  `size` int(11) NOT NULL DEFAULT '0',
  `created` double DEFAULT NULL,
  `confirmed` double DEFAULT NULL,
  PRIMARY KEY (`id_bundle`),
  KEY `bundle_INDEX` (`bundle`),
  KEY `created_INDEX` (`created`),
  KEY `confirmed_INDEX` (`confirmed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Table structure for table `txload`
--

DROP TABLE IF EXISTS `txload`;
CREATE TABLE `txload` (
  `id_txload` int(11) NOT NULL AUTO_INCREMENT,
  `event` char(3) NOT NULL,
  `count` int(11) NOT NULL DEFAULT 1,
  `timestamp` double NOT NULL,
  PRIMARY KEY (`id_txload`),
  KEY `event_INDEX` (`event`),
  KEY `timestamp_INDEX` (`timestamp`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

--
-- Table structure for table `tx`
--

DROP TABLE IF EXISTS `tx`;
CREATE TABLE `tx` (
  `id_tx` int(11) NOT NULL UNIQUE,
  `hash` char(81) NOT NULL,
  `id_trunk` int(11) DEFAULT NULL,
  `id_branch` int(11) DEFAULT NULL,
  `id_address` int(11) DEFAULT NULL,
  `id_bundle` int(11) DEFAULT NULL,
  `tag` char(27) DEFAULT NULL,
  `value` bigint(14) DEFAULT '0',
  `timestamp` double DEFAULT '0',
  `arrival` double DEFAULT '0',
  `conftime` double DEFAULT '0',
  `current_idx` int(11) DEFAULT NULL,
  `last_idx` int(11) DEFAULT NULL,
  `da` int(11) DEFAULT '0',
  `height` int(11) DEFAULT '0',
  `weight` double DEFAULT '0',
  `is_mst` char(1) DEFAULT '0',
  `mst_a` char(1) DEFAULT '0',
  `solid` char(1) DEFAULT '0',
  PRIMARY KEY (`id_tx`),
  UNIQUE KEY `hash_UNIQUE` (`hash`) USING BTREE,
  KEY `id_trunk_INDEX` (`id_trunk`),
  KEY `id_branch_INDEX` (`id_branch`),
  KEY `id_address_INDEX` (`id_address`),
  KEY `id_bundle_INDEX` (`id_bundle`),
  KEY `da_INDEX` (`da`),
  KEY `is_mst_INDEX` (`is_mst`),
  KEY `mst_a_INDEX` (`mst_a`)
) ENGINE=InnoDB CHARSET=utf8 KEY_BLOCK_SIZE=2;

