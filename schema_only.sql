-- ----------------------------
-- Table structure for `product_details`
-- ----------------------------
CREATE TABLE `product_details` (
  `entry_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scrape_run_id` varchar(36) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id` bigint(20) DEFAULT NULL,
  `category_url` text,
  `product_url` text,
  `name` text,
  `sku` varchar(255) DEFAULT NULL,
  `price` varchar(100) DEFAULT NULL,
  `discounted_price` varchar(100) DEFAULT NULL,
  `sizes_available` text,
  `main_image_urls` text,
  `max_allowed_qty` varchar(100) DEFAULT NULL,
  `is_out_of_stock` varchar(50) DEFAULT NULL,
  `color_name` varchar(255) DEFAULT NULL,
  `category_name` varchar(255) DEFAULT NULL,
  `gender` enum('boys','girls') DEFAULT NULL,
  `gender_code` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`entry_id`),
  KEY `idx_pdet_run` (`scrape_run_id`),
  KEY `idx_pdet_source_id` (`id`),
  KEY `idx_gender` (`gender`),
  CONSTRAINT `fk_pdet_run` FOREIGN KEY (`scrape_run_id`) REFERENCES `scrape_runs` (`scrape_run_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=25388 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for `products_from_category`
-- ----------------------------
CREATE TABLE `products_from_category` (
  `entry_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scrape_run_id` varchar(36) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id` bigint(20) DEFAULT NULL,
  `sku` varchar(255) DEFAULT NULL,
  `price` varchar(100) DEFAULT NULL,
  `discountedPrice` varchar(100) DEFAULT NULL,
  `discount` varchar(100) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `imageUrl` text,
  `categoryId` varchar(255) DEFAULT NULL,
  `title` text,
  PRIMARY KEY (`entry_id`),
  KEY `idx_pfcat_run` (`scrape_run_id`),
  KEY `idx_pfcat_source_id` (`id`),
  CONSTRAINT `fk_pfcat_run` FOREIGN KEY (`scrape_run_id`) REFERENCES `scrape_runs` (`scrape_run_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=41897 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for `scrape_runs`
-- ----------------------------
CREATE TABLE `scrape_runs` (
  `scrape_run_id` varchar(36) NOT NULL,
  `started_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `finished_at` datetime DEFAULT NULL,
  `listing_urls` longtext,
  `product_count` int(11) DEFAULT NULL,
  `details_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`scrape_run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

