# Python_Report_Generation_Application
A multi-threaded application capable of generating reports on the basis of CSV Files.
The required CSV files should also be added to the main parent directory (where the main.py file resides).


## TABLES TO BE ADDED
The following tables should first be added to the local (or any other) database, since they are being accessed from within the code:

CREATE TABLE `menu_hours` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `store_id` bigint unsigned NOT NULL,
  `day` tinyint unsigned NOT NULL,
  `start_time_local` time NOT NULL,
  `end_time_local` time NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=86199 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `report_generation_status` (
  `report_id` varchar(60) NOT NULL,
  `report_status` text NOT NULL,
  PRIMARY KEY (`report_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `report_id_mapping` (
  `report_id` varchar(60) NOT NULL,
  `filename` text NOT NULL,
  PRIMARY KEY (`report_id`),
  UNIQUE KEY `report_filename_idx` (`report_id`,`filename`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `store_status` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `store_id` bigint unsigned NOT NULL,
  `timestamp_utc` timestamp NOT NULL,
  `status` varchar(45) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1822081 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `store_timezone` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `store_id` bigint unsigned NOT NULL,
  `timezone_str` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13560 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

## PREVIEW 
![image](https://user-images.githubusercontent.com/122728136/234468195-2487c945-e7f8-4a16-b39f-fd84d01f348c.png)

![image](https://user-images.githubusercontent.com/122728136/234468278-943d5562-de3a-409a-a7c1-b886830d97f5.png)

![image](https://user-images.githubusercontent.com/122728136/234468301-22c1a4aa-a1f9-487b-b9c8-3f05fa8b27dd.png)

![image](https://user-images.githubusercontent.com/122728136/234468325-82b0c036-1633-4c2e-a1fb-4ce75fa7573a.png)

