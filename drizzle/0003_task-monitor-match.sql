CREATE TABLE `task_logs` (
	`id` bigint unsigned AUTO_INCREMENT NOT NULL,
	`task_id` varchar(36) NOT NULL,
	`level` enum('info','warn','error') NOT NULL DEFAULT 'info',
	`phase` varchar(64) NOT NULL,
	`message` text NOT NULL,
	`data` json,
	`created_at` datetime(3) NOT NULL,
	CONSTRAINT `task_logs_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
ALTER TABLE `jobs` MODIFY COLUMN `type` enum('validate','scene_detect','persist','analyze','embed','delete','cleanup','publish','update','retry','callback','match') NOT NULL;--> statement-breakpoint
ALTER TABLE `tasks` MODIFY COLUMN `type` enum('upload','delete','publish','update','retry','match') NOT NULL;--> statement-breakpoint
ALTER TABLE `tasks` ADD `input` json;--> statement-breakpoint
ALTER TABLE `task_logs` ADD CONSTRAINT `task_logs_task_id_tasks_id_fk` FOREIGN KEY (`task_id`) REFERENCES `tasks`(`id`) ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX `task_logs_task_id_idx` ON `task_logs` (`task_id`,`id`);