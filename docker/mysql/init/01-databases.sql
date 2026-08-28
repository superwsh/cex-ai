-- ============================================================
-- CEX 平台数据库初始化（MySQL 首次启动自动执行）
-- 按服务维度分库：每个服务独立 schema，为分库分表预留空间
-- ============================================================

CREATE DATABASE IF NOT EXISTS cex_user DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS cex_order DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS cex_asset DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS cex_account DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 订单库：分表预留（t_order_0 ~ t_order_7，ShardingSphere 配置启用后使用）
-- 分表键：user_id（HASH_MOD）
