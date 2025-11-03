-- Facebook CSV ingestion tables
-- Run inside your Railway MySQL database.

CREATE TABLE IF NOT EXISTS fb_csv_uploads (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    uploaded_by BIGINT NULL,
    buyer_id BIGINT NULL,
    original_filename VARCHAR(255) NOT NULL,
    period_start DATE NULL,
    period_end DATE NULL,
    row_count INT NOT NULL DEFAULT 0,
    has_totals TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fb_csv_upload_user  FOREIGN KEY (uploaded_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_csv_upload_buyer FOREIGN KEY (buyer_id)   REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_csv_rows (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    upload_id BIGINT NOT NULL,
    account_name VARCHAR(255) NULL,
    campaign_name VARCHAR(255) NOT NULL,
    adset_name VARCHAR(255) NULL,
    ad_name VARCHAR(255) NULL,
    day_date DATE NULL,
    currency VARCHAR(16) NULL,
    spend DECIMAL(18,6) NULL,
    impressions BIGINT NULL,
    clicks BIGINT NULL,
    leads INT NULL,
    registrations INT NULL,
    cpc DECIMAL(18,6) NULL,
    ctr DECIMAL(18,6) NULL,
    is_total TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fb_rows_upload (upload_id),
    INDEX idx_fb_rows_campaign_day (campaign_name, day_date),
    INDEX idx_fb_rows_account_day (account_name, day_date),
    CONSTRAINT fk_fb_rows_upload FOREIGN KEY (upload_id) REFERENCES fb_csv_uploads (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_campaign_daily (
    campaign_name VARCHAR(255) NOT NULL,
    day_date DATE NOT NULL,
    account_name VARCHAR(255) NULL,
    buyer_id BIGINT NULL,
    geo VARCHAR(16) NULL,
    spend DECIMAL(18,6) NULL,
    impressions BIGINT NULL,
    clicks BIGINT NULL,
    registrations INT NULL,
    leads INT NULL,
    ftd INT NULL,
    revenue DECIMAL(18,6) NULL,
    ctr DECIMAL(18,6) NULL,
    cpc DECIMAL(18,6) NULL,
    roi DECIMAL(18,6) NULL,
    ftd_rate DECIMAL(18,6) NULL,
    status_id BIGINT NULL,
    flag_id BIGINT NULL,
    upload_id BIGINT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_name, day_date),
    INDEX idx_fb_daily_buyer_day (buyer_id, day_date),
    INDEX idx_fb_daily_account_day (account_name, day_date),
    CONSTRAINT fk_fb_daily_upload FOREIGN KEY (upload_id) REFERENCES fb_csv_uploads (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_daily_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_campaign_totals (
    campaign_name VARCHAR(255) PRIMARY KEY,
    account_name VARCHAR(255) NULL,
    buyer_id BIGINT NULL,
    geo VARCHAR(16) NULL,
    spend DECIMAL(18,6) NULL,
    impressions BIGINT NULL,
    clicks BIGINT NULL,
    registrations INT NULL,
    leads INT NULL,
    ftd INT NULL,
    revenue DECIMAL(18,6) NULL,
    ctr DECIMAL(18,6) NULL,
    cpc DECIMAL(18,6) NULL,
    roi DECIMAL(18,6) NULL,
    ftd_rate DECIMAL(18,6) NULL,
    status_id BIGINT NULL,
    flag_id BIGINT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fb_totals_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_accounts (
    account_name VARCHAR(255) PRIMARY KEY,
    buyer_id BIGINT NULL,
    owner_since DATE NULL,
    owner_until DATE NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fb_accounts_buyer FOREIGN KEY (buyer_id) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_statuses (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(32) NOT NULL UNIQUE,
    title VARCHAR(128) NOT NULL,
    description TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_flags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(32) NOT NULL UNIQUE,
    title VARCHAR(128) NOT NULL,
    severity INT NOT NULL DEFAULT 0,
    description TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_campaign_state (
    campaign_name VARCHAR(255) PRIMARY KEY,
    status_id BIGINT NULL,
    flag_id BIGINT NULL,
    buyer_comment TEXT NULL,
    lead_comment TEXT NULL,
    updated_by BIGINT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fb_state_status FOREIGN KEY (status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_state_flag FOREIGN KEY (flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_state_user FOREIGN KEY (updated_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fb_campaign_history (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    campaign_name VARCHAR(255) NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by BIGINT NULL,
    old_status_id BIGINT NULL,
    new_status_id BIGINT NULL,
    old_flag_id BIGINT NULL,
    new_flag_id BIGINT NULL,
    note TEXT NULL,
    CONSTRAINT fk_fb_hist_status_old FOREIGN KEY (old_status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_hist_status_new FOREIGN KEY (new_status_id) REFERENCES fb_statuses (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_hist_flag_old FOREIGN KEY (old_flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_hist_flag_new FOREIGN KEY (new_flag_id) REFERENCES fb_flags (id) ON DELETE SET NULL,
    CONSTRAINT fk_fb_hist_user FOREIGN KEY (changed_by) REFERENCES tg_users (telegram_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
