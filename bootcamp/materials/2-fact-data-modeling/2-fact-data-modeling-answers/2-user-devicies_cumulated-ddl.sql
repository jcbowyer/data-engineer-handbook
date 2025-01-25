DROP TABLE IF EXISTS user_devices_cumulated CASCADE;

CREATE TABLE user_devices_cumulated (
    user_id NUMERIC  NOT NULL, -- User identifier
    browser_type TEXT NOT NULL, -- Browser type (e.g., Chrome, Firefox)
    device_activity_datelist DATE[] NOT NULL, -- Array of dates when the user was active with the browser
    PRIMARY KEY (user_id, browser_type) -- Composite primary key to prevent duplicate entries for the same browser type
);