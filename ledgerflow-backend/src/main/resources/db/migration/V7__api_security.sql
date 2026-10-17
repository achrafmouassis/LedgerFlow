CREATE TABLE app_users (
    id UUID PRIMARY KEY,
    username VARCHAR(80) NOT NULL UNIQUE,
    password_hash VARCHAR(100) NOT NULL,
    role VARCHAR(12) NOT NULL CHECK (role IN ('ADMIN','OPERATOR','VIEWER','CUSTOMER')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    failed_logins INTEGER NOT NULL DEFAULT 0,
    locked_until TIMESTAMP WITH TIME ZONE
);
CREATE TABLE account_access (
    user_id UUID NOT NULL REFERENCES app_users(id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    PRIMARY KEY(user_id,account_id)
);
CREATE TABLE app_sessions (
    token_hash VARCHAR(64) PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX sessions_expiry ON app_sessions(expires_at);
