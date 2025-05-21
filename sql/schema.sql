CREATE TABLE Users(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255)
);
CREATE TABLE Repositories(
    repo_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255)
);
