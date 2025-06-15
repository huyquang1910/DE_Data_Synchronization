
CREATE TABLE IF NOT EXISTS User_log_before(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS User_log_after(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DELIMITER //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "UPDATE");
END //

CREATE TRIGGER before_insert_users
BEFORE INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END //

CREATE TRIGGER before_delete_users
BEFORE DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END //

CREATE TRIGGER after_update_users
AFTER UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "UPDATE");
END //

CREATE TRIGGER after_insert_users
AFTER INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END //

CREATE TRIGGER after_delete_users
AFTER DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO User_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END //

DELIMITER ;


-- insert data
INSERT INTO Users VALUES (1, 'alice', 'grav1', 'http://avatar.com/alice.png', 'http://user.com/alice');
INSERT INTO Users VALUES (2, 'bob', 'grav2', 'http://avatar.com/bob.png', 'http://user.com/bob');
INSERT INTO Users VALUES (3, 'carol', 'grav3', 'http://avatar.com/carol.png', 'http://user.com/carol');
INSERT INTO Users VALUES (4, 'david', 'grav4', 'http://avatar.com/david.png', 'http://user.com/david');
INSERT INTO Users VALUES (5, 'eve', 'grav5', 'http://avatar.com/eve.png', 'http://user.com/eve');
INSERT INTO Users VALUES (6, 'frank', 'grav6', 'http://avatar.com/frank.png', 'http://user.com/frank');
INSERT INTO Users VALUES (7, 'grace', 'grav7', 'http://avatar.com/grace.png', 'http://user.com/grace');
INSERT INTO Users VALUES (8, 'heidi', 'grav8', 'http://avatar.com/heidi.png', 'http://user.com/heidi');
INSERT INTO Users VALUES (9, 'ivan', 'grav9', 'http://avatar.com/ivan.png', 'http://user.com/ivan');
INSERT INTO Users VALUES (10, 'judy', 'grav10', 'http://avatar.com/judy.png', 'http://user.com/judy');
--update data
UPDATE Users SET login = 'alice_updated' WHERE user_id = 1;
UPDATE Users SET avatar_url = 'http://avatar.com/bob_new.png' WHERE user_id = 2;
UPDATE Users SET gravatar_id = 'newgrav3' WHERE user_id = 3;
UPDATE Users SET url = 'http://newsite.com/david' WHERE user_id = 4;
UPDATE Users SET login = 'eve_new', url = 'http://user.com/eve123' WHERE user_id = 5;
UPDATE Users SET login = CONCAT(login, '_edit') WHERE user_id = 6;
UPDATE Users SET avatar_url = NULL WHERE user_id = 7;
UPDATE Users SET gravatar_id = 'G8X' WHERE user_id = 8;
UPDATE Users SET url = REPLACE(url, 'user.com', 'profile.com') WHERE user_id = 9;
UPDATE Users SET login = UPPER(login) WHERE user_id = 10;
--delete data
DELETE FROM Users WHERE user_id = 1;
DELETE FROM Users WHERE login = 'bob_updated';
DELETE FROM Users WHERE gravatar_id = 'newgrav3';

