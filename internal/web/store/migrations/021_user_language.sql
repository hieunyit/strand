-- Persist user language preference on the user record.
ALTER TABLE users ADD COLUMN language text NOT NULL DEFAULT '';
