ALTER TABLE library_event ALTER COLUMN library_event_id DROP DEFAULT;

DROP SEQUENCE IF EXISTS library_event_library_event_id_seq;
