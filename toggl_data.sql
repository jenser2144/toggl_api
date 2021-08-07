CREATE TABLE "toggl_project" (
  "id" SERIAL PRIMARY KEY,
  "project_name" varchar UNIQUE,
  "created_at_date" timestamp,
  "active" boolean
);

CREATE TABLE "toggl_task" (
  "id" SERIAL PRIMARY KEY,
  "task_name" varchar UNIQUE
);

CREATE TABLE "toggl_tag" (
  "id" SERIAL PRIMARY KEY,
  "tag_name" varchar UNIQUE
);

CREATE TABLE "toggl_user" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar
);

CREATE TABLE "toggl_entry_tag" (
  "id" SERIAL PRIMARY KEY,
  "toggl_entry_id" int,
  "toggl_tag_id" int
);

CREATE TABLE "toggl_entry" (
  "id" SERIAL PRIMARY KEY,
  "toggl_project_id" int,
  "toggl_task_id" int,
  "toggl_user_id" int,
  "start_date" timestamp,
  "end_date" timestamp,
  "update_date" timestamp
);

ALTER TABLE "toggl_entry" ADD FOREIGN KEY ("toggl_task_id") REFERENCES "toggl_task" ("id");

ALTER TABLE "toggl_entry" ADD FOREIGN KEY ("toggl_user_id") REFERENCES "toggl_user" ("id");

ALTER TABLE "toggl_entry" ADD FOREIGN KEY ("toggl_project_id") REFERENCES "toggl_project" ("id");

ALTER TABLE "toggl_entry_tag" ADD FOREIGN KEY ("toggl_entry_id") REFERENCES "toggl_entry" ("id");

ALTER TABLE "toggl_entry_tag" ADD FOREIGN KEY ("toggl_tag_id") REFERENCES "toggl_tag" ("id");
