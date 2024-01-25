CREATE TABLE "attributes" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "mep_id" INTEGER NOT NULL,
  "timestamp" TIMESTAMP NOT NULL,
  "name" TEXT,
  "ms" TEXT,
  "date_birth" TIMESTAMP,
  "place_birth" TEXT,
  "date_death" TIMESTAMP,
  FOREIGN KEY (mep_id) REFERENCES meps(mep_id)
);

CREATE TABLE "meps" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "mep_id" INTEGER UNIQUE NOT NULL,
  "url_name" TEXT
);

CREATE TABLE "roles" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "mep_id" INTEGER NOT NULL,
  "url" TEXT NOT NULL,
  "timestamp" TIMESTAMP NOT NULL,
  "ep_num" INTEGER,
  "date_start" TIMESTAMP,
  "date_end" TIMESTAMP,
  "role" TEXT,
  "entity" TEXT,
  "type" TEXT,
  FOREIGN KEY (mep_id) REFERENCES meps(mep_id)
);

CREATE TABLE "mep_data" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "mep_id" INTEGER NOT NULL,
  "timestamp" TIMESTAMP,
  "url" TEXT,
  "html" TEXT NOT NULL,
  FOREIGN KEY (mep_id) REFERENCES meps(mep_id)
);

CREATE TABLE "mep_requests" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "mep_id" INTEGER NOT NULL,
  "ep_num" INTEGER NOT NULL,
  "timestamp" TIMESTAMP NOT NULL,
  "status_code" INTEGER,
  FOREIGN KEY (mep_id) REFERENCES meps(mep_id)
  UNIQUE(mep_id, ep_num)
);